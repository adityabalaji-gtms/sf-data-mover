import { parse } from 'csv-parse/sync';
import { stringify } from 'csv-stringify/sync';
import { BulkLoader } from './bulk-loader.js';
import { BulkJobInfo } from '../types.js';

/** Errors that are safe to retry (transient lock/batch contention) */
const RETRYABLE_PATTERNS = [
  'CANNOT_INSERT_UPDATE_ACTIVATE_ENTITY',
  'UNABLE_TO_LOCK_ROW',
  'Row was retrieved via a query locker',
  'Too many retries of batch save',
];

export interface RetryResult {
  retriedCount: number;
  recoveredCount: number;
  stillFailedCount: number;
  /** CSV of records that remain failed after all retries */
  permanentFailureCsv: string;
  /** CSV of records that were recovered */
  recoveredSuccessCsv: string;
  /** Salesforce IDs of recovered records */
  recoveredSfIds: string[];
}

/**
 * Retries failed Bulk API records that hit transient errors (row locks, batch contention).
 * Extracts retryable rows from the failure CSV and resubmits in smaller batches.
 */
export class RetryHandler {
  constructor(
    private loader: BulkLoader,
    private maxRetries = 2,
    private delayMs = 5_000,
  ) {}

  /**
   * Attempt to retry failed records from a Bulk API job.
   * Only retries rows with transient/retryable errors.
   */
  async retryFailedRecords(
    sobject: string,
    externalIdField: string | null,
    failedCsv: string,
  ): Promise<RetryResult> {
    if (!failedCsv || typeof failedCsv !== 'string' || failedCsv.trim().length === 0) {
      return { retriedCount: 0, recoveredCount: 0, stillFailedCount: 0, permanentFailureCsv: '', recoveredSuccessCsv: '', recoveredSfIds: [] };
    }

    const { retryable, permanent } = this.splitByRetryability(failedCsv);

    if (retryable.length === 0) {
      return { retriedCount: 0, recoveredCount: 0, stillFailedCount: 0, permanentFailureCsv: failedCsv, recoveredSuccessCsv: '', recoveredSfIds: [] };
    }

    const retryCsv = this.buildRetryCsv(retryable);
    let currentCsv = retryCsv;
    let allRecoveredCsv = '';
    const allRecoveredSfIds: string[] = [];
    let attempt = 0;

    while (attempt < this.maxRetries && currentCsv) {
      attempt++;
      await this.sleep(this.delayMs * attempt);

      const { jobId, info } = await this.loader.runUpsert(sobject, externalIdField, currentCsv);
      const successCsv = await this.loader.getSuccessResults(jobId);
      const newFailedCsv = await this.loader.getFailedResults(jobId);

      // Collect recovered records
      if (successCsv && successCsv.trim()) {
        allRecoveredCsv = allRecoveredCsv ? this.mergeCsvs(allRecoveredCsv, successCsv) : successCsv;
        allRecoveredSfIds.push(...this.extractSfIds(successCsv));
      }

      // If no more failures or nothing retryable, stop
      if (info.numberRecordsFailed === 0 || !newFailedCsv || !newFailedCsv.trim()) {
        break;
      }

      const { retryable: newRetryable } = this.splitByRetryability(newFailedCsv);
      if (newRetryable.length === 0) {
        // Remaining failures are permanent
        break;
      }

      currentCsv = this.buildRetryCsv(newRetryable);
    }

    // Merge permanent failures from original + any leftover retryables that didn't recover
    const totalRecovered = allRecoveredSfIds.length;
    const totalRetried = retryable.length;
    const stillFailed = totalRetried - totalRecovered;

    return {
      retriedCount: totalRetried,
      recoveredCount: totalRecovered,
      stillFailedCount: stillFailed,
      permanentFailureCsv: permanent.length > 0 ? this.buildFailureCsv(permanent) : '',
      recoveredSuccessCsv: allRecoveredCsv,
      recoveredSfIds: allRecoveredSfIds,
    };
  }

  /**
   * Split failure CSV rows into retryable and permanent failures.
   */
  private splitByRetryability(failedCsv: string): {
    retryable: Record<string, string>[];
    permanent: Record<string, string>[];
  } {
    const records = parse(failedCsv, {
      columns: true,
      skip_empty_lines: true,
      bom: true,
      relax_quotes: true,
      relax_column_count: true,
    }) as Record<string, string>[];

    const retryable: Record<string, string>[] = [];
    const permanent: Record<string, string>[] = [];

    for (const rec of records) {
      const error = rec['sf__Error'] ?? '';
      if (RETRYABLE_PATTERNS.some((p) => error.includes(p))) {
        retryable.push(rec);
      } else {
        permanent.push(rec);
      }
    }

    return { retryable, permanent };
  }

  /**
   * Build a clean CSV from failed records by stripping the sf__ prefix columns.
   */
  private buildRetryCsv(records: Record<string, string>[]): string {
    if (records.length === 0) return '';
    const allHeaders = Object.keys(records[0]);
    const dataHeaders = allHeaders.filter((h) => !h.startsWith('sf__'));

    const rows = records.map((rec) => dataHeaders.map((h) => rec[h] ?? ''));
    return stringify([dataHeaders, ...rows]);
  }

  /**
   * Build a failure CSV preserving sf__ columns (for admin review).
   */
  private buildFailureCsv(records: Record<string, string>[]): string {
    if (records.length === 0) return '';
    const headers = Object.keys(records[0]);
    const rows = records.map((rec) => headers.map((h) => rec[h] ?? ''));
    return stringify([headers, ...rows]);
  }

  private mergeCsvs(csv1: string, csv2: string): string {
    const lines1 = csv1.trim().split('\n');
    const lines2 = csv2.trim().split('\n');
    // Skip header of csv2
    return [...lines1, ...lines2.slice(1)].join('\n');
  }

  private extractSfIds(successCsv: string): string[] {
    try {
      const records = parse(successCsv, {
        columns: true,
        skip_empty_lines: true,
        bom: true,
        relax_quotes: true,
      }) as Record<string, string>[];
      return records.map((r) => r['sf__Id']).filter((id) => id && id.trim());
    } catch {
      return [];
    }
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
