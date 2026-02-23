import { writeFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { parse } from 'csv-parse/sync';
import { ImportLog, ImportLogEntry, BulkJobInfo } from '../types.js';

/**
 * Accumulates import results and writes the _import-log.json artifact.
 * The log captures every external ID that was successfully loaded, enabling rollback.
 */
export class ImportTracker {
  private entries: ImportLogEntry[] = [];

  constructor(
    private targetOrg: string,
    private recipe: string,
    private sourceExportDir: string,
  ) {}

  /**
   * Record the result of a single Bulk API job.
   * Parses the success CSV to extract external IDs for rollback tracking.
   */
  addResult(
    order: number,
    sobject: string,
    externalIdField: string | null,
    csvFile: string,
    recordsSubmitted: number,
    jobId: string,
    info: BulkJobInfo,
    successCsv: string,
  ): ImportLogEntry {
    const succeeded = info.numberRecordsProcessed - info.numberRecordsFailed;
    const externalIds = this.extractExternalIds(successCsv, externalIdField);
    const sfIds = this.extractSfIds(successCsv);

    let status: ImportLogEntry['status'];
    if (info.state === 'Failed') {
      status = 'failed';
    } else if (info.numberRecordsFailed > 0) {
      status = 'partial';
    } else {
      status = 'success';
    }

    const entry: ImportLogEntry = {
      order,
      sobject,
      externalIdField,
      csvFile,
      recordsSubmitted,
      recordsSucceeded: succeeded,
      recordsFailed: info.numberRecordsFailed,
      externalIds,
      sfIds,
      jobId,
      status,
    };

    this.entries.push(entry);
    return entry;
  }

  /**
   * Write the accumulated log to _import-log.json in the export directory.
   */
  writeLog(outputDir: string): string {
    const log: ImportLog = {
      generated: new Date().toISOString(),
      targetOrg: this.targetOrg,
      recipe: this.recipe,
      sourceExportDir: this.sourceExportDir,
      objects: this.entries,
    };

    const logPath = resolve(outputDir, '_import-log.json');
    writeFileSync(logPath, JSON.stringify(log, null, 2), 'utf-8');
    return logPath;
  }

  /**
   * Save a raw CSV result (success or failure) to the export dir for inspection.
   */
  saveResultCsv(outputDir: string, filename: string, csvContent: string): void {
    if (!csvContent || typeof csvContent !== 'string' || csvContent.trim().length === 0) return;
    const filePath = resolve(outputDir, filename);
    writeFileSync(filePath, csvContent, 'utf-8');
  }

  getEntries(): ImportLogEntry[] {
    return [...this.entries];
  }

  /**
   * Append additional Salesforce IDs to the most recent entry (e.g., from retry recoveries).
   */
  appendSfIds(sfIds: string[]): void {
    const last = this.entries.at(-1);
    if (last) {
      last.sfIds.push(...sfIds);
      last.recordsSucceeded += sfIds.length;
      last.recordsFailed = Math.max(0, last.recordsFailed - sfIds.length);
      if (last.recordsFailed === 0) last.status = 'success';
    }
  }

  /**
   * Parse the Bulk API successfulResults CSV and extract external ID values.
   * The success CSV has columns: "sf__Id","sf__Created",<original columns...>
   */
  private extractExternalIds(successCsv: string, externalIdField: string | null): string[] {
    if (!externalIdField || !successCsv || typeof successCsv !== 'string' || successCsv.trim().length === 0) {
      return [];
    }

    try {
      const records = parse(successCsv, {
        columns: true,
        skip_empty_lines: true,
        bom: true,
        relax_quotes: true,
      }) as Record<string, string>[];

      const ids: string[] = [];
      for (const rec of records) {
        const val = rec[externalIdField];
        if (val && val.trim().length > 0) {
          ids.push(val.trim());
        }
      }

      return ids;
    } catch {
      return [];
    }
  }

  /**
   * Extract Salesforce record IDs (sf__Id) from success results CSV.
   * Used for rollback when external IDs aren't available (auto-number fields).
   */
  private extractSfIds(successCsv: string): string[] {
    if (!successCsv || typeof successCsv !== 'string' || successCsv.trim().length === 0) {
      return [];
    }

    try {
      const records = parse(successCsv, {
        columns: true,
        skip_empty_lines: true,
        bom: true,
        relax_quotes: true,
      }) as Record<string, string>[];

      const ids: string[] = [];
      for (const rec of records) {
        const val = rec['sf__Id'];
        if (val && val.trim().length > 0) {
          ids.push(val.trim());
        }
      }

      return ids;
    } catch {
      return [];
    }
  }
}
