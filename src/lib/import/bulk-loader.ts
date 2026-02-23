import { Connection } from '@salesforce/core';
import { BulkJobInfo } from '../types.js';

const API_VERSION = 'v62.0';
const POLL_INTERVAL_MS = 2_000;
const MAX_POLL_ATTEMPTS = 300; // 10 min ceiling

/**
 * Wraps Salesforce Bulk API 2.0 ingest operations.
 * Supports upsert (with external ID) and delete.
 */
export class BulkLoader {
  constructor(private conn: Connection) {}

  /**
   * Create a Bulk API 2.0 ingest job for upsert.
   * Falls back to 'insert' if no external ID field is provided.
   */
  async createUpsertJob(sobject: string, externalIdField: string | null): Promise<string> {
    const body: Record<string, string> = {
      object: sobject,
      contentType: 'CSV',
      lineEnding: 'LF',
    };

    if (externalIdField) {
      body.operation = 'upsert';
      body.externalIdFieldName = externalIdField;
    } else {
      body.operation = 'insert';
    }

    const result = await this.conn.request<BulkJobInfo>({
      method: 'POST',
      url: `/services/data/${API_VERSION}/jobs/ingest`,
      body: JSON.stringify(body),
      headers: { 'Content-Type': 'application/json' },
    });

    return result.id;
  }

  /**
   * Create a Bulk API 2.0 delete job (soft delete — records go to Recycle Bin).
   */
  async createDeleteJob(sobject: string): Promise<string> {
    const result = await this.conn.request<BulkJobInfo>({
      method: 'POST',
      url: `/services/data/${API_VERSION}/jobs/ingest`,
      body: JSON.stringify({
        object: sobject,
        operation: 'delete',
        contentType: 'CSV',
        lineEnding: 'LF',
      }),
      headers: { 'Content-Type': 'application/json' },
    });

    return result.id;
  }

  /**
   * Upload raw CSV data to an open job.
   */
  async uploadCsvData(jobId: string, csvContent: string): Promise<void> {
    const clean = csvContent.charCodeAt(0) === 0xfeff ? csvContent.slice(1) : csvContent;

    await this.conn.request({
      method: 'PUT',
      url: `/services/data/${API_VERSION}/jobs/ingest/${jobId}/batches`,
      body: clean,
      headers: { 'Content-Type': 'text/csv' },
    });
  }

  /**
   * Signal the job that all data has been uploaded.
   */
  async closeJob(jobId: string): Promise<void> {
    await this.conn.request({
      method: 'PATCH',
      url: `/services/data/${API_VERSION}/jobs/ingest/${jobId}`,
      body: JSON.stringify({ state: 'UploadComplete' }),
      headers: { 'Content-Type': 'application/json' },
    });
  }

  /**
   * Poll until the job reaches a terminal state (JobComplete, Failed, or Aborted).
   */
  async pollUntilDone(jobId: string): Promise<BulkJobInfo> {
    for (let i = 0; i < MAX_POLL_ATTEMPTS; i++) {
      const info = await this.getJobInfo(jobId);

      if (info.state === 'JobComplete' || info.state === 'Failed' || info.state === 'Aborted') {
        return info;
      }

      await this.sleep(POLL_INTERVAL_MS);
    }

    throw new Error(`Bulk job ${jobId} did not complete within ${(MAX_POLL_ATTEMPTS * POLL_INTERVAL_MS) / 1000}s`);
  }

  /**
   * Get current job status.
   */
  async getJobInfo(jobId: string): Promise<BulkJobInfo> {
    return this.conn.request<BulkJobInfo>({
      method: 'GET',
      url: `/services/data/${API_VERSION}/jobs/ingest/${jobId}`,
    });
  }

  /**
   * Download successfulResults CSV from a completed job.
   * Uses raw fetch because conn.request() auto-parses JSON and mangles CSV responses.
   */
  async getSuccessResults(jobId: string): Promise<string> {
    return this.fetchCsv(`/services/data/${API_VERSION}/jobs/ingest/${jobId}/successfulResults`);
  }

  /**
   * Download failedResults CSV from a completed job.
   */
  async getFailedResults(jobId: string): Promise<string> {
    return this.fetchCsv(`/services/data/${API_VERSION}/jobs/ingest/${jobId}/failedResults`);
  }

  /**
   * Convenience: run a full upsert cycle — create job, upload, close, poll, return info.
   */
  async runUpsert(
    sobject: string,
    externalIdField: string | null,
    csvContent: string,
  ): Promise<{ jobId: string; info: BulkJobInfo }> {
    const jobId = await this.createUpsertJob(sobject, externalIdField);
    await this.uploadCsvData(jobId, csvContent);
    await this.closeJob(jobId);
    const info = await this.pollUntilDone(jobId);
    return { jobId, info };
  }

  /**
   * Convenience: run a full delete cycle.
   */
  async runDelete(
    sobject: string,
    csvContent: string,
  ): Promise<{ jobId: string; info: BulkJobInfo }> {
    const jobId = await this.createDeleteJob(sobject);
    await this.uploadCsvData(jobId, csvContent);
    await this.closeJob(jobId);
    const info = await this.pollUntilDone(jobId);
    return { jobId, info };
  }

  /**
   * Fetch a CSV endpoint using native fetch to bypass conn.request()'s JSON auto-parsing.
   */
  private async fetchCsv(path: string): Promise<string> {
    const baseUrl = this.conn.instanceUrl;
    const token = this.conn.accessToken;
    const url = `${baseUrl}${path}`;

    const resp = await fetch(url, {
      method: 'GET',
      headers: {
        Authorization: `Bearer ${token}`,
        Accept: 'text/csv',
      },
    });

    if (!resp.ok) {
      throw new Error(`Failed to fetch CSV from ${path}: ${resp.status} ${resp.statusText}`);
    }

    return resp.text();
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
