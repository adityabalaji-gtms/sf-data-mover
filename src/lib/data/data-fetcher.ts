import { Connection } from '@salesforce/core';

type SalesforceRecord = Record<string, unknown> & { Id: string; attributes?: { type: string; url: string } };

/**
 * Executes SOQL queries against an org with automatic pagination (queryMore).
 */
export class DataFetcher {
  constructor(private conn: Connection) {}

  async fetchAll(soql: string): Promise<SalesforceRecord[]> {
    const records: SalesforceRecord[] = [];
    let result = await this.conn.query<SalesforceRecord>(soql);

    for (const rec of result.records) {
      records.push(this.stripAttributes(rec));
    }

    while (!result.done && result.nextRecordsUrl) {
      result = await this.conn.queryMore<SalesforceRecord>(result.nextRecordsUrl);
      for (const rec of result.records) {
        records.push(this.stripAttributes(rec));
      }
    }

    return records;
  }

  /**
   * Fetch records in batches using SOQL with LIMIT/OFFSET.
   * Useful for very large datasets where we want progress reporting.
   */
  async fetchBatched(
    soql: string,
    batchSize: number,
    onBatch?: (records: SalesforceRecord[], total: number) => void,
  ): Promise<SalesforceRecord[]> {
    const all: SalesforceRecord[] = [];
    let offset = 0;

    while (true) {
      const batchSoql = `${soql} LIMIT ${batchSize} OFFSET ${offset}`;
      let result;
      try {
        result = await this.conn.query<SalesforceRecord>(batchSoql);
      } catch {
        // OFFSET not supported — fall back to queryMore
        return this.fetchAll(soql);
      }

      const records = result.records.map((r) => this.stripAttributes(r));
      all.push(...records);
      onBatch?.(records, all.length);

      if (records.length < batchSize) break;
      offset += batchSize;
    }

    return all;
  }

  private stripAttributes(record: SalesforceRecord): SalesforceRecord {
    const { attributes, ...rest } = record;
    return rest as SalesforceRecord;
  }
}
