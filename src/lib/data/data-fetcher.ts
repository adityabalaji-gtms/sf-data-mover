import { Connection } from '@salesforce/core';

type SalesforceRecord = Record<string, unknown> & { Id: string; attributes?: { type: string; url: string } };

const SOQL_LENGTH_THRESHOLD = 8000;
const FIELDS_PER_CHUNK = 200;

/**
 * Executes SOQL queries against an org with automatic pagination (queryMore).
 * Automatically splits queries with many fields into parallel chunks and merges
 * results to avoid HTTP 431 "Request Header Fields Too Large" errors.
 */
export class DataFetcher {
  constructor(private conn: Connection) {}

  async fetchAll(soql: string): Promise<SalesforceRecord[]> {
    if (soql.length > SOQL_LENGTH_THRESHOLD && this.isChunkable(soql)) {
      return this.fetchChunked(soql);
    }

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

  /**
   * Split a wide SELECT into multiple narrower queries (each with Id),
   * execute them sequentially, and merge results by Id.
   */
  private async fetchChunked(soql: string): Promise<SalesforceRecord[]> {
    const parsed = this.parseSoql(soql);
    if (!parsed) return this.fetchAllDirect(soql);

    const { fields, fromClause } = parsed;
    const nonIdFields = fields.filter((f) => f !== 'Id');
    const chunks: string[][] = [];
    for (let i = 0; i < nonIdFields.length; i += FIELDS_PER_CHUNK) {
      chunks.push(nonIdFields.slice(i, i + FIELDS_PER_CHUNK));
    }

    const recordMap = new Map<string, SalesforceRecord>();

    for (const chunk of chunks) {
      const chunkSoql = `SELECT Id, ${chunk.join(', ')} FROM ${fromClause}`;
      const chunkRecords = await this.fetchAllDirect(chunkSoql);
      for (const rec of chunkRecords) {
        const existing = recordMap.get(rec.Id);
        if (existing) {
          Object.assign(existing, rec);
        } else {
          recordMap.set(rec.Id, rec);
        }
      }
    }

    return Array.from(recordMap.values());
  }

  private async fetchAllDirect(soql: string): Promise<SalesforceRecord[]> {
    const records: SalesforceRecord[] = [];
    let result = await this.conn.query<SalesforceRecord>(soql);
    for (const rec of result.records) records.push(this.stripAttributes(rec));
    while (!result.done && result.nextRecordsUrl) {
      result = await this.conn.queryMore<SalesforceRecord>(result.nextRecordsUrl);
      for (const rec of result.records) records.push(this.stripAttributes(rec));
    }
    return records;
  }

  private isChunkable(soql: string): boolean {
    return /^SELECT\s+.+\s+FROM\s+/is.test(soql);
  }

  private parseSoql(soql: string): { fields: string[]; fromClause: string } | null {
    const match = soql.match(/^SELECT\s+([\s\S]+?)\s+FROM\s+([\s\S]+)$/i);
    if (!match) return null;
    const fields = match[1].split(/\s*,\s*/);
    return { fields, fromClause: match[2] };
  }

  private stripAttributes(record: SalesforceRecord): SalesforceRecord {
    const { attributes, ...rest } = record;
    return rest as SalesforceRecord;
  }
}
