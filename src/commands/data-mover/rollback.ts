import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { SfCommand, Flags } from '@salesforce/sf-plugins-core';
import { Org } from '@salesforce/core';
import { BulkLoader } from '../../lib/import/bulk-loader.js';
import { ImportLog } from '../../lib/types.js';

const SOQL_IN_CHUNK = 200;

export default class Rollback extends SfCommand<void> {
  public static readonly summary = 'Roll back a previous import by deleting loaded records in reverse dependency order.';

  public static readonly description =
    'Reads an _import-log.json produced by "data-mover import", reverses the load order ' +
    '(children first), queries for records by external ID, and deletes them via Bulk API 2.0.';

  public static readonly examples = [
    'sf data-mover rollback --target-org ppdev --import-log ./exports/uat-rules/_import-log.json',
    'sf data-mover rollback --target-org ppdev --import-log ./exports/uat-rules/_import-log.json --dry-run',
  ];

  public static readonly flags = {
    'target-org': Flags.requiredOrg({
      summary: 'Org to delete records from.',
      char: 'o',
      required: true,
    }),
    'import-log': Flags.string({
      summary: 'Path to the _import-log.json file from a previous import.',
      char: 'l',
      required: true,
    }),
    'dry-run': Flags.boolean({
      summary: 'Show what would be deleted without actually deleting.',
      default: false,
    }),
  };

  public async run(): Promise<void> {
    const { flags } = await this.parse(Rollback);
    const dryRun = flags['dry-run'];

    const logPath = resolve(flags['import-log']);
    let importLog: ImportLog;
    try {
      importLog = JSON.parse(readFileSync(logPath, 'utf-8'));
    } catch {
      this.error(`Could not read import log at ${logPath}`);
    }

    const org: Org = flags['target-org'];
    const conn = org.getConnection();
    const alias = org.getUsername() ?? 'unknown';

    // Collect entries that have either external IDs or Salesforce IDs tracked
    const entries = importLog.objects
      .filter((e) => e.recordsSucceeded > 0 && (
        (e.externalIdField && e.externalIds.length > 0) ||
        (e.sfIds && e.sfIds.length > 0)
      ));

    if (entries.length === 0) {
      this.log('Nothing to roll back — no successfully imported records with tracked IDs.');
      return;
    }

    // Reverse the load order: children (higher order) deleted first, then parents
    entries.sort((a, b) => b.order - a.order);

    // Deduplicate: if the same sobject appears multiple times (main + self-ref pass),
    // merge their IDs into one entry for deletion
    const mergedMap = new Map<string, {
      sobject: string;
      externalIdField: string | null;
      externalIds: Set<string>;
      sfIds: Set<string>;
    }>();
    for (const e of entries) {
      const existing = mergedMap.get(e.sobject);
      if (existing) {
        for (const id of (e.externalIds ?? [])) existing.externalIds.add(id);
        for (const id of (e.sfIds ?? [])) existing.sfIds.add(id);
      } else {
        mergedMap.set(e.sobject, {
          sobject: e.sobject,
          externalIdField: e.externalIdField,
          externalIds: new Set(e.externalIds ?? []),
          sfIds: new Set(e.sfIds ?? []),
        });
      }
    }

    // Preserve reverse order using the first occurrence of each sobject
    const seen = new Set<string>();
    const mergedEntries: {
      sobject: string;
      externalIdField: string | null;
      externalIds: string[];
      sfIds: string[];
    }[] = [];
    for (const e of entries) {
      if (seen.has(e.sobject)) continue;
      seen.add(e.sobject);
      const merged = mergedMap.get(e.sobject)!;
      mergedEntries.push({
        sobject: merged.sobject,
        externalIdField: merged.externalIdField,
        externalIds: [...merged.externalIds],
        sfIds: [...merged.sfIds],
      });
    }

    this.log(`\nTarget org: ${alias}`);
    this.log(`Import log: ${logPath}`);
    this.log(`Objects to roll back: ${mergedEntries.length} (in reverse tier order)\n`);

    if (dryRun) {
      this.log('── Dry Run ──');
      for (const e of mergedEntries) {
        const count = e.sfIds.length > 0 ? e.sfIds.length : e.externalIds.length;
        const method = e.sfIds.length > 0 ? 'by Salesforce Id' : `by ${e.externalIdField}`;
        this.log(`  ${e.sobject}: ${count} records to delete (${method})`);
      }
      return;
    }

    const loader = new BulkLoader(conn);
    const summaryRows: { Object: string; Queried: number; Deleted: number; Status: string }[] = [];

    for (const entry of mergedEntries) {
      const totalIds = entry.sfIds.length > 0 ? entry.sfIds.length : entry.externalIds.length;
      this.spinner.start(`Deleting ${entry.sobject} (${totalIds} records)`);

      try {
        // Get Salesforce IDs: use sfIds directly if available, otherwise query by external ID
        let sfIds: string[];
        if (entry.sfIds.length > 0) {
          sfIds = entry.sfIds;
        } else if (entry.externalIdField && entry.externalIds.length > 0) {
          sfIds = await this.queryIdsByExternalIds(
            conn, entry.sobject, entry.externalIdField, entry.externalIds,
          );
        } else {
          sfIds = [];
        }

        if (sfIds.length === 0) {
          this.spinner.stop('0 records found — nothing to delete');
          summaryRows.push({ Object: entry.sobject, Queried: 0, Deleted: 0, Status: 'SKIP' });
          continue;
        }

        // Build delete CSV (just Id column)
        const csv = 'Id\n' + sfIds.join('\n');

        const { info } = await loader.runDelete(entry.sobject, csv);
        const deleted = info.numberRecordsProcessed - info.numberRecordsFailed;

        if (info.numberRecordsFailed > 0) {
          this.spinner.stop(`${deleted} deleted, ${info.numberRecordsFailed} failed`);
          summaryRows.push({
            Object: entry.sobject,
            Queried: sfIds.length,
            Deleted: deleted,
            Status: 'PARTIAL',
          });
        } else {
          this.spinner.stop(`${deleted} records deleted`);
          summaryRows.push({
            Object: entry.sobject,
            Queried: sfIds.length,
            Deleted: deleted,
            Status: 'OK',
          });
        }
      } catch (err) {
        this.spinner.stop('ERROR');
        const msg = err instanceof Error ? err.message : String(err);
        this.warn(`${entry.sobject}: ${msg}`);
        summaryRows.push({
          Object: entry.sobject,
          Queried: totalIds,
          Deleted: 0,
          Status: 'ERROR',
        });
      }
    }

    // Summary table
    this.log('\n── Rollback Summary ──');
    this.table({ data: summaryRows });

    const totalDeleted = summaryRows.reduce((s, r) => s + r.Deleted, 0);
    this.log(`\nTotal deleted: ${totalDeleted} records`);
  }

  /**
   * Query Salesforce record IDs by external ID values, chunked to stay within SOQL limits.
   */
  private async queryIdsByExternalIds(
    conn: import('@salesforce/core').Connection,
    sobject: string,
    externalIdField: string,
    externalIds: string[],
  ): Promise<string[]> {
    const allIds: string[] = [];

    for (let i = 0; i < externalIds.length; i += SOQL_IN_CHUNK) {
      const chunk = externalIds.slice(i, i + SOQL_IN_CHUNK);
      const inClause = chunk.map((id) => `'${id.replace(/'/g, "\\'")}'`).join(',');
      const soql = `SELECT Id FROM ${sobject} WHERE ${externalIdField} IN (${inClause})`;

      const result = await conn.query<{ Id: string }>(soql);
      for (const rec of result.records) {
        allIds.push(rec.Id);
      }
    }

    return allIds;
  }
}
