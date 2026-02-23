import { Connection } from '@salesforce/core';
import { Recipe, RecipeObject, ObjectDiff, DiffRecord, ModifiedRecord, DiffResult, DiffSummary, ObjectDescribe } from '../types.js';
import { DataFetcher } from '../data/data-fetcher.js';
import { QueryBuilder } from '../data/query-builder.js';
import { SchemaDescriber } from '../schema/describer.js';

type SalesforceRecord = Record<string, unknown> & { Id: string };

const SYSTEM_FIELDS = new Set([
  'Id', 'IsDeleted', 'CreatedDate', 'CreatedById',
  'LastModifiedDate', 'LastModifiedById', 'SystemModstamp',
  'OwnerId', 'attributes',
]);

/**
 * Core diff logic: fetches data from both orgs, joins by external ID,
 * and performs field-by-field comparison.
 */
export class DiffEngine {
  constructor(
    private sourceConn: Connection,
    private targetConn: Connection,
    private recipe: Recipe,
  ) {}

  async diffAll(
    onProgress?: (sobject: string, status: string) => void,
  ): Promise<DiffResult> {
    const objects: Record<string, ObjectDiff> = {};
    const summary: DiffSummary = { totalNew: 0, totalModified: 0, totalDeleted: 0, totalIdentical: 0 };

    for (const recipeObj of this.recipe.objects) {
      if (!recipeObj.externalIdField) {
        onProgress?.(recipeObj.sobject, 'skipped (no external ID)');
        continue;
      }

      onProgress?.(recipeObj.sobject, 'fetching...');
      const diff = await this.diffObject(recipeObj);
      objects[recipeObj.sobject] = diff;

      summary.totalNew += diff.counts.new;
      summary.totalModified += diff.counts.modified;
      summary.totalDeleted += diff.counts.deleted;
      summary.totalIdentical += diff.counts.identical;

      onProgress?.(recipeObj.sobject, `+${diff.counts.new} ~${diff.counts.modified} -${diff.counts.deleted} =${diff.counts.identical}`);
    }

    return {
      generated: new Date().toISOString(),
      sourceOrg: this.sourceConn.getUsername() ?? 'source',
      targetOrg: this.targetConn.getUsername() ?? 'target',
      recipe: this.recipe.name,
      summary,
      objects,
    };
  }

  async diffObject(recipeObj: RecipeObject): Promise<ObjectDiff> {
    const extIdField = recipeObj.externalIdField!;
    const queryBuilder = new QueryBuilder();

    // Describe from source org to get field list
    const describer = new SchemaDescriber(this.sourceConn);
    const describe = await describer.describe(recipeObj.sobject);
    const fields = queryBuilder.selectFields(recipeObj, describe, this.recipe.settings);

    const fieldList = fields.join(', ');
    const where = recipeObj.filter ? ` WHERE ${recipeObj.filter}` : '';
    const soql = `SELECT ${fieldList} FROM ${recipeObj.sobject}${where}`;

    const sourceFetcher = new DataFetcher(this.sourceConn);
    const targetFetcher = new DataFetcher(this.targetConn);

    const [sourceRecords, targetRecords] = await Promise.all([
      sourceFetcher.fetchAll(soql),
      targetFetcher.fetchAll(soql).catch(() => [] as SalesforceRecord[]),
    ]);

    // Build maps keyed by external ID
    const sourceMap = this.buildExtIdMap(sourceRecords, extIdField);
    const targetMap = this.buildExtIdMap(targetRecords, extIdField);

    const ignoreFields = new Set([
      ...SYSTEM_FIELDS,
      ...(recipeObj.compareIgnoreFields ?? []),
    ]);

    const compareFields = fields.filter((f) => !ignoreFields.has(f));

    const newRecords: DiffRecord[] = [];
    const modifiedRecords: ModifiedRecord[] = [];
    const deletedRecords: DiffRecord[] = [];
    let identical = 0;

    // Check source records against target
    for (const [extId, sourceRec] of sourceMap) {
      const targetRec = targetMap.get(extId);

      if (!targetRec) {
        newRecords.push({
          externalId: extId,
          name: this.extractName(sourceRec),
        });
        continue;
      }

      const changes: Record<string, { source: unknown; target: unknown }> = {};
      for (const field of compareFields) {
        const sv = this.normalize(sourceRec[field]);
        const tv = this.normalize(targetRec[field]);
        if (sv !== tv) {
          changes[field] = { source: sourceRec[field] ?? null, target: targetRec[field] ?? null };
        }
      }

      if (Object.keys(changes).length > 0) {
        modifiedRecords.push({
          externalId: extId,
          name: this.extractName(sourceRec),
          changes,
        });
      } else {
        identical++;
      }
    }

    // Check for deleted (in target but not source)
    for (const [extId, targetRec] of targetMap) {
      if (!sourceMap.has(extId)) {
        deletedRecords.push({
          externalId: extId,
          name: this.extractName(targetRec),
        });
      }
    }

    return {
      counts: {
        source: sourceMap.size,
        target: targetMap.size,
        new: newRecords.length,
        modified: modifiedRecords.length,
        deleted: deletedRecords.length,
        identical,
      },
      newRecords,
      modifiedRecords,
      deletedRecords,
    };
  }

  private buildExtIdMap(
    records: SalesforceRecord[],
    extIdField: string,
  ): Map<string, SalesforceRecord> {
    const map = new Map<string, SalesforceRecord>();
    for (const rec of records) {
      const key = rec[extIdField];
      if (key != null && key !== '') {
        map.set(String(key), rec);
      }
    }
    return map;
  }

  private extractName(record: SalesforceRecord): string | undefined {
    return (record.Name ?? record.SBQQ__RuleName__c ?? record.DeveloperName) as string | undefined;
  }

  private normalize(value: unknown): string {
    if (value === null || value === undefined) return '';
    if (typeof value === 'boolean') return value ? 'true' : 'false';
    return String(value);
  }
}
