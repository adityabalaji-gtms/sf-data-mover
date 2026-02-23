import { Connection } from '@salesforce/core';
import { Recipe, RecipeObject, ObjectDiff, DiffRecord, ModifiedRecord, DiffResult, DiffSummary, ObjectDescribe, FieldDescribe } from '../types.js';
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
 * Core diff logic: fetches data from both orgs, joins by external ID
 * (or content fingerprint for auto-number fields), and performs
 * field-by-field comparison excluding reference/lookup fields.
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
      let diff: ObjectDiff;
      try {
        diff = await this.diffObject(recipeObj);
      } catch {
        onProgress?.(recipeObj.sobject, 'skipped (object not found in one or both orgs)');
        continue;
      }
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

    const describer = new SchemaDescriber(this.sourceConn);
    const describe = await describer.describe(recipeObj.sobject);
    const fields = queryBuilder.selectFields(recipeObj, describe, this.recipe.settings);

    const fieldList = fields.join(', ');
    const where = recipeObj.filter ? ` WHERE ${recipeObj.filter}` : '';
    const soql = `SELECT ${fieldList} FROM ${recipeObj.sobject}${where}`;

    const sourceFetcher = new DataFetcher(this.sourceConn);
    const targetFetcher = new DataFetcher(this.targetConn);

    // Describe target separately — it may have a different field set
    const targetDescriber = new SchemaDescriber(this.targetConn);
    let targetDescribe: ObjectDescribe;
    try {
      targetDescribe = await targetDescriber.describe(recipeObj.sobject);
    } catch {
      targetDescribe = describe;
    }

    const targetFields = queryBuilder.selectFields(recipeObj, targetDescribe, this.recipe.settings);
    const commonFields = fields.filter((f) => targetFields.includes(f));
    const commonFieldList = commonFields.join(', ');
    const targetSoql = `SELECT ${commonFieldList} FROM ${recipeObj.sobject}${where}`;

    const [sourceRecords, targetRecords] = await Promise.all([
      sourceFetcher.fetchAll(soql),
      targetFetcher.fetchAll(targetSoql),
    ]);

    const referenceFields = this.getReferenceFields(describe);
    const extIdDescribe = describe.fields.find((f) => f.name === extIdField);
    const useFingerprint = extIdDescribe?.autoNumber === true;

    const ignoreFields = new Set([
      ...SYSTEM_FIELDS,
      ...(recipeObj.compareIgnoreFields ?? []),
      ...referenceFields,
    ]);

    // Auto-number external IDs differ per org by definition; exclude from comparison
    if (useFingerprint) {
      ignoreFields.add(extIdField);
    }

    const compareFields = commonFields.filter((f) => !ignoreFields.has(f));

    if (useFingerprint) {
      return this.diffByFingerprint(
        sourceRecords, targetRecords, compareFields, describe,
      );
    }

    return this.diffByExternalId(
      sourceRecords, targetRecords, extIdField, compareFields,
    );
  }

  /**
   * Standard diff: join records by external ID field value.
   */
  private diffByExternalId(
    sourceRecords: SalesforceRecord[],
    targetRecords: SalesforceRecord[],
    extIdField: string,
    compareFields: string[],
  ): ObjectDiff {
    const sourceMap = this.buildExtIdMap(sourceRecords, extIdField);
    const targetMap = this.buildExtIdMap(targetRecords, extIdField);

    const newRecords: DiffRecord[] = [];
    const modifiedRecords: ModifiedRecord[] = [];
    const deletedRecords: DiffRecord[] = [];
    let identical = 0;

    for (const [extId, sourceRec] of sourceMap) {
      const targetRec = targetMap.get(extId);

      if (!targetRec) {
        newRecords.push({ externalId: extId, name: this.extractName(sourceRec) });
        continue;
      }

      const changes = this.compareRecords(sourceRec, targetRec, compareFields);
      if (Object.keys(changes).length > 0) {
        modifiedRecords.push({ externalId: extId, name: this.extractName(sourceRec), changes });
      } else {
        identical++;
      }
    }

    for (const [extId, targetRec] of targetMap) {
      if (!sourceMap.has(extId)) {
        deletedRecords.push({ externalId: extId, name: this.extractName(targetRec) });
      }
    }

    return {
      counts: {
        source: sourceMap.size, target: targetMap.size,
        new: newRecords.length, modified: modifiedRecords.length,
        deleted: deletedRecords.length, identical,
      },
      matchStrategy: 'externalId',
      newRecords, modifiedRecords, deletedRecords,
    };
  }

  /**
   * Fingerprint diff: for auto-number external IDs, match records by a
   * content fingerprint built from all comparable fields.
   * Handles collisions (true duplicates) via positional matching.
   */
  private diffByFingerprint(
    sourceRecords: SalesforceRecord[],
    targetRecords: SalesforceRecord[],
    compareFields: string[],
    describe: ObjectDescribe,
  ): ObjectDiff {
    const fingerprintFields = this.getFingerprintFields(describe, compareFields);

    const sourceGroups = this.buildFingerprintMap(sourceRecords, fingerprintFields);
    const targetGroups = this.buildFingerprintMap(targetRecords, fingerprintFields);

    const newRecords: DiffRecord[] = [];
    const modifiedRecords: ModifiedRecord[] = [];
    const deletedRecords: DiffRecord[] = [];
    let identical = 0;

    const matchedTargetFingerprints = new Set<string>();

    for (const [fp, sourceGroup] of sourceGroups) {
      const targetGroup = targetGroups.get(fp);

      if (!targetGroup || targetGroup.length === 0) {
        for (const rec of sourceGroup) {
          newRecords.push({ externalId: fp, name: this.extractName(rec) });
        }
        continue;
      }

      matchedTargetFingerprints.add(fp);

      const pairCount = Math.min(sourceGroup.length, targetGroup.length);

      for (let i = 0; i < pairCount; i++) {
        const changes = this.compareRecords(sourceGroup[i], targetGroup[i], compareFields);
        if (Object.keys(changes).length > 0) {
          modifiedRecords.push({
            externalId: fp,
            name: this.extractName(sourceGroup[i]),
            changes,
          });
        } else {
          identical++;
        }
      }

      // Extra source records beyond what target has
      for (let i = pairCount; i < sourceGroup.length; i++) {
        newRecords.push({ externalId: fp, name: this.extractName(sourceGroup[i]) });
      }

      // Extra target records beyond what source has
      for (let i = pairCount; i < targetGroup.length; i++) {
        deletedRecords.push({ externalId: fp, name: this.extractName(targetGroup[i]) });
      }
    }

    // Target fingerprints with no source match at all
    for (const [fp, targetGroup] of targetGroups) {
      if (!matchedTargetFingerprints.has(fp)) {
        for (const rec of targetGroup) {
          deletedRecords.push({ externalId: fp, name: this.extractName(rec) });
        }
      }
    }

    const sourceTotal = [...sourceGroups.values()].reduce((s, g) => s + g.length, 0);
    const targetTotal = [...targetGroups.values()].reduce((s, g) => s + g.length, 0);

    return {
      counts: {
        source: sourceTotal, target: targetTotal,
        new: newRecords.length, modified: modifiedRecords.length,
        deleted: deletedRecords.length, identical,
      },
      matchStrategy: 'fingerprint',
      newRecords, modifiedRecords, deletedRecords,
    };
  }

  /**
   * Build a map of fingerprint → ordered list of records.
   * Multiple records with the same fingerprint (true duplicates) are grouped together.
   */
  private buildFingerprintMap(
    records: SalesforceRecord[],
    fingerprintFields: string[],
  ): Map<string, SalesforceRecord[]> {
    const map = new Map<string, SalesforceRecord[]>();
    for (const rec of records) {
      const fp = this.buildFingerprint(rec, fingerprintFields);
      const group = map.get(fp) ?? [];
      group.push(rec);
      map.set(fp, group);
    }
    return map;
  }

  /**
   * Create a deterministic string key from a record's field values.
   * Fields are sorted alphabetically for consistency.
   */
  private buildFingerprint(
    record: SalesforceRecord,
    fingerprintFields: string[],
  ): string {
    return fingerprintFields
      .map((f) => `${f}=${this.normalize(record[f])}`)
      .join('|');
  }

  /**
   * Returns fields suitable for fingerprinting: non-system, non-reference,
   * non-auto-number, sorted alphabetically for deterministic key generation.
   */
  private getFingerprintFields(
    describe: ObjectDescribe,
    compareFields: string[],
  ): string[] {
    const autoNumberNames = new Set(
      describe.fields.filter((f) => f.autoNumber).map((f) => f.name),
    );
    return compareFields
      .filter((f) => !autoNumberNames.has(f))
      .sort();
  }

  /**
   * Returns the set of reference/lookup field names from the schema describe.
   */
  private getReferenceFields(describe: ObjectDescribe): Set<string> {
    const refs = new Set<string>();
    for (const f of describe.fields) {
      if (f.type === 'reference') {
        refs.add(f.name);
      }
    }
    return refs;
  }

  private compareRecords(
    sourceRec: SalesforceRecord,
    targetRec: SalesforceRecord,
    compareFields: string[],
  ): Record<string, { source: unknown; target: unknown }> {
    const changes: Record<string, { source: unknown; target: unknown }> = {};
    for (const field of compareFields) {
      const sv = this.normalize(sourceRec[field]);
      const tv = this.normalize(targetRec[field]);
      if (sv !== tv) {
        changes[field] = { source: sourceRec[field] ?? null, target: targetRec[field] ?? null };
      }
    }
    return changes;
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
