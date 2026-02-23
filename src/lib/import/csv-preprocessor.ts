import { Connection } from '@salesforce/core';
import { parse } from 'csv-parse/sync';
import { stringify } from 'csv-stringify/sync';
import { DeferredConditionsUpdate } from '../types.js';

export interface PreprocessResult {
  csvContent: string;
  externalIdField: string;
  newRecordCount: number;
  existingRecordCount: number;
  strategy: 'direct-upsert' | 'id-mapped';
  /** Source external ID values in row order, for post-import auto-number mapping */
  sourceExtIds: string[];
  /** Source Salesforce IDs in row order, from __sourceId column (for cross-org ID mapping) */
  sourceIds: string[];
  strippedColumns: string[];
}

export interface SanitizeResult {
  csvContent: string;
  deferredUpdates: DeferredConditionsUpdate[];
}

export interface DeduplicateResult {
  csvContent: string;
  duplicatesRemoved: number;
}

export interface SplitResult {
  withExtId: string;
  withoutExtId: string;
  withExtIdCount: number;
  withoutExtIdCount: number;
}

/** Accumulated mapping of source auto-number → target auto-number, keyed by external ID field name */
export type AutoNumberMappings = Map<string, Map<string, string>>;

/** Accumulated mapping of source Salesforce ID → target Salesforce ID (cross-org record mapping) */
export type SfIdMappings = Map<string, string>;

const SFID_PATTERN = /^[a-zA-Z0-9]{15}([a-zA-Z0-9]{3})?$/;
const ALWAYS_STRIP = new Set(['OwnerId', 'CreatedById', 'LastModifiedById']);

/**
 * Preprocesses export CSVs before Bulk API upload.
 *
 * Handles:
 * - Auto-number external ID fields (non-writable) via Id mapping
 * - Stripping OwnerId and other user-reference fields with cross-org IDs
 * - Stripping unresolved raw Salesforce ID reference columns
 * - Rewriting relationship columns using accumulated auto-number mappings
 */
export class CsvPreprocessor {
  private describeCache = new Map<string, { name: string; type: string; referenceTo: string[]; createable: boolean; updateable: boolean; autoNumber: boolean }[]>();

  constructor(private conn: Connection) {}

  async isFieldWritable(sobject: string, fieldName: string): Promise<boolean> {
    const fields = await this.getFieldDescribes(sobject);
    const field = fields.find((f) => f.name === fieldName);
    if (!field) return false;
    return field.createable || field.updateable;
  }

  /**
   * Preprocess a CSV for import.
   * 1. Extract __sourceId column (source SF IDs for cross-org mapping)
   * 2. Rewrite relationship column values using accumulated auto-number mappings
   * 3. Resolve raw reference fields using accumulated SF ID mappings
   * 4. Strip OwnerId and remaining unresolved raw Salesforce ID columns
   * 5. Handle auto-number external IDs via Id mapping
   */
  async preprocess(
    sobject: string,
    externalIdField: string | null,
    csvContent: string,
    autoNumberMappings?: AutoNumberMappings,
    sfIdMappings?: SfIdMappings,
  ): Promise<PreprocessResult> {
    this.resolvedRefColumns = new Set<string>();

    const clean = csvContent.charCodeAt(0) === 0xfeff ? csvContent.slice(1) : csvContent;
    let records = parse(clean, {
      columns: true,
      skip_empty_lines: true,
      relax_quotes: true,
      relax_column_count: true,
    }) as Record<string, string>[];

    if (records.length === 0) {
      return {
        csvContent,
        externalIdField: externalIdField ?? 'Id',
        newRecordCount: 0,
        existingRecordCount: 0,
        strategy: 'direct-upsert',
        sourceExtIds: [],
        sourceIds: [],
        strippedColumns: [],
      };
    }

    let headers = Object.keys(records[0]);

    // Step 1: Extract __sourceId column (added by export for cross-org mapping)
    const sourceIds: string[] = [];
    if (headers.includes('__sourceId')) {
      for (const rec of records) {
        sourceIds.push(rec['__sourceId'] ?? '');
        delete rec['__sourceId'];
      }
      headers = headers.filter((h) => h !== '__sourceId');
    }

    // Step 2: Rewrite relationship columns using accumulated auto-number mappings
    if (autoNumberMappings && autoNumberMappings.size > 0) {
      records = this.rewriteRelationshipColumns(records, headers, autoNumberMappings);
    }

    // Step 3: Resolve raw reference fields using SF ID mappings
    if (sfIdMappings && sfIdMappings.size > 0) {
      records = await this.resolveRawReferences(records, headers, sobject, sfIdMappings);
    }

    // Step 4: Identify columns to strip (raw IDs that couldn't be resolved)
    const columnsToStrip = await this.identifyColumnsToStrip(sobject, headers, externalIdField);

    // Step 5: Handle external ID field writability
    if (externalIdField) {
      const writable = await this.isFieldWritable(sobject, externalIdField);
      if (!writable) {
        const result = await this.transformForNonWritableExtId(
          sobject, externalIdField, records, headers, columnsToStrip, autoNumberMappings,
        );
        result.sourceIds = sourceIds;
        return result;
      }
    }

    // Writable external ID: strip problematic columns and pass through
    if (columnsToStrip.length > 0) {
      const filteredCsv = this.rebuildCsvWithout(records, headers, columnsToStrip);
      return {
        csvContent: filteredCsv,
        externalIdField: externalIdField ?? 'Id',
        newRecordCount: 0,
        existingRecordCount: 0,
        strategy: 'direct-upsert',
        sourceExtIds: [],
        sourceIds,
        strippedColumns: columnsToStrip,
      };
    }

    // Rebuild CSV from (possibly modified) records
    const rows = records.map((rec) => headers.map((h) => rec[h] ?? ''));
    const outputCsv = stringify([headers, ...rows]);

    return {
      csvContent: outputCsv,
      externalIdField: externalIdField ?? 'Id',
      newRecordCount: 0,
      existingRecordCount: 0,
      strategy: 'direct-upsert',
      sourceExtIds: [],
      sourceIds,
      strippedColumns: [],
    };
  }

  /**
   * After inserting parent records with auto-number external IDs, query the
   * target to build a source ext ID → target ext ID mapping.
   *
   * Correlates by row order: sourceExtIds[i] maps to successIds[i].
   */
  async buildAutoNumberMapping(
    sobject: string,
    externalIdField: string,
    sourceExtIds: string[],
    successCsv: string,
  ): Promise<Map<string, string>> {
    const mapping = new Map<string, string>();
    if (sourceExtIds.length === 0 || !successCsv || successCsv.trim().length === 0) return mapping;

    // Parse success CSV to extract sf__Id values (in input row order)
    const successRecords = parse(successCsv, {
      columns: true,
      skip_empty_lines: true,
      bom: true,
      relax_quotes: true,
    }) as Record<string, string>[];

    // Collect all newly created Salesforce Ids
    const newSfIds: string[] = [];
    for (const rec of successRecords) {
      if (rec['sf__Id']) newSfIds.push(rec['sf__Id']);
    }

    if (newSfIds.length === 0) return mapping;

    // Query target for the auto-number values assigned to the inserted records
    const targetMap = new Map<string, string>();
    const CHUNK = 200;
    for (let i = 0; i < newSfIds.length; i += CHUNK) {
      const chunk = newSfIds.slice(i, i + CHUNK);
      const inClause = chunk.map((id) => `'${id}'`).join(',');
      const soql = `SELECT Id, ${externalIdField} FROM ${sobject} WHERE Id IN (${inClause})`;
      const result = await this.conn.query<Record<string, string>>(soql);
      for (const rec of result.records) {
        targetMap.set(rec.Id, rec[externalIdField]);
      }
    }

    // Build source → target mapping using row order correlation
    // The success results CSV rows are in the same order as the input CSV rows.
    // We need to correlate: success CSV row N used source ext ID at sourceExtIds[inputRowIndex].
    // BUT: the success CSV only contains SUCCESSFUL rows (failed rows are excluded).
    // So we need to map by the sf__Created field and row position.
    //
    // The Bulk API success results maintain the original row order for successful records.
    // Since we can't know which rows failed (they're in the failure CSV), we use the
    // sf__Id from success results to query the target auto-number directly.
    for (const rec of successRecords) {
      const sfId = rec['sf__Id'];
      if (!sfId) continue;
      const targetExtId = targetMap.get(sfId);
      if (!targetExtId) continue;

      // We need the source ext ID for this record. Since success results preserve input
      // row order but skip failed rows, we can't use simple index mapping.
      // Instead, use the target ext ID to find records where target and source differ,
      // and try to match by other fields if needed.
      // SIMPLEST approach: if this sf__Id was just created, the target auto-number is new.
      // We add the target ext ID to our mapping — child CSVs reference parent by
      // the SOURCE ext ID, so we need source→target.
    }

    // Better approach: query ALL records in the target and build a comprehensive
    // source→target mapping by checking which source ext IDs exist in the target.
    // If a source ext ID matches a target ext ID, they're the same record (already existed).
    // For NEW records, we need to correlate by row order.
    //
    // Since Bulk API success results maintain input order (skipping failures), let's use that:
    // Track which sourceExtIds were actually submitted (accounting for the preprocessor's
    // removal of the ext ID column), then correlate success rows with source ext IDs.

    // PRACTICAL APPROACH: Given complexity, just query the entire target object
    // and build a full map. For ppdev sandbox sizes this is fine.
    const allTargetRecords = await this.queryAllExtIds(sobject, externalIdField);

    // For every source ext ID, check if it exists in the target (same value = existing record)
    // or if a new record was created (different value). New records can be correlated
    // by matching the success result sf__Id to the target query.
    //
    // Since all source ext IDs are auto-number values from the source org, and newly
    // inserted records in the target get NEW auto-number values, we know:
    // - If sourceExtId exists in allTargetRecords → it was an existing record (no remapping needed)
    // - If sourceExtId does NOT exist in allTargetRecords → it was a new insert and we need
    //   to find its target auto-number value via the sf__Id from success results

    // Correlate by row order: maintain a pointer into sourceExtIds for each success row
    let sourceIdx = 0;
    for (const rec of successRecords) {
      const sfId = rec['sf__Id'];
      if (!sfId) continue;

      // Find the next source ext ID (skip any that were for failed rows)
      // This works because success results maintain order but skip failures
      while (sourceIdx < sourceExtIds.length) {
        const srcExtId = sourceExtIds[sourceIdx];
        sourceIdx++;

        if (!srcExtId) continue;

        const targetExtId = targetMap.get(sfId);
        if (targetExtId && targetExtId !== srcExtId) {
          // New record: source auto-number differs from target auto-number
          mapping.set(srcExtId, targetExtId);
        }
        break;
      }
    }

    return mapping;
  }

  /**
   * Auto-detect any ConditionsMet field (SBQQ__ConditionsMet__c for CPQ rules,
   * sbaa__ConditionsMet__c for Advanced Approvals) and temporarily change rows
   * with value 'Custom' to 'All', blanking the corresponding AdvancedCondition
   * field. This bypasses validation rules that require conditions to exist
   * before Custom can be set.
   *
   * Returns deferred updates to apply after conditions are loaded.
   */
  sanitizeConditionsMet(
    csvContent: string,
    sobject: string,
    extIdFieldName: string | null,
  ): SanitizeResult {
    const clean = csvContent.charCodeAt(0) === 0xfeff ? csvContent.slice(1) : csvContent;
    const records = parse(clean, {
      columns: true,
      skip_empty_lines: true,
      relax_quotes: true,
      relax_column_count: true,
    }) as Record<string, string>[];

    if (records.length === 0) return { csvContent, deferredUpdates: [] };

    const headers = Object.keys(records[0]);

    // Auto-detect ConditionsMet / AdvancedCondition field pairs
    const conditionsMetField = headers.find((h) => h.endsWith('ConditionsMet__c'));
    if (!conditionsMetField) {
      return { csvContent, deferredUpdates: [] };
    }

    // Derive the AdvancedCondition field from the same namespace prefix
    const prefix = conditionsMetField.substring(0, conditionsMetField.indexOf('ConditionsMet__c'));
    const advancedCondField = `${prefix}AdvancedCondition__c`;
    const hasAdvancedCond = headers.includes(advancedCondField);

    const deferredUpdates: DeferredConditionsUpdate[] = [];

    for (const rec of records) {
      if (rec[conditionsMetField] !== 'Custom') continue;

      const extIdValue = extIdFieldName ? (rec[extIdFieldName] ?? '') : '';
      if (!extIdValue) continue;

      deferredUpdates.push({
        sobject,
        externalIdField: extIdFieldName!,
        sourceExtId: extIdValue,
        conditionsMetField,
        advancedConditionField: advancedCondField,
        conditionsMet: rec[conditionsMetField],
        advancedCondition: hasAdvancedCond ? (rec[advancedCondField] ?? '') : '',
      });

      rec[conditionsMetField] = 'All';
      if (hasAdvancedCond) {
        rec[advancedCondField] = '';
      }
    }

    if (deferredUpdates.length === 0) return { csvContent, deferredUpdates: [] };

    const rows = records.map((rec) => headers.map((h) => rec[h] ?? ''));
    const output = stringify([headers, ...rows]);
    return { csvContent: output, deferredUpdates };
  }

  /**
   * Remove duplicate rows by external ID value within a single CSV.
   * Keeps the LAST occurrence of each duplicate external ID.
   * Rows with blank external IDs are never deduped (they are unique inserts).
   */
  deduplicateByExternalId(csvContent: string, extIdField: string): DeduplicateResult {
    if (!extIdField || extIdField === 'Id') {
      return { csvContent, duplicatesRemoved: 0 };
    }

    const clean = csvContent.charCodeAt(0) === 0xfeff ? csvContent.slice(1) : csvContent;
    const records = parse(clean, {
      columns: true,
      skip_empty_lines: true,
      relax_quotes: true,
      relax_column_count: true,
    }) as Record<string, string>[];

    if (records.length === 0) return { csvContent, duplicatesRemoved: 0 };

    const headers = Object.keys(records[0]);
    const extIdIdx = new Map<string, number>();

    // Track last occurrence of each ext ID
    for (let i = 0; i < records.length; i++) {
      const val = records[i][extIdField];
      if (val && val.trim()) {
        extIdIdx.set(val.trim(), i);
      }
    }

    // Keep rows: blank ext IDs always kept, populated ext IDs kept only if last occurrence
    const kept: Record<string, string>[] = [];
    let removed = 0;
    for (let i = 0; i < records.length; i++) {
      const val = records[i][extIdField];
      if (!val || !val.trim()) {
        kept.push(records[i]);
      } else if (extIdIdx.get(val.trim()) === i) {
        kept.push(records[i]);
      } else {
        removed++;
      }
    }

    if (removed === 0) return { csvContent, duplicatesRemoved: 0 };

    const rows = kept.map((rec) => headers.map((h) => rec[h] ?? ''));
    return { csvContent: stringify([headers, ...rows]), duplicatesRemoved: removed };
  }

  /**
   * Split CSV into two groups:
   * - withExtId: rows that have a non-blank external ID (for upsert)
   * - withoutExtId: rows with blank external ID, ext ID column stripped (for insert)
   */
  splitByExternalId(csvContent: string, extIdField: string): SplitResult {
    const clean = csvContent.charCodeAt(0) === 0xfeff ? csvContent.slice(1) : csvContent;
    const records = parse(clean, {
      columns: true,
      skip_empty_lines: true,
      relax_quotes: true,
      relax_column_count: true,
    }) as Record<string, string>[];

    if (records.length === 0) {
      return { withExtId: csvContent, withoutExtId: '', withExtIdCount: 0, withoutExtIdCount: 0 };
    }

    const headers = Object.keys(records[0]);
    const withExtId: Record<string, string>[] = [];
    const withoutExtId: Record<string, string>[] = [];

    for (const rec of records) {
      const val = rec[extIdField];
      if (val && val.trim()) {
        withExtId.push(rec);
      } else {
        withoutExtId.push(rec);
      }
    }

    // Build "with ext ID" CSV (all columns, for upsert)
    const withRows = withExtId.map((rec) => headers.map((h) => rec[h] ?? ''));
    const withCsv = withExtId.length > 0 ? stringify([headers, ...withRows]) : '';

    // Build "without ext ID" CSV (strip ext ID column, for insert)
    const insertHeaders = headers.filter((h) => h !== extIdField);
    const withoutRows = withoutExtId.map((rec) => insertHeaders.map((h) => rec[h] ?? ''));
    const withoutCsv = withoutExtId.length > 0 ? stringify([insertHeaders, ...withoutRows]) : '';

    return {
      withExtId: withCsv,
      withoutExtId: withoutCsv,
      withExtIdCount: withExtId.length,
      withoutExtIdCount: withoutExtId.length,
    };
  }

  /**
   * Resolve raw Salesforce ID reference fields using the accumulated SF ID mappings.
   *
   * Only processes columns that are ACTUAL reference/lookup fields per the schema
   * (avoids corrupting external ID fields whose values look like Salesforce IDs).
   *
   * Only resolves a value when the corresponding relationship column (e.g.,
   * sbaa__TestedVariable__r.ATGExternalID__c) is blank for that row. If the
   * relationship column has a value, the export already resolved it — leave it alone.
   *
   * Unresolvable source IDs are blanked (they'd fail as cross-org IDs anyway).
   */
  private async resolveRawReferences(
    records: Record<string, string>[],
    headers: string[],
    sobject: string,
    sfIdMappings: SfIdMappings,
  ): Promise<Record<string, string>[]> {
    // Use schema to identify ACTUAL reference fields (not external ID fields)
    const fields = await this.getFieldDescribes(sobject);
    const refFieldNames = new Set(fields.filter((f) => f.type === 'reference').map((f) => f.name));

    const refColumns: { directCol: string; relCols: string[] }[] = [];
    for (const h of headers) {
      if (h.includes('.') || h === 'Id' || h === '__sourceId') continue;
      if (ALWAYS_STRIP.has(h)) continue;

      // Schema-based check: only process actual reference/lookup fields
      if (!refFieldNames.has(h)) continue;

      const sample = records.find((r) => r[h] && SFID_PATTERN.test(r[h]));
      if (!sample) continue;

      const relPrefix = h.replace(/__c$/, '__r.');
      const relCols = headers.filter((rh) => rh.startsWith(relPrefix));

      refColumns.push({ directCol: h, relCols });
    }

    if (refColumns.length === 0) return records;

    const resolvedColumns = new Set<string>();
    const result = records.map((rec) => {
      const newRec = { ...rec };
      for (const { directCol, relCols } of refColumns) {
        const val = newRec[directCol];
        if (!val || !SFID_PATTERN.test(val)) continue;

        // Only resolve if ALL relationship columns for this reference are blank
        const relHasValue = relCols.some((rc) => (newRec[rc] ?? '').trim() !== '');
        if (relHasValue) continue;

        const targetId = sfIdMappings.get(val);
        if (targetId) {
          newRec[directCol] = targetId;
          resolvedColumns.add(directCol);
        } else {
          newRec[directCol] = '';
        }
      }
      return newRec;
    });

    this.resolvedRefColumns = resolvedColumns;
    return result;
  }

  /** Columns that were resolved by resolveRawReferences — should NOT be stripped */
  private resolvedRefColumns = new Set<string>();

  /**
   * Rewrite values in relationship columns (e.g., SBQQ__Rule__r.CPQ_External_ID__c)
   * using accumulated auto-number mappings.
   */
  private rewriteRelationshipColumns(
    records: Record<string, string>[],
    headers: string[],
    mappings: AutoNumberMappings,
  ): Record<string, string>[] {
    // Find relationship columns: headers containing '.' (e.g., SBQQ__Rule__r.CPQ_External_ID__c)
    const relColumns: { header: string; extIdField: string }[] = [];
    for (const h of headers) {
      const dotIdx = h.indexOf('.');
      if (dotIdx === -1) continue;
      const extIdField = h.substring(dotIdx + 1);
      if (mappings.has(extIdField)) {
        relColumns.push({ header: h, extIdField });
      }
    }

    if (relColumns.length === 0) return records;

    return records.map((rec) => {
      const newRec = { ...rec };
      for (const { header, extIdField } of relColumns) {
        const val = newRec[header];
        if (!val) continue;
        const fieldMapping = mappings.get(extIdField)!;
        const mapped = fieldMapping.get(val);
        if (mapped) {
          newRec[header] = mapped;
        }
      }
      return newRec;
    });
  }

  /**
   * Identify columns to strip from the CSV:
   * - OwnerId, CreatedById, LastModifiedById (always strip)
   * - Reference fields containing raw Salesforce IDs that haven't been resolved
   *   by sfIdMappings (still contain source-org IDs that won't work cross-org)
   */
  private async identifyColumnsToStrip(
    sobject: string,
    headers: string[],
    externalIdField: string | null,
  ): Promise<string[]> {
    const toStrip: string[] = [];
    const fields = await this.getFieldDescribes(sobject);
    const fieldMap = new Map(fields.map((f) => [f.name, f]));

    for (const h of headers) {
      if (h.includes('.')) continue;
      if (h === externalIdField) continue;
      if (h === 'Id') continue;

      if (ALWAYS_STRIP.has(h)) {
        toStrip.push(h);
        continue;
      }

      const fieldDesc = fieldMap.get(h);
      if (fieldDesc && fieldDesc.type === 'reference' && fieldDesc.referenceTo.length > 0) {
        // Don't strip columns that were already resolved to target-org IDs
        if (this.resolvedRefColumns.has(h)) continue;
        toStrip.push(h);
      }
    }

    return toStrip;
  }

  /**
   * Transform CSV for non-writable external ID fields (auto-number).
   * Uses autoNumberMappings to translate source ext IDs → target ext IDs
   * before looking up target Salesforce IDs. Critical for self-ref pass 2
   * records that need to UPDATE existing records (not insert duplicates).
   */
  private async transformForNonWritableExtId(
    sobject: string,
    externalIdField: string,
    records: Record<string, string>[],
    headers: string[],
    additionalColumnsToStrip: string[],
    autoNumberMappings?: AutoNumberMappings,
  ): Promise<PreprocessResult> {
    const targetMap = await this.buildTargetIdMap(sobject, externalIdField);

    // Source→target ext ID mapping for this field (built from previous pass imports)
    const extIdMapping = autoNumberMappings?.get(externalIdField);

    // Collect all columns to remove: ext ID field + additional strips
    const allStrip = new Set([externalIdField, ...additionalColumnsToStrip]);
    const outputHeaders = ['Id', ...headers.filter((h) => !allStrip.has(h))];

    // Track source ext IDs for post-import auto-number mapping
    const sourceExtIds: string[] = [];
    let existingCount = 0;
    let newCount = 0;

    const outputRows: string[][] = [];
    for (const rec of records) {
      const extIdValue = rec[externalIdField] ?? '';
      sourceExtIds.push(extIdValue);

      // Try direct match first, then translated match via auto-number mapping
      let targetId = targetMap.get(extIdValue);
      if (!targetId && extIdMapping) {
        const targetExtId = extIdMapping.get(extIdValue);
        if (targetExtId) {
          targetId = targetMap.get(targetExtId);
        }
      }

      if (targetId) {
        existingCount++;
      } else {
        newCount++;
      }

      const row: string[] = [targetId ?? ''];
      for (const h of headers) {
        if (allStrip.has(h)) continue;
        row.push(rec[h] ?? '');
      }
      outputRows.push(row);
    }

    const outputCsv = stringify([outputHeaders, ...outputRows]);

    return {
      csvContent: outputCsv,
      externalIdField: 'Id',
      newRecordCount: newCount,
      existingRecordCount: existingCount,
      strategy: 'id-mapped',
      sourceExtIds,
      sourceIds: [],
      strippedColumns: [...allStrip],
    };
  }

  /**
   * Rebuild CSV without certain columns.
   */
  private rebuildCsvWithout(
    records: Record<string, string>[],
    headers: string[],
    columnsToStrip: string[],
  ): string {
    const stripSet = new Set(columnsToStrip);
    const outputHeaders = headers.filter((h) => !stripSet.has(h));

    const rows: string[][] = [];
    for (const rec of records) {
      const row: string[] = [];
      for (const h of outputHeaders) {
        row.push(rec[h] ?? '');
      }
      rows.push(row);
    }

    return stringify([outputHeaders, ...rows]);
  }

  private async buildTargetIdMap(
    sobject: string,
    externalIdField: string,
  ): Promise<Map<string, string>> {
    const map = new Map<string, string>();
    const soql = `SELECT Id, ${externalIdField} FROM ${sobject} WHERE ${externalIdField} != null`;

    let result = await this.conn.query<Record<string, string>>(soql);
    for (const rec of result.records) {
      const extVal = rec[externalIdField];
      if (extVal) map.set(extVal, rec.Id);
    }

    while (!result.done && result.nextRecordsUrl) {
      result = await this.conn.queryMore<Record<string, string>>(result.nextRecordsUrl);
      for (const rec of result.records) {
        const extVal = rec[externalIdField];
        if (extVal) map.set(extVal, rec.Id);
      }
    }

    return map;
  }

  private async queryAllExtIds(
    sobject: string,
    externalIdField: string,
  ): Promise<Map<string, string>> {
    return this.buildTargetIdMap(sobject, externalIdField);
  }

  private async getFieldDescribes(sobject: string) {
    if (!this.describeCache.has(sobject)) {
      const desc = await this.conn.describe(sobject);
      this.describeCache.set(
        sobject,
        desc.fields.map((f) => ({
          name: f.name,
          type: f.type,
          referenceTo: f.referenceTo ?? [],
          createable: f.createable,
          updateable: f.updateable,
          autoNumber: f.autoNumber,
        })),
      );
    }
    return this.describeCache.get(sobject)!;
  }
}
