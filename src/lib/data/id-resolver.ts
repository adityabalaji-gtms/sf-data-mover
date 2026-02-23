import { Connection } from '@salesforce/core';
import { ObjectDescribe, Recipe, RecipeObject, GraphNode, DependencyGraph, FieldDescribe } from '../types.js';
import { DataFetcher } from './data-fetcher.js';

type SalesforceRecord = Record<string, unknown> & { Id: string };

/**
 * Builds an in-memory map of {SalesforceId -> ExternalIdValue} for every
 * object in the recipe, then rewrites record references to use external IDs
 * and transforms column headers to Inspector-compatible relationship notation.
 */
export class IdResolver {
  /** sobject -> { sfId -> externalIdValue } */
  private idMaps = new Map<string, Map<string, string>>();

  /** sobject -> externalIdField */
  private extIdFields = new Map<string, string>();

  constructor(
    private conn: Connection,
    private graph: DependencyGraph,
    private recipe: Recipe,
  ) {}

  /**
   * Phase 1: Build ID-to-external-key maps for all objects in the recipe
   * that have an externalIdField.
   */
  async buildIdMaps(
    onProgress?: (sobject: string, count: number) => void,
  ): Promise<void> {
    const fetcher = new DataFetcher(this.conn);

    for (const obj of this.recipe.objects) {
      if (!obj.externalIdField) continue;

      this.extIdFields.set(obj.sobject, obj.externalIdField);

      const soql = `SELECT Id, ${obj.externalIdField} FROM ${obj.sobject}${obj.filter ? ' WHERE ' + obj.filter : ''}`;
      const records = await fetcher.fetchAll(soql);

      const map = new Map<string, string>();
      for (const rec of records) {
        const extVal = rec[obj.externalIdField];
        if (extVal != null && extVal !== '') {
          map.set(rec.Id, String(extVal));
        }
      }

      this.idMaps.set(obj.sobject, map);
      onProgress?.(obj.sobject, map.size);
    }
  }

  /**
   * Phase 2: For a set of records of a given object, replace Salesforce ID
   * values in reference fields with external ID values, and rewrite the
   * column headers from "FieldId__c" to "Relationship__r.ExternalId__c".
   */
  resolveRecords(
    sobject: string,
    records: SalesforceRecord[],
    describe: ObjectDescribe,
  ): { headers: string[]; rows: Record<string, unknown>[]; selfRefField?: string } {
    const recipeObj = this.recipe.objects.find((o) => o.sobject === sobject);
    const node = this.graph.nodes.get(sobject);
    if (!recipeObj || !node) {
      throw new Error(`Object ${sobject} not found in recipe/graph`);
    }

    const referenceFields = describe.fields.filter(
      (f) => f.type === 'reference' && f.referenceTo.length > 0
    );

    const selfRefFields = node.selfReferences.map((sr) => sr.field);

    const headerMap = new Map<string, string>();
    const resolvedRows: Record<string, unknown>[] = [];

    for (const record of records) {
      const resolved: Record<string, unknown> = {};

      for (const [key, value] of Object.entries(record)) {
        // Preserve source Salesforce Id as __sourceId for cross-org mapping during import
        if (key === 'Id') {
          resolved['__sourceId'] = value;
          continue;
        }

        const refField = referenceFields.find((f) => f.name === key);

        if (refField && value && typeof value === 'string') {
          if (selfRefFields.includes(key)) {
            // Self-references handled in pass 2
            continue;
          }

          const targetObj = refField.referenceTo[0];
          const targetExtId = this.extIdFields.get(targetObj);
          const targetMap = this.idMaps.get(targetObj);

          if (targetExtId && targetMap) {
            const extValue = targetMap.get(value);
            if (extValue) {
              const relName = refField.relationshipName ?? key.replace(/__c$/, '__r');
              const header = `${relName}.${targetExtId}`;
              headerMap.set(key, header);
              resolved[header] = extValue;
              continue;
            }
          }
        }

        headerMap.set(key, key);
        resolved[key] = value;
      }

      resolvedRows.push(resolved);
    }

    // Build ordered headers: external ID first, then the rest
    const allHeaders = new Set<string>();
    for (const row of resolvedRows) {
      for (const key of Object.keys(row)) {
        allHeaders.add(key);
      }
    }

    const extIdField = recipeObj.externalIdField;
    const headers: string[] = [];
    if (extIdField && allHeaders.has(extIdField)) {
      headers.push(extIdField);
      allHeaders.delete(extIdField);
    }
    headers.push(...[...allHeaders].sort());

    return {
      headers,
      rows: resolvedRows,
      selfRefField: selfRefFields.length > 0 ? selfRefFields[0] : undefined,
    };
  }

  /**
   * Build self-reference pass-2 rows: only the external ID + self-ref field(s),
   * resolved to external ID values.
   */
  buildSelfRefPass2(
    sobject: string,
    records: SalesforceRecord[],
    describe: ObjectDescribe,
  ): { headers: string[]; rows: Record<string, unknown>[] } | null {
    const recipeObj = this.recipe.objects.find((o) => o.sobject === sobject);
    const node = this.graph.nodes.get(sobject);
    if (!recipeObj || !node || node.selfReferences.length === 0) return null;

    const extIdField = recipeObj.externalIdField;
    if (!extIdField) return null;

    const selfMap = this.idMaps.get(sobject);
    if (!selfMap) return null;

    const rows: Record<string, unknown>[] = [];

    for (const sr of node.selfReferences) {
      const refField = describe.fields.find((f) => f.name === sr.field);
      if (!refField) continue;

      const relName = refField.relationshipName ?? sr.field.replace(/__c$/, '__r');
      const header = `${relName}.${extIdField}`;

      for (const record of records) {
        const selfVal = record[sr.field];
        if (!selfVal || typeof selfVal !== 'string') continue;

        const extVal = selfMap.get(selfVal);
        if (!extVal) continue;

        const recordExtId = record[extIdField];
        if (!recordExtId) continue;

        rows.push({
          [extIdField]: recordExtId,
          [header]: extVal,
        });
      }

      if (rows.length > 0) {
        return { headers: [extIdField, header], rows };
      }
    }

    return null;
  }
}
