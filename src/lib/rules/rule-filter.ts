import { Connection } from '@salesforce/core';
import { Recipe, RecipeObject, DependencyGraph } from '../types.js';
import { DataFetcher } from '../data/data-fetcher.js';
import { SchemaDescriber } from '../schema/describer.js';

const IN_CLAUSE_CHUNK = 200;

export interface RuleFilterOptions {
  /** When true, generates cross-org-safe filters (Name/ExternalId) instead of Id-based filters */
  forCompare?: boolean;
}

/**
 * Filters a recipe to only include records related to specific "root" rules.
 *
 * Given a root SObject (e.g., SBQQ__PriceRule__c) and identifiers (Names or IDs),
 * walks the dependency graph to discover all related child and upstream records,
 * then returns a new Recipe with SOQL WHERE filters on every object.
 *
 * Supports two filter modes:
 * - Export mode (default): `Id IN (...)` — fast, single-org only
 * - Compare mode (`forCompare: true`): `ExternalId IN (...)` or `Name IN (...)` — works across orgs
 */
export class RuleFilter {
  private fetcher: DataFetcher;
  private describer: SchemaDescriber;
  private selectedIds = new Map<string, Set<string>>();
  private recipeObjects: Set<string>;

  private rootSobject = '';
  private rootNames: string[] = [];

  private selectedExtIdValues = new Map<string, Set<string>>();
  private selectedNameValues = new Map<string, Set<string>>();
  private autoNumberFields = new Set<string>();

  constructor(
    private conn: Connection,
    private recipe: Recipe,
    private graph: DependencyGraph,
  ) {
    this.fetcher = new DataFetcher(conn);
    this.describer = new SchemaDescriber(conn);
    this.recipeObjects = new Set(recipe.objects.map((o) => o.sobject));
  }

  async filter(
    rootSobject: string,
    identifiers: string[],
    matchBy: 'name' | 'id',
    onProgress?: (message: string) => void,
    options?: RuleFilterOptions,
  ): Promise<Recipe> {
    if (!this.recipeObjects.has(rootSobject)) {
      throw new Error(
        `Root object ${rootSobject} is not in the recipe. ` +
        `Available: ${[...this.recipeObjects].join(', ')}`,
      );
    }

    this.rootSobject = rootSobject;

    // Phase 1: Resolve root IDs
    onProgress?.(`Resolving ${identifiers.length} ${rootSobject} by ${matchBy}`);
    await this.resolveRootIds(rootSobject, identifiers, matchBy);

    // Phase 2: Walk graph downward — find children of root, then grandchildren
    onProgress?.('Discovering child records');
    await this.walkDownward(rootSobject);

    // Phase 3: Walk graph upward — find upstream dependencies referenced by selected records
    onProgress?.('Discovering upstream dependencies');
    await this.walkUpward();

    // Phase 3.5: For compare mode, collect cross-org-safe identifiers
    if (options?.forCompare) {
      onProgress?.('Collecting cross-org identifiers');
      await this.collectCrossOrgKeys();
    }

    // Phase 4: Build filtered recipe
    return options?.forCompare
      ? this.buildCompareFilteredRecipe()
      : this.buildFilteredRecipe();
  }

  /**
   * Phase 1: Query the source org for the root records by Name or Id.
   * Also stores root Names for cross-org filtering.
   */
  private async resolveRootIds(
    rootSobject: string,
    identifiers: string[],
    matchBy: 'name' | 'id',
  ): Promise<void> {
    const field = matchBy === 'name' ? 'Name' : 'Id';
    const escaped = identifiers.map((v) => v.replace(/'/g, "\\'"));
    const inClause = escaped.map((v) => `'${v}'`).join(',');

    const soql = `SELECT Id, Name FROM ${rootSobject} WHERE ${field} IN (${inClause})`;
    const records = await this.fetcher.fetchAll(soql);

    if (records.length === 0) {
      throw new Error(
        `No ${rootSobject} records found matching ${field} IN (${identifiers.join(', ')}). ` +
        'Check spelling and ensure the records exist in the source org.',
      );
    }

    this.selectedIds.set(rootSobject, new Set(records.map((r) => r.Id)));
    this.rootNames = records.map((r) => String(r.Name ?? '')).filter(Boolean);
  }

  /**
   * Phase 2: Walk the dependency graph downward from the root.
   */
  private async walkDownward(rootSobject: string): Promise<void> {
    let changed = true;
    while (changed) {
      changed = false;

      for (const recipeObj of this.recipe.objects) {
        if (this.selectedIds.has(recipeObj.sobject)) continue;

        const childEdges = this.graph.edges.filter(
          (e) => e.from === recipeObj.sobject && this.selectedIds.has(e.to),
        );

        if (childEdges.length === 0) continue;

        const edge = childEdges[0];
        const parentIds = this.selectedIds.get(edge.to)!;
        const childIds = await this.queryByParentIds(
          recipeObj.sobject, edge.field, parentIds, recipeObj.filter,
        );

        if (childIds.size > 0) {
          this.selectedIds.set(recipeObj.sobject, childIds);
          changed = true;
        }
      }
    }
  }

  /**
   * Phase 3: Walk the graph upward to find upstream objects referenced
   * by already-selected records.
   */
  private async walkUpward(): Promise<void> {
    for (const recipeObj of this.recipe.objects) {
      if (this.selectedIds.has(recipeObj.sobject)) continue;

      const upstreamEdges = this.graph.edges.filter(
        (e) => e.to === recipeObj.sobject && this.selectedIds.has(e.from),
      );

      if (upstreamEdges.length === 0) continue;

      const referencedIds = new Set<string>();
      for (const edge of upstreamEdges) {
        const childIds = this.selectedIds.get(edge.from)!;
        const ids = await this.collectReferencedIds(
          edge.from, edge.field, childIds,
        );
        for (const id of ids) referencedIds.add(id);
      }

      if (referencedIds.size > 0) {
        this.selectedIds.set(recipeObj.sobject, referencedIds);
      }
    }
  }

  /**
   * Phase 3.5: For compare mode, collect external ID values and Names from
   * the source org for each selected object. Also detects auto-number fields.
   */
  private async collectCrossOrgKeys(): Promise<void> {
    for (const [sobject, ids] of this.selectedIds) {
      if (sobject === this.rootSobject) continue;

      const recipeObj = this.recipe.objects.find((o) => o.sobject === sobject);
      const extIdField = recipeObj?.externalIdField;

      // Check if external ID is auto-number
      if (extIdField) {
        const desc = await this.describer.describe(sobject);
        const fieldDesc = desc.fields.find((f) => f.name === extIdField);
        if (fieldDesc?.autoNumber) {
          this.autoNumberFields.add(`${sobject}.${extIdField}`);
        }
      }

      const fieldsToQuery = ['Name'];
      if (extIdField) fieldsToQuery.push(extIdField);

      const extIdValues = new Set<string>();
      const names = new Set<string>();

      for (const chunk of this.chunkIds([...ids])) {
        const inClause = chunk.map((id) => `'${id}'`).join(',');
        const soql = `SELECT ${fieldsToQuery.join(',')} FROM ${sobject} WHERE Id IN (${inClause})`;
        const records = await this.fetcher.fetchAll(soql);
        for (const r of records) {
          if (r.Name) names.add(String(r.Name));
          if (extIdField && r[extIdField]) extIdValues.add(String(r[extIdField]));
        }
      }

      this.selectedExtIdValues.set(sobject, extIdValues);
      this.selectedNameValues.set(sobject, names);
    }
  }

  /**
   * Phase 4 (export mode): Build recipe with `Id IN (...)` filters.
   */
  private buildFilteredRecipe(): Recipe {
    const filteredObjects: RecipeObject[] = [];

    for (const obj of this.recipe.objects) {
      const ids = this.selectedIds.get(obj.sobject);
      if (!ids || ids.size === 0) continue;

      filteredObjects.push({
        ...obj,
        filter: this.buildInClause('Id', ids),
      });
    }

    if (filteredObjects.length === 0) {
      throw new Error('No related records found for the selected rules.');
    }

    return {
      ...this.recipe,
      objects: filteredObjects,
      description: `${this.recipe.description} [filtered]`,
    };
  }

  /**
   * Phase 4 (compare mode): Build recipe with cross-org-safe filters.
   * Uses Name for root + auto-number objects, ExternalId for others.
   */
  private buildCompareFilteredRecipe(): Recipe {
    const filteredObjects: RecipeObject[] = [];

    for (const obj of this.recipe.objects) {
      const ids = this.selectedIds.get(obj.sobject);
      if (!ids || ids.size === 0) continue;

      let filter: string;

      if (obj.sobject === this.rootSobject) {
        filter = this.buildInClause('Name', new Set(this.rootNames));
      } else {
        const isAutoNumber = this.autoNumberFields.has(`${obj.sobject}.${obj.externalIdField}`);
        const extIdValues = this.selectedExtIdValues.get(obj.sobject);

        if (!isAutoNumber && extIdValues && extIdValues.size > 0 && obj.externalIdField) {
          filter = this.buildInClause(obj.externalIdField, extIdValues);
        } else {
          // Auto-number or no external ID: use Name as fallback
          const names = this.selectedNameValues.get(obj.sobject);
          if (names && names.size > 0) {
            filter = this.buildInClause('Name', names);
          } else {
            filter = this.buildInClause('Id', ids);
          }
        }
      }

      filteredObjects.push({ ...obj, filter });
    }

    if (filteredObjects.length === 0) {
      throw new Error('No related records found for the selected rules.');
    }

    return {
      ...this.recipe,
      objects: filteredObjects,
      description: `${this.recipe.description} [filtered]`,
    };
  }

  private async queryByParentIds(
    sobject: string,
    lookupField: string,
    parentIds: Set<string>,
    existingFilter?: string,
  ): Promise<Set<string>> {
    const ids = new Set<string>();

    for (const chunk of this.chunkIds([...parentIds])) {
      const inClause = chunk.map((id) => `'${id}'`).join(',');
      let where = `${lookupField} IN (${inClause})`;
      if (existingFilter) {
        where = `(${where}) AND (${existingFilter})`;
      }

      const soql = `SELECT Id FROM ${sobject} WHERE ${where}`;
      const records = await this.fetcher.fetchAll(soql);
      for (const r of records) ids.add(r.Id);
    }

    return ids;
  }

  private async collectReferencedIds(
    childSobject: string,
    referenceField: string,
    childIds: Set<string>,
  ): Promise<Set<string>> {
    const refIds = new Set<string>();

    for (const chunk of this.chunkIds([...childIds])) {
      const inClause = chunk.map((id) => `'${id}'`).join(',');
      const soql = `SELECT ${referenceField} FROM ${childSobject} WHERE Id IN (${inClause}) AND ${referenceField} != null`;
      const records = await this.fetcher.fetchAll(soql);
      for (const r of records) {
        const val = r[referenceField];
        if (typeof val === 'string' && val) refIds.add(val);
      }
    }

    return refIds;
  }

  private buildInClause(field: string, ids: Set<string>): string {
    const escaped = [...ids].map((v) => v.replace(/'/g, "\\'"));
    return `${field} IN (${escaped.map((v) => `'${v}'`).join(',')})`;
  }

  private chunkIds(ids: string[]): string[][] {
    const chunks: string[][] = [];
    for (let i = 0; i < ids.length; i += IN_CLAUSE_CHUNK) {
      chunks.push(ids.slice(i, i + IN_CLAUSE_CHUNK));
    }
    return chunks.length > 0 ? chunks : [[]];
  }
}
