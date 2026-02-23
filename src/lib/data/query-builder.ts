import { ObjectDescribe, RecipeObject, RecipeSettings } from '../types.js';

const FORMULA_TYPES = new Set(['formula', 'address']);
const ALWAYS_EXCLUDE = new Set([
  'IsDeleted', 'SystemModstamp',
  'CreatedDate', 'CreatedById',
  'LastModifiedDate', 'LastModifiedById',
]);

/**
 * Builds SOQL queries from recipe + schema metadata.
 * Automatically excludes formula fields, system fields, and non-createable fields.
 */
export class QueryBuilder {
  buildQuery(
    recipeObj: RecipeObject,
    describe: ObjectDescribe,
    settings: RecipeSettings,
  ): string {
    const fields = this.selectFields(recipeObj, describe, settings);
    let soql = `SELECT ${fields.join(', ')} FROM ${recipeObj.sobject}`;

    if (recipeObj.filter) {
      soql += ` WHERE ${recipeObj.filter}`;
    }

    return soql;
  }

  /**
   * Returns the list of field API names to query.
   * Includes Id (needed for ID map building) and all createable/updateable non-formula fields.
   */
  selectFields(
    recipeObj: RecipeObject,
    describe: ObjectDescribe,
    settings: RecipeSettings,
  ): string[] {
    const excludeSet = new Set<string>([
      ...settings.defaultExcludeFields,
      ...(recipeObj.excludeFields ?? []),
    ]);

    // Always keep Id — we need it for the ID resolution map
    excludeSet.delete('Id');

    // Always keep the external ID field — needed for ID resolution and comparison,
    // even if it's auto-number or non-createable.
    const extIdField = recipeObj.externalIdField;
    if (extIdField) excludeSet.delete(extIdField);

    if (recipeObj.includeFields?.length) {
      const fields = ['Id', ...recipeObj.includeFields.filter((f) => f !== 'Id')];
      if (extIdField && !fields.includes(extIdField)) fields.splice(1, 0, extIdField);
      return fields;
    }

    const fields: string[] = ['Id'];

    for (const f of describe.fields) {
      if (f.name === 'Id') continue;
      if (f.name === extIdField) {
        fields.push(f.name);
        continue;
      }
      if (excludeSet.has(f.name)) continue;
      if (ALWAYS_EXCLUDE.has(f.name)) continue;
      if (f.calculated && f.type !== 'reference') continue;
      if (FORMULA_TYPES.has(f.type)) continue;
      if (f.autoNumber) continue;
      if (!f.createable && !f.updateable && f.type !== 'reference') continue;

      fields.push(f.name);
    }

    return fields;
  }

  /**
   * Build the SOQL that fetches just Id + externalIdField for a given object.
   * Used to build the ID resolution map.
   */
  buildIdMapQuery(sobject: string, externalIdField: string, filter?: string): string {
    let soql = `SELECT Id, ${externalIdField} FROM ${sobject}`;
    if (filter) soql += ` WHERE ${filter}`;
    return soql;
  }
}
