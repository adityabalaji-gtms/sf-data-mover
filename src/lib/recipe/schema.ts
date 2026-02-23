import { Recipe, RecipeObject, RecipeSettings } from '../types.js';

export const DEFAULT_EXCLUDE_FIELDS: string[] = [
  'Id', 'IsDeleted',
  'CreatedDate', 'CreatedById',
  'LastModifiedDate', 'LastModifiedById',
  'SystemModstamp',
];

export const DEFAULT_SETTINGS: RecipeSettings = {
  defaultExcludeFields: DEFAULT_EXCLUDE_FIELDS,
  batchSize: 200,
  selfReferenceStrategy: 'two-pass',
};

export function validateRecipe(recipe: unknown): { valid: boolean; errors: string[] } {
  const errors: string[] = [];

  if (!recipe || typeof recipe !== 'object') {
    return { valid: false, errors: ['Recipe must be a JSON object'] };
  }

  const r = recipe as Record<string, unknown>;

  if (typeof r.name !== 'string' || !r.name) errors.push('Missing required field: name');
  if (typeof r.version !== 'string') errors.push('Missing required field: version');
  if (!Array.isArray(r.objects) || r.objects.length === 0) errors.push('objects must be a non-empty array');

  if (Array.isArray(r.objects)) {
    for (let i = 0; i < r.objects.length; i++) {
      const obj = r.objects[i] as Record<string, unknown>;
      if (typeof obj.sobject !== 'string' || !obj.sobject) {
        errors.push(`objects[${i}]: missing sobject`);
      }
      if (!('externalIdField' in obj)) {
        errors.push(`objects[${i}]: missing externalIdField (use null for composite key objects)`);
      }
    }
  }

  return { valid: errors.length === 0, errors };
}

export function buildEmptyRecipe(name: string, description: string): Recipe {
  return {
    name,
    version: '1.0',
    description,
    objects: [],
    settings: { ...DEFAULT_SETTINGS },
  };
}

export function addObjectToRecipe(
  recipe: Recipe,
  sobject: string,
  externalIdField: string | null,
  options?: Partial<RecipeObject>,
): void {
  recipe.objects.push({
    sobject,
    externalIdField,
    ...options,
  });
}
