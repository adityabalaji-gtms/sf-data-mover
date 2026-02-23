import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { SfCommand, Flags } from '@salesforce/sf-plugins-core';
import { validateRecipe } from '../../../lib/recipe/schema.js';

export default class RecipeValidate extends SfCommand<{ valid: boolean; errors: string[] }> {
  public static readonly summary = 'Validate a migration recipe JSON file.';

  public static readonly examples = [
    'sf data-mover recipe validate --recipe recipes/cpq-full.json',
  ];

  public static readonly flags = {
    recipe: Flags.string({
      summary: 'Path to the recipe JSON file.',
      char: 'r',
      required: true,
    }),
  };

  public async run(): Promise<{ valid: boolean; errors: string[] }> {
    const { flags } = await this.parse(RecipeValidate);
    const recipePath = resolve(flags.recipe);

    let raw: unknown;
    try {
      raw = JSON.parse(readFileSync(recipePath, 'utf-8'));
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      this.error(`Failed to read recipe file: ${msg}`);
    }

    const result = validateRecipe(raw);

    if (result.valid) {
      this.log(`Recipe is valid: ${recipePath}`);
    } else {
      this.log(`Recipe has ${result.errors.length} error(s):`);
      for (const err of result.errors) {
        this.log(`  - ${err}`);
      }
    }

    return result;
  }
}
