import { writeFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { SfCommand, Flags } from '@salesforce/sf-plugins-core';
import { Org } from '@salesforce/core';
import { SchemaDescriber } from '../../../lib/schema/describer.js';
import { GraphBuilder } from '../../../lib/schema/graph.js';
import { TopologicalSorter } from '../../../lib/schema/sorter.js';
import { RecipeBuilder } from '../../../lib/recipe/builder.js';
import { PRESETS, listPresets } from '../../../lib/recipe/presets.js';
import { Recipe, ObjectDescribe } from '../../../lib/types.js';

export default class RecipeCreate extends SfCommand<Recipe> {
  public static readonly summary = 'Create a migration recipe from a preset or by selecting objects interactively.';

  public static readonly examples = [
    'sf data-mover recipe create --target-org UAT --preset cpq-full --output recipes/cpq-full.json',
    'sf data-mover recipe create --target-org UAT --objects "Product2,SBQQ__PriceRule__c" --output recipes/custom.json',
  ];

  public static readonly flags = {
    'target-org': Flags.requiredOrg({
      summary: 'Org to introspect for schema information.',
      char: 'o',
      required: true,
    }),
    preset: Flags.string({
      summary: 'Start from a pre-built recipe preset (cpq-full, cpq-rules, cpq-products, cpq-templates, approvals).',
      char: 'p',
      options: Object.keys(PRESETS),
    }),
    objects: Flags.string({
      summary: 'Comma-separated list of object API names to include.',
    }),
    output: Flags.string({
      summary: 'Output file path for the recipe JSON.',
      char: 'O',
      required: true,
    }),
    name: Flags.string({
      summary: 'Recipe name.',
      default: 'Custom Migration',
    }),
    'list-presets': Flags.boolean({
      summary: 'List available presets and exit.',
    }),
  };

  public async run(): Promise<Recipe> {
    const { flags } = await this.parse(RecipeCreate);

    if (flags['list-presets']) {
      this.log('\nAvailable presets:');
      for (const p of listPresets()) {
        this.log(`  ${p.key.padEnd(16)} ${p.name.padEnd(30)} ${p.objectCount} objects`);
        this.log(`  ${''.padEnd(16)} ${p.description}`);
        this.log('');
      }
      return {} as Recipe;
    }

    let recipe: Recipe;

    if (flags.preset) {
      recipe = structuredClone(PRESETS[flags.preset]);
      this.log(`Using preset: ${recipe.name} (${recipe.objects.length} objects)`);
    } else if (flags.objects) {
      const org: Org = flags['target-org'];
      const conn = org.getConnection();
      const objectNames = flags.objects.split(',').map((s) => s.trim());

      this.spinner.start('Discovering schemas');
      const describer = new SchemaDescriber(conn);
      const describes = new Map<string, ObjectDescribe>();
      const recordCounts = new Map<string, number>();

      for (const name of objectNames) {
        const desc = await describer.describe(name);
        const count = await describer.countRecords(name);
        describes.set(name, desc);
        recordCounts.set(name, count);
      }

      const graph = new GraphBuilder().build(describes, recordCounts);
      new TopologicalSorter().sort(graph);
      this.spinner.stop();

      const builder = new RecipeBuilder();
      recipe = builder.buildFromSelection(graph, objectNames, flags.name!, '');
    } else {
      this.error('Provide --preset or --objects. Use --list-presets to see available presets.');
    }

    const outPath = resolve(flags.output);
    writeFileSync(outPath, JSON.stringify(recipe, null, 2) + '\n', 'utf-8');
    this.log(`Recipe written to ${outPath}`);

    return recipe;
  }
}
