import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { SfCommand, Flags } from '@salesforce/sf-plugins-core';
import { Org } from '@salesforce/core';
import { SchemaDescriber } from '../../lib/schema/describer.js';
import { GraphBuilder } from '../../lib/schema/graph.js';
import { TopologicalSorter } from '../../lib/schema/sorter.js';
import { SchemaAnalyzer } from '../../lib/schema/analyzer.js';
import { Recipe, ObjectDescribe } from '../../lib/types.js';
import { validateRecipe } from '../../lib/recipe/schema.js';

interface PlanOutput {
  tiers: { tier: number; objects: { sobject: string; recordCount: number; externalId: string | null }[] }[];
}

export default class Plan extends SfCommand<PlanOutput> {
  public static readonly summary = 'Dry-run: show what would be exported and in what order.';

  public static readonly examples = [
    'sf data-mover plan --recipe recipes/cpq-full.json --target-org UAT',
  ];

  public static readonly flags = {
    recipe: Flags.string({
      summary: 'Path to the recipe JSON file.',
      char: 'r',
      required: true,
    }),
    'target-org': Flags.requiredOrg({
      summary: 'Org to check record counts against.',
      char: 'o',
      required: true,
    }),
  };

  public async run(): Promise<PlanOutput> {
    const { flags } = await this.parse(Plan);
    const recipePath = resolve(flags.recipe);
    const recipe: Recipe = JSON.parse(readFileSync(recipePath, 'utf-8'));

    const validation = validateRecipe(recipe);
    if (!validation.valid) {
      this.error(`Invalid recipe: ${validation.errors.join('; ')}`);
    }

    const org: Org = flags['target-org'];
    const conn = org.getConnection();
    const describer = new SchemaDescriber(conn);

    // Describe + count
    this.spinner.start('Fetching schemas and record counts');
    const describes = new Map<string, ObjectDescribe>();
    const recordCounts = new Map<string, number>();

    for (const obj of recipe.objects) {
      try {
        const desc = await describer.describe(obj.sobject);
        const count = await describer.countRecords(obj.sobject, obj.filter);
        describes.set(obj.sobject, desc);
        recordCounts.set(obj.sobject, count);
      } catch {
        this.warn(`Could not describe ${obj.sobject} — skipping`);
      }
    }
    this.spinner.stop();

    // Build graph limited to recipe objects
    const graph = new GraphBuilder().build(describes, recordCounts);
    const tiers = new TopologicalSorter().sort(graph);
    const analyzer = new SchemaAnalyzer();

    const output: PlanOutput = { tiers: [] };
    let order = 1;

    this.log('');
    this.log(`Recipe: ${recipe.name}`);
    this.log(`Objects: ${recipe.objects.length}`);
    this.log('');

    for (let t = 0; t < tiers.length; t++) {
      const tierObjects: PlanOutput['tiers'][number]['objects'] = [];
      this.log(`── Tier ${t} ${t === 0 ? '(load first)' : ''} ──`);

      for (const sobject of tiers[t]) {
        const recipeObj = recipe.objects.find((o) => o.sobject === sobject);
        const node = graph.nodes.get(sobject);
        if (!recipeObj || !node) continue;

        const extId = recipeObj.externalIdField ?? analyzer.pickExternalId(node) ?? '(composite key)';
        const count = recordCounts.get(sobject) ?? 0;

        tierObjects.push({ sobject, recordCount: count, externalId: extId });
        this.log(`  ${String(order++).padStart(3)}. ${sobject.padEnd(45)} ${String(count).padStart(8)} records   ext-id: ${extId}`);
      }

      output.tiers.push({ tier: t, objects: tierObjects });
      this.log('');
    }

    const total = [...recordCounts.values()].reduce((a, b) => a + b, 0);
    this.log(`Total records: ${total}`);

    return output;
  }
}
