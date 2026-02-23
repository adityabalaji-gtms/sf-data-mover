import { readFileSync, mkdirSync } from 'node:fs';
import { resolve } from 'node:path';
import { SfCommand, Flags } from '@salesforce/sf-plugins-core';
import { Org } from '@salesforce/core';
import { DiffEngine } from '../../lib/compare/diff-engine.js';
import { DiffReporter } from '../../lib/compare/diff-reporter.js';
import { DeltaExporter } from '../../lib/compare/delta-exporter.js';
import { Recipe, DiffResult } from '../../lib/types.js';
import { validateRecipe } from '../../lib/recipe/schema.js';

export default class Compare extends SfCommand<DiffResult> {
  public static readonly summary = 'Compare two orgs record-by-record and report differences.';

  public static readonly description =
    'Diffs source and target orgs by joining on external ID. ' +
    'Reports new, modified, deleted, and identical records per object. ' +
    'Optionally exports delta-only CSVs for incremental migration.';

  public static readonly examples = [
    'sf data-mover compare --source-org UAT --target-org ppdev --recipe recipes/cpq-full.json',
    'sf data-mover compare --source-org UAT --target-org ppdev --recipe recipes/cpq-full.json --object SBQQ__PriceRule__c',
    'sf data-mover compare --source-org UAT --target-org ppdev --recipe recipes/cpq-full.json --export-delta ./exports/delta/',
  ];

  public static readonly flags = {
    'source-org': Flags.requiredOrg({
      summary: 'Source org (the "truth" org).',
      required: true,
    }),
    'target-org': Flags.requiredOrg({
      summary: 'Target org to compare against.',
      char: 'o',
      required: true,
    }),
    recipe: Flags.string({
      summary: 'Path to the recipe JSON file.',
      char: 'r',
      required: true,
    }),
    object: Flags.string({
      summary: 'Compare only a single object (API name).',
    }),
    'export-delta': Flags.string({
      summary: 'Directory to export delta-only CSVs (new + modified records).',
    }),
  };

  public async run(): Promise<DiffResult> {
    const { flags } = await this.parse(Compare);
    const recipePath = resolve(flags.recipe);
    let recipe: Recipe = JSON.parse(readFileSync(recipePath, 'utf-8'));

    const validation = validateRecipe(recipe);
    if (!validation.valid) {
      this.error(`Invalid recipe: ${validation.errors.join('; ')}`);
    }

    // If --object is specified, filter the recipe to just that object
    if (flags.object) {
      const filtered = recipe.objects.filter((o) => o.sobject === flags.object);
      if (filtered.length === 0) {
        this.error(`Object ${flags.object} not found in recipe`);
      }
      recipe = { ...recipe, objects: filtered };
    }

    const sourceOrg: Org = flags['source-org'];
    const targetOrg: Org = flags['target-org'];
    const sourceConn = sourceOrg.getConnection();
    const targetConn = targetOrg.getConnection();

    this.log(`Source: ${sourceOrg.getUsername()}`);
    this.log(`Target: ${targetOrg.getUsername()}`);
    this.log(`Recipe: ${recipe.name} (${recipe.objects.length} objects)`);
    this.log('');

    // Run diff
    const engine = new DiffEngine(sourceConn, targetConn, recipe);
    const result = await engine.diffAll((sobject, status) => {
      this.log(`  ${sobject.padEnd(45)} ${status}`);
    });

    // Print summary table
    const reporter = new DiffReporter();
    this.log('');
    this.log(reporter.formatSummaryTable(result));
    this.log('');

    // Write report
    if (flags['export-delta']) {
      const deltaDir = resolve(flags['export-delta']);
      mkdirSync(deltaDir, { recursive: true });

      reporter.writeReport(deltaDir, result);
      this.log(`Diff report written to ${deltaDir}/_diff-report.json`);

      // Export delta CSVs
      this.log('');
      this.spinner.start('Exporting delta CSVs');
      const deltaExporter = new DeltaExporter(sourceConn, recipe, result);
      await deltaExporter.exportDelta(deltaDir, (sobject, count) => {
        this.spinner.status = `${sobject} (${count} records)`;
      });
      this.spinner.stop('done');
      this.log(`Delta CSVs written to ${deltaDir}/`);
    }

    return result;
  }
}
