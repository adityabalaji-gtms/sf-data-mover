import { readFileSync, mkdirSync } from 'node:fs';
import { resolve } from 'node:path';
import { SfCommand, Flags } from '@salesforce/sf-plugins-core';
import { Org } from '@salesforce/core';
import { DiffEngine } from '../../lib/compare/diff-engine.js';
import { DiffReporter } from '../../lib/compare/diff-reporter.js';
import { DeltaExporter } from '../../lib/compare/delta-exporter.js';
import { SchemaDescriber } from '../../lib/schema/describer.js';
import { GraphBuilder } from '../../lib/schema/graph.js';
import { Recipe, DiffResult, ObjectDescribe } from '../../lib/types.js';
import { validateRecipe } from '../../lib/recipe/schema.js';
import { RuleFilter } from '../../lib/rules/rule-filter.js';

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
    'select-object': Flags.string({
      summary: 'Root SObject to select specific rules from (e.g., sbaa__ApprovalRule__c).',
    }),
    select: Flags.string({
      summary: 'Comma-separated rule identifiers (Names or IDs) to compare.',
    }),
    'match-by': Flags.string({
      summary: 'How to match selected rules: "name" or "id".',
      default: 'name',
      options: ['name', 'id'],
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

    const selectObject = flags['select-object'];
    const selectValues = flags.select;
    const matchBy = (flags['match-by'] ?? 'name') as 'name' | 'id';

    if (selectObject && !selectValues) {
      this.error('--select is required when --select-object is specified');
    }
    if (selectValues && !selectObject) {
      this.error('--select-object is required when --select is specified');
    }

    const sourceOrg: Org = flags['source-org'];
    const targetOrg: Org = flags['target-org'];
    const sourceConn = sourceOrg.getConnection();
    const targetConn = targetOrg.getConnection();

    // If selective compare requested, build filtered recipe via RuleFilter
    if (selectObject && selectValues) {
      const identifiers = selectValues.split(',').map((s) => s.trim()).filter(Boolean);

      this.spinner.start('Building dependency graph for rule selection');
      const selDescriber = new SchemaDescriber(sourceConn);
      const selDescribes = new Map<string, ObjectDescribe>();
      const selCounts = new Map<string, number>();
      for (const obj of recipe.objects) {
        try {
          const desc = await selDescriber.describe(obj.sobject);
          const count = await selDescriber.countRecords(obj.sobject, obj.filter);
          selDescribes.set(obj.sobject, desc);
          selCounts.set(obj.sobject, count);
        } catch {
          // skip objects that can't be described
        }
      }
      const selGraph = new GraphBuilder().build(selDescribes, selCounts);
      this.spinner.stop('done');

      this.spinner.start(`Filtering recipe to selected ${selectObject} rules`);
      const ruleFilter = new RuleFilter(sourceConn, recipe, selGraph);
      recipe = await ruleFilter.filter(selectObject, identifiers, matchBy, (msg) => {
        this.spinner.status = msg;
      }, { forCompare: true });
      this.spinner.stop(`${recipe.objects.length} objects in filtered recipe`);
      this.log(`Selected rules: ${identifiers.join(', ')} (matched by ${matchBy})`);
      this.log('');
    }

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
