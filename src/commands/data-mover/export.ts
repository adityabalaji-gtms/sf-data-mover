import { readFileSync, mkdirSync } from 'node:fs';
import { resolve } from 'node:path';
import { SfCommand, Flags } from '@salesforce/sf-plugins-core';
import { Org } from '@salesforce/core';
import { SchemaDescriber } from '../../lib/schema/describer.js';
import { GraphBuilder } from '../../lib/schema/graph.js';
import { TopologicalSorter } from '../../lib/schema/sorter.js';
import { QueryBuilder } from '../../lib/data/query-builder.js';
import { DataFetcher } from '../../lib/data/data-fetcher.js';
import { IdResolver } from '../../lib/data/id-resolver.js';
import { CsvWriter } from '../../lib/output/csv-writer.js';
import { ManifestWriter } from '../../lib/output/manifest-writer.js';
import { Recipe, ObjectDescribe, ExportManifest } from '../../lib/types.js';
import { validateRecipe } from '../../lib/recipe/schema.js';
import { RuleFilter } from '../../lib/rules/rule-filter.js';

export default class Export extends SfCommand<ExportManifest> {
  public static readonly summary = 'Export data from an org as Inspector-ready CSVs with external ID references.';

  public static readonly description =
    'Queries data per recipe, resolves Salesforce IDs to external key references, ' +
    'and outputs CSVs in the correct dependency load order with Inspector-compatible relationship headers.';

  public static readonly examples = [
    'sf data-mover export --target-org UAT --recipe recipes/cpq-full.json --output-dir ./exports/uat/',
  ];

  public static readonly flags = {
    'target-org': Flags.requiredOrg({
      summary: 'Source org to export data from.',
      char: 'o',
      required: true,
    }),
    recipe: Flags.string({
      summary: 'Path to the recipe JSON file.',
      char: 'r',
      required: true,
    }),
    'output-dir': Flags.string({
      summary: 'Directory to write CSV files to.',
      char: 'd',
      required: true,
    }),
    'select-object': Flags.string({
      summary: 'Root SObject to select specific rules from (e.g., SBQQ__PriceRule__c).',
    }),
    select: Flags.string({
      summary: 'Comma-separated rule identifiers (Names or IDs) to export.',
    }),
    'match-by': Flags.string({
      summary: 'How to match selected rules: "name" or "id".',
      default: 'name',
      options: ['name', 'id'],
    }),
  };

  public async run(): Promise<ExportManifest> {
    const { flags } = await this.parse(Export);
    const recipePath = resolve(flags.recipe);
    let recipe: Recipe = JSON.parse(readFileSync(recipePath, 'utf-8'));

    const validation = validateRecipe(recipe);
    if (!validation.valid) {
      this.error(`Invalid recipe: ${validation.errors.join('; ')}`);
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

    const org: Org = flags['target-org'];
    const conn = org.getConnection();
    const alias = org.getUsername() ?? 'unknown';
    const outputDir = resolve(flags['output-dir']);
    mkdirSync(outputDir, { recursive: true });

    // If selective export requested, build filtered recipe via RuleFilter
    if (selectObject && selectValues) {
      const identifiers = selectValues.split(',').map((s) => s.trim()).filter(Boolean);

      this.spinner.start('Building dependency graph for rule selection');
      const selDescriber = new SchemaDescriber(conn);
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
      const ruleFilter = new RuleFilter(conn, recipe, selGraph);
      recipe = await ruleFilter.filter(selectObject, identifiers, matchBy, (msg) => {
        this.spinner.status = msg;
      });
      this.spinner.stop(`${recipe.objects.length} objects in filtered recipe`);
      this.log(`Selected rules: ${identifiers.join(', ')} (matched by ${matchBy})`);
      this.log('');
    }

    const describer = new SchemaDescriber(conn);
    const queryBuilder = new QueryBuilder();
    const fetcher = new DataFetcher(conn);
    const csvWriter = new CsvWriter();
    const manifestWriter = new ManifestWriter();

    // 1. Describe all objects in the recipe
    this.spinner.start('Describing schemas');
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
    this.spinner.stop(`${describes.size} objects`);

    // 2. Build graph + tiers (restricted to recipe objects)
    const graph = new GraphBuilder().build(describes, recordCounts);
    const tiers = new TopologicalSorter().sort(graph);

    // 3. Build ID resolution maps
    this.spinner.start('Building ID resolution maps');
    const idResolver = new IdResolver(conn, graph, recipe);
    await idResolver.buildIdMaps((sobject, count) => {
      this.spinner.status = `${sobject} (${count} keys)`;
    });
    this.spinner.stop('done');

    // 4. Export each object, tier by tier
    let fileOrder = 1;

    for (let t = 0; t < tiers.length; t++) {
      const tierDir = `tier-${t}`;
      this.log(`\n── Tier ${t} ──`);

      for (const sobject of tiers[t]) {
        const recipeObj = recipe.objects.find((o) => o.sobject === sobject);
        const describe = describes.get(sobject);
        if (!recipeObj || !describe) continue;

        if (recipeObj.resolveOnly) {
          this.log(`  ${sobject}: resolve-only (ID map built, no data export)`);
          continue;
        }

        // Query data
        const soql = queryBuilder.buildQuery(recipeObj, describe, recipe.settings);
        this.spinner.start(`Querying ${sobject}`);
        const records = await fetcher.fetchAll(soql);
        this.spinner.stop(`${records.length} records`);

        if (records.length === 0) {
          this.log(`  ${sobject}: 0 records — skipped`);
          continue;
        }

        // Resolve IDs
        const { headers, rows, selfRefField } = idResolver.resolveRecords(sobject, records, describe);

        // Write main CSV
        const filename = `${String(fileOrder).padStart(2, '0')}-${sobject}.csv`;
        csvWriter.write(outputDir, tierDir, filename, headers, rows);
        this.log(`  ${filename} (${rows.length} records)`);

        manifestWriter.addFile(t, {
          order: fileOrder,
          filename: `${tierDir}/${filename}`,
          sobject,
          externalIdField: recipeObj.externalIdField,
          recordCount: rows.length,
          operation: recipeObj.externalIdField ? 'upsert' : 'insert',
          notes: recipeObj.preImport ? `deactivate ${recipeObj.preImport.deactivate} before import` : undefined,
        });
        fileOrder++;

        // Self-reference pass 2
        if (selfRefField) {
          const pass2 = idResolver.buildSelfRefPass2(sobject, records, describe);
          if (pass2 && pass2.rows.length > 0) {
            const pass2Filename = `${String(fileOrder).padStart(2, '0')}-${sobject}_self-ref-pass2.csv`;
            csvWriter.write(outputDir, tierDir, pass2Filename, pass2.headers, pass2.rows);
            this.log(`  ${pass2Filename} (${pass2.rows.length} self-ref records)`);

            manifestWriter.addFile(t, {
              order: fileOrder,
              filename: `${tierDir}/${pass2Filename}`,
              sobject,
              externalIdField: recipeObj.externalIdField,
              recordCount: pass2.rows.length,
              operation: 'upsert',
              isSelfRefPass2: true,
              notes: 'Load AFTER the main file for this object',
            });
            fileOrder++;
          }
        }
      }
    }

    // 5. Write manifest
    manifestWriter.write(outputDir, alias, recipe.name);
    this.log(`\nManifest written to ${outputDir}/_manifest.json`);
    this.log(`Total files: ${fileOrder - 1}`);

    return JSON.parse(readFileSync(resolve(outputDir, '_manifest.json'), 'utf-8'));
  }
}
