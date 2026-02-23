import { SfCommand, Flags } from '@salesforce/sf-plugins-core';
import { Connection, Org } from '@salesforce/core';
import { SchemaDescriber } from '../../lib/schema/describer.js';
import { GraphBuilder } from '../../lib/schema/graph.js';
import { TopologicalSorter } from '../../lib/schema/sorter.js';
import { SchemaAnalyzer } from '../../lib/schema/analyzer.js';
import { DiscoveryResult, ObjectDescribe } from '../../lib/types.js';

export default class Discover extends SfCommand<DiscoveryResult> {
  public static readonly summary = 'Discover object schemas, relationships, and dependency tiers in an org.';

  public static readonly description =
    'Connects to an org, describes objects, identifies external IDs, builds a relationship graph, ' +
    'and outputs a topologically sorted dependency order. Useful for understanding what to migrate and in what order.';

  public static readonly examples = [
    'sf data-mover discover --target-org UAT',
    'sf data-mover discover --target-org UAT --filter "SBQQ__,sbaa__,Product2"',
    'sf data-mover discover --target-org UAT --min-records 1',
  ];

  public static readonly flags = {
    'target-org': Flags.requiredOrg({
      summary: 'Org to discover against.',
      char: 'o',
      required: true,
    }),
    filter: Flags.string({
      summary: 'Comma-separated object name prefixes to include (e.g. "SBQQ__,sbaa__,Product2").',
      char: 'f',
    }),
    'min-records': Flags.integer({
      summary: 'Skip objects with fewer records than this threshold.',
      default: 0,
    }),
    json: Flags.boolean({
      summary: 'Output as JSON.',
    }),
  };

  public async run(): Promise<DiscoveryResult> {
    const { flags } = await this.parse(Discover);
    const org: Org = flags['target-org'];
    const conn: Connection = org.getConnection();
    const alias = org.getUsername() ?? 'unknown';

    const filters = flags.filter?.split(',').map((s) => s.trim()) ?? undefined;
    const minRecords = flags['min-records'] ?? 0;

    const describer = new SchemaDescriber(conn);
    const graphBuilder = new GraphBuilder();
    const sorter = new TopologicalSorter();
    const analyzer = new SchemaAnalyzer();

    // 1. List objects
    this.spinner.start('Listing objects');
    const objectNames = await describer.listObjects(filters);
    this.spinner.stop(`found ${objectNames.length} objects`);

    // 2. Describe each object + count records
    this.spinner.start('Describing schemas & counting records');
    const describes = new Map<string, ObjectDescribe>();
    const recordCounts = new Map<string, number>();

    const CONCURRENCY = 5;
    for (let i = 0; i < objectNames.length; i += CONCURRENCY) {
      const batch = objectNames.slice(i, i + CONCURRENCY);
      const results = await Promise.all(
        batch.map(async (name) => {
          const desc = await describer.describe(name);
          const count = await describer.countRecords(name);
          return { name, desc, count };
        })
      );
      for (const { name, desc, count } of results) {
        if (count >= minRecords) {
          describes.set(name, desc);
          recordCounts.set(name, count);
        }
      }
    }
    this.spinner.stop(`described ${describes.size} objects`);

    // 3. Build graph
    this.spinner.start('Building dependency graph');
    const graph = graphBuilder.build(describes, recordCounts);
    this.spinner.stop(`${graph.edges.length} edges`);

    // 4. Sort
    this.spinner.start('Computing load order');
    const tiers = sorter.sort(graph);
    this.spinner.stop(`${tiers.length} tiers`);

    // 5. Gaps
    const gaps = analyzer.findExternalIdGaps(graph);

    const result: DiscoveryResult = {
      orgAlias: alias,
      timestamp: new Date().toISOString(),
      objects: [...graph.nodes.values()].sort((a, b) => a.tier - b.tier || a.sobject.localeCompare(b.sobject)),
      edges: graph.edges,
      tiers,
      gaps,
    };

    // Console output (unless --json)
    if (!flags.json) {
      this.printSummary(result);
    }

    return result;
  }

  private printSummary(result: DiscoveryResult): void {
    this.log('');
    this.log(`Org:        ${result.orgAlias}`);
    this.log(`Objects:    ${result.objects.length}`);
    this.log(`Edges:      ${result.edges.length}`);
    this.log(`Tiers:      ${result.tiers.length}`);
    this.log('');

    for (let t = 0; t < result.tiers.length; t++) {
      this.log(`── Tier ${t} (${t === 0 ? 'load first' : 'depends on tier ' + (t - 1)}) ──`);
      for (const sobject of result.tiers[t]) {
        const node = result.objects.find((o) => o.sobject === sobject)!;
        const extId = node.externalIdFields.length > 0
          ? node.externalIdFields.join(', ')
          : 'NONE';
        const selfRef = node.selfReferences.length > 0
          ? ` [self-ref: ${node.selfReferences.map((s) => s.field).join(', ')}]`
          : '';
        this.log(
          `  ${sobject.padEnd(45)} ${String(node.recordCount).padStart(8)} records   ext-id: ${extId}${selfRef}`
        );
      }
      this.log('');
    }

    if (result.gaps.length > 0) {
      this.log('⚠ External ID Gaps:');
      for (const gap of result.gaps) {
        this.log(`  ${gap.sobject}: ${gap.reason}`);
      }
      this.log('');
    }
  }
}
