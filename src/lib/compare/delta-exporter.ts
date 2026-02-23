import { Connection } from '@salesforce/core';
import { mkdirSync } from 'node:fs';
import { DiffResult, Recipe, ObjectDescribe, DependencyGraph } from '../types.js';
import { DataFetcher } from '../data/data-fetcher.js';
import { QueryBuilder } from '../data/query-builder.js';
import { IdResolver } from '../data/id-resolver.js';
import { CsvWriter } from '../output/csv-writer.js';
import { ManifestWriter } from '../output/manifest-writer.js';
import { SchemaDescriber } from '../schema/describer.js';
import { GraphBuilder } from '../schema/graph.js';
import { TopologicalSorter } from '../schema/sorter.js';

type SalesforceRecord = Record<string, unknown> & { Id: string };

/**
 * Exports only new + modified records (the delta) as Inspector-ready CSVs.
 * Uses the same ID resolution pipeline as the full export command.
 */
export class DeltaExporter {
  constructor(
    private sourceConn: Connection,
    private recipe: Recipe,
    private diffResult: DiffResult,
  ) {}

  async exportDelta(
    outputDir: string,
    onProgress?: (sobject: string, count: number) => void,
  ): Promise<void> {
    mkdirSync(outputDir, { recursive: true });

    const describer = new SchemaDescriber(this.sourceConn);
    const queryBuilder = new QueryBuilder();
    const fetcher = new DataFetcher(this.sourceConn);
    const csvWriter = new CsvWriter();
    const manifestWriter = new ManifestWriter();

    // Build graph for tier ordering
    const describes = new Map<string, ObjectDescribe>();
    const recordCounts = new Map<string, number>();

    for (const obj of this.recipe.objects) {
      try {
        const desc = await describer.describe(obj.sobject);
        describes.set(obj.sobject, desc);
        recordCounts.set(obj.sobject, 0);
      } catch {
        // skip
      }
    }

    const graph = new GraphBuilder().build(describes, recordCounts);
    const tiers = new TopologicalSorter().sort(graph);

    // Build ID maps from source org
    const idResolver = new IdResolver(this.sourceConn, graph, this.recipe);
    await idResolver.buildIdMaps();

    let fileOrder = 1;

    for (let t = 0; t < tiers.length; t++) {
      const tierDir = `tier-${t}`;

      for (const sobject of tiers[t]) {
        const recipeObj = this.recipe.objects.find((o) => o.sobject === sobject);
        const describe = describes.get(sobject);
        const objDiff = this.diffResult.objects[sobject];
        if (!recipeObj || !describe || !objDiff) continue;
        if (!recipeObj.externalIdField) continue;

        // Collect external IDs that are new or modified
        const deltaExtIds = new Set<string>();
        for (const rec of objDiff.newRecords) deltaExtIds.add(rec.externalId);
        for (const rec of objDiff.modifiedRecords) deltaExtIds.add(rec.externalId);

        if (deltaExtIds.size === 0) continue;

        // Fetch only delta records from source
        const allSoql = queryBuilder.buildQuery(recipeObj, describe, this.recipe.settings);
        const allRecords = await fetcher.fetchAll(allSoql);

        const deltaRecords = allRecords.filter((rec) => {
          const extVal = rec[recipeObj.externalIdField!];
          return extVal != null && deltaExtIds.has(String(extVal));
        });

        if (deltaRecords.length === 0) continue;

        // Resolve IDs
        const { headers, rows } = idResolver.resolveRecords(sobject, deltaRecords, describe);

        const filename = `${String(fileOrder).padStart(2, '0')}-${sobject}_delta.csv`;
        csvWriter.write(outputDir, tierDir, filename, headers, rows);

        manifestWriter.addFile(t, {
          order: fileOrder,
          filename: `${tierDir}/${filename}`,
          sobject,
          externalIdField: recipeObj.externalIdField,
          recordCount: rows.length,
          operation: 'upsert',
          notes: `Delta: ${objDiff.counts.new} new, ${objDiff.counts.modified} modified`,
        });

        onProgress?.(sobject, rows.length);
        fileOrder++;
      }
    }

    manifestWriter.write(
      outputDir,
      this.sourceConn.getUsername() ?? 'source',
      this.recipe.name,
      'delta',
    );
  }
}
