import { DependencyGraph, ExternalIdGap, GraphNode } from '../types.js';

/**
 * Post-processes a dependency graph to identify gaps and produce actionable insights.
 */
export class SchemaAnalyzer {
  /**
   * Find objects that have inbound references but no external ID field —
   * these cannot be targets of relationship-based upserts.
   */
  findExternalIdGaps(graph: DependencyGraph): ExternalIdGap[] {
    const gaps: ExternalIdGap[] = [];
    const referencedObjects = new Set(graph.edges.map((e) => e.to));

    for (const sobject of referencedObjects) {
      const node = graph.nodes.get(sobject);
      if (!node) continue;
      if (node.externalIdFields.length === 0) {
        gaps.push({
          sobject,
          reason: `Referenced by ${this.inboundCount(graph, sobject)} object(s) but has no external ID field`,
        });
      }
    }

    for (const [, node] of graph.nodes) {
      if (node.selfReferences.length > 0 && node.externalIdFields.length === 0) {
        if (!gaps.find((g) => g.sobject === node.sobject)) {
          gaps.push({
            sobject: node.sobject,
            reason: 'Has self-referencing field(s) but no external ID field',
          });
        }
      }
    }

    return gaps.sort((a, b) => a.sobject.localeCompare(b.sobject));
  }

  /**
   * Pick the best external ID field for a given object.
   * Prefers CPQ_External_ID__c, then ATGExternalID__c, then any other.
   */
  pickExternalId(node: GraphNode): string | null {
    const fields = node.externalIdFields;
    if (fields.length === 0) return null;

    const preferred = ['CPQ_External_ID__c', 'ATGExternalID__c', 'External_id__c'];
    for (const pref of preferred) {
      if (fields.includes(pref)) return pref;
    }

    const custom = fields.filter((f) => f.endsWith('__c'));
    if (custom.length > 0) return custom[0];

    return fields[0];
  }

  private inboundCount(graph: DependencyGraph, sobject: string): number {
    return graph.edges.filter((e) => e.to === sobject).length;
  }
}
