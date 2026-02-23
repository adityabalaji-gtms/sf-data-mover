import {
  ObjectDescribe,
  GraphNode,
  GraphEdge,
  ReferenceField,
  SelfReference,
  DependencyGraph,
} from '../types.js';

const SYSTEM_REFERENCE_TARGETS = new Set(['User', 'Group', 'Organization', 'RecordType', 'Profile']);
const SYSTEM_REFERENCE_FIELDS = new Set(['OwnerId', 'CreatedById', 'LastModifiedById', 'RecordTypeId', 'SetupOwnerId']);

/**
 * Builds a directed dependency graph from a set of described objects.
 * Nodes are objects; edges point from child → parent (the load direction is reversed).
 */
export class GraphBuilder {
  /**
   * @param objectScope - the set of object API names we care about
   */
  build(
    describes: Map<string, ObjectDescribe>,
    recordCounts: Map<string, number>,
  ): DependencyGraph {
    const objectScope = new Set(describes.keys());
    const nodes = new Map<string, GraphNode>();
    const edges: GraphEdge[] = [];

    for (const [name, desc] of describes) {
      const externalIdFields = desc.fields
        .filter((f) => f.externalId)
        .map((f) => f.name);

      const referenceFields: ReferenceField[] = [];
      const selfReferences: SelfReference[] = [];

      for (const field of desc.fields) {
        if (field.type !== 'reference' || !field.referenceTo.length) continue;
        if (SYSTEM_REFERENCE_FIELDS.has(field.name)) continue;

        const target = field.referenceTo[0];
        if (SYSTEM_REFERENCE_TARGETS.has(target)) continue;

        if (target === name) {
          selfReferences.push({
            field: field.name,
            relationshipName: field.relationshipName,
          });
          continue;
        }

        if (!objectScope.has(target)) continue;

        const targetDesc = describes.get(target);
        const extIdOnTarget = targetDesc?.fields.find((f) => f.externalId)?.name ?? null;

        referenceFields.push({
          field: field.name,
          relationshipName: field.relationshipName,
          referenceTo: target,
          externalIdOnTarget: extIdOnTarget,
        });

        edges.push({
          from: name,
          to: target,
          field: field.name,
          relationshipName: field.relationshipName,
        });
      }

      nodes.set(name, {
        sobject: name,
        label: desc.label,
        tier: -1,
        externalIdFields,
        selfReferences,
        recordCount: recordCounts.get(name) ?? 0,
        referenceFields,
      });
    }

    return { nodes, edges, tiers: [] };
  }
}
