import { DependencyGraph } from '../types.js';

/**
 * Topological sort using Kahn's algorithm.
 * Assigns tier numbers to each node and returns ordered tier arrays.
 * Tier 0 = no dependencies (load first), higher tiers depend on lower ones.
 */
export class TopologicalSorter {
  sort(graph: DependencyGraph): string[][] {
    const { nodes, edges } = graph;
    const inDegree = new Map<string, number>();
    const adjacency = new Map<string, Set<string>>();

    for (const name of nodes.keys()) {
      inDegree.set(name, 0);
      adjacency.set(name, new Set());
    }

    for (const edge of edges) {
      if (!nodes.has(edge.from) || !nodes.has(edge.to)) continue;
      adjacency.get(edge.to)!.add(edge.from);
      inDegree.set(edge.from, (inDegree.get(edge.from) ?? 0) + 1);
    }

    const tiers: string[][] = [];
    const remaining = new Set(nodes.keys());

    while (remaining.size > 0) {
      const tierNodes = [...remaining].filter((n) => (inDegree.get(n) ?? 0) === 0);

      if (tierNodes.length === 0) {
        // Cycle detected — put all remaining nodes in the next tier with a warning
        tiers.push([...remaining]);
        for (const n of remaining) {
          const node = nodes.get(n);
          if (node) node.tier = tiers.length - 1;
        }
        break;
      }

      const tierIndex = tiers.length;
      tiers.push(tierNodes.sort());

      for (const n of tierNodes) {
        remaining.delete(n);
        const node = nodes.get(n);
        if (node) node.tier = tierIndex;

        for (const dependent of adjacency.get(n) ?? []) {
          inDegree.set(dependent, (inDegree.get(dependent) ?? 0) - 1);
        }
      }
    }

    graph.tiers = tiers;
    return tiers;
  }
}
