package org.knime.core.table.virtual.graph.rag;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

class RagGraphUtils {

    // --------------------------------------------------------------------
    //   Transitive Closure and Transitive Reduction

    /**
     * Add edges of the given {@code edgeType} according to given {@code adjacency} matrix.
     */
    public static void addEdges(final RagGraph graph, final List<RagNode> vertices, final boolean[] adjacency, final RagEdgeType edgeType)
    {
        final int n = vertices.size();
        if (adjacency.length != n * n)
            throw new IllegalArgumentException("adjacency size doesn't match");
        for (int i = 0; i < n; ++i) {
            for (int j = 0; j < n; ++j) {
                if (adjacency[i * n + j])
                    graph.getOrAddEdge(vertices.get(i), vertices.get(j), edgeType);
            }
        }
    }

    /**
     * Computes the transitive closure of the given {@code adjacency} matrix.
     *
     * @param adjacency flattened adjacency matrix.
     * @return transitive closure (as flattened adjacency matrix), i.e., flattened reachability matrix for {@code adjacency}.
     */
    public static boolean[] transitiveClosure(final boolean[] adjacency) {
        final int n = (int)Math.sqrt(adjacency.length);
        if (adjacency.length != n * n)
            throw new IllegalArgumentException("expected square matrix");
        final boolean[] reachability = adjacency.clone();
        for (int k = 0; k < n; ++k) {
            for (int i = 0; i < n; ++i) {
                for (int j = 0; j < n; ++j) {
                    if (reachability[i * n + k] && reachability[k * n + j])
                        reachability[i * n + j] = true;
                }
            }
        }
        return reachability;
    }

    /**
     * Compute the adjacency matrix for the given list of vertices.
     * <p>
     * {@code adjacency(i,j) == true} if there is an edge of one of the specified
     * {@code edgeTypes} from vertices(i) to vertices(j)
     *
     * @return flattened adjacency matrix
     */
    public static boolean[] adjacency(final List<RagNode> vertices, final RagEdgeType... edgeTypes) {
        final int n = vertices.size();
        final boolean[] adjacency = new boolean[n * n];
        for (final RagEdgeType edgeType : edgeTypes) {
            for (int i = 0; i < n; ++i) {
                final RagNode source = vertices.get(i);
                for (int j = 0; j < n; ++j) {
                    final RagNode target = vertices.get(j);
                    for (final RagEdge edge : source.outgoingEdges(edgeType)) {
                        if (edge.getTarget().equals(target)) {
                            adjacency[i * n + j] = true;
                            break;
                        }
                    }
                }
            }
        }
        return adjacency;
    }

    /**
     * Computes the transitive reduction of the given {@code adjacency} matrix.
     *
     * @param adjacency flattened adjacency matrix.
     * @return transitive reduction (as flattened adjacency matrix).
     */
    public static boolean[] transitiveReduction(final boolean[] adjacency) {
        return transitiveReduction(adjacency, transitiveClosure(adjacency));
    }

    /**
     * Computes the transitive reduction of the given {@code adjacency} matrix.
     *
     * @param adjacency flattened adjacency matrix.
     * @param reachability flattened reachability matrix.
     * @return transitive reduction (as flattened adjacency matrix).
     */
    public static boolean[] transitiveReduction(final boolean[] adjacency, final boolean[] reachability) {
        final int n = (int)Math.sqrt(adjacency.length);
        if (adjacency.length != n * n)
            throw new IllegalArgumentException("expected square matrix");
        if (reachability.length != adjacency.length)
            throw new IllegalArgumentException();

        final boolean[] reduction = new boolean[n * n];
        for (int i = 0; i < n; ++i) {
            for (int j = 0; j < n; ++j) {
                if (adjacency[i * n + j]) {
                    boolean keep = true;
                    for (int k = 0; k < n; ++k) {
                        if (reachability[i * n + k] && reachability[k * n + j]) {
                            keep = false;
                            break;
                        }
                    }
                    if ( keep )
                        reduction[i * n + j] = true;
                }
            }
        }
        return reduction;
    }



    // --------------------------------------------------------------------
    //   Topological Sort

    private static final Function<List<RagNode>, RagNode> defaultPolicy = nodes -> nodes.get(nodes.size() - 1);

    public static List<RagNode> topologicalSort(
            final RagGraph graph,
            final RagEdgeType edgeType) {
        return topologicalSort(graph, edgeType, defaultPolicy);
    }

    public static List<RagNode> topologicalSort(
            final RagGraph graph,
            final RagEdgeType edgeType,
            final Function<List<RagNode>, RagNode> policy) {
        return topologicalSort(graph, edgeType, null, policy);
    }

    public static List<RagNode> topologicalSort(
            final RagGraph graph,
            final RagEdgeType edgeType,
            final List<RagNode> sinks) {
        return topologicalSort(graph, edgeType, sinks, defaultPolicy);
    }

    public static List<RagNode> topologicalSort(
            final RagGraph graph,
            final RagEdgeType edgeType,
            final List<RagNode> sinks,
            final Function<List<RagNode>, RagNode> policy) {

        final List<RagNode> readyNodes = new ArrayList<>();
        if (sinks != null) {
            // trace backwards from sinks to find sources from which to start
            graph.nodes().forEach(node -> node.setMark(-1));
            final List<RagNode> sinks_ = new ArrayList<>(sinks);
            while (!sinks_.isEmpty()) {
                final RagNode node = sinks_.remove(sinks_.size() - 1);
                if (node.getMark() < 0) {
                    final List<RagNode> predecessors = node.predecessors(edgeType);
                    final int mark = predecessors.size();
                    node.setMark(mark);
                    if (mark == 0)
                        readyNodes.add(node);
                    sinks_.addAll(predecessors);
                }
            }
        } else {
            // find sources (no incoming edges) from which to start
            for (final RagNode node : graph.nodes()) {
                final int mark = node.predecessors(edgeType).size();
                node.setMark(mark);
                if (mark == 0)
                    readyNodes.add(node);
            }
        }

        final List<RagNode> sorted = new ArrayList<>();
        while (!readyNodes.isEmpty()) {
            var node = policy.apply(readyNodes);
            sorted.add(node);
            readyNodes.remove(node);
            node.successors(edgeType).forEach(n -> {
                int mark = n.getMark();
                if (mark == 0) {
                    throw new IllegalStateException();
                } else if (mark > 0) {
                    n.setMark(--mark);
                    if (mark == 0) {
                        readyNodes.add(n);
                    }
                }
            });
        }

        return sorted;
    }
}
