/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ---------------------------------------------------------------------
 */
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
