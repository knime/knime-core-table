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

import static org.knime.core.table.virtual.graph.rag.RagEdgeType.SPEC;
import static org.knime.core.table.virtual.graph.rag.RagNodeType.MISSING;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.knime.core.table.schema.DataSpecs;
import org.knime.core.table.virtual.TableTransform;

/**
 * The "RowAccessible Graph" (or "ReadAccess Graph", or something like that... ;-)
 * represents a {@code VirtualTable} structure. Nodes of the graph represent {@code
 * TableTransform}s, and edges represent various {@link RagEdgeType dependencies}
 * between nodes.
 */
public class RagGraph {

    private final TypedObjects<RagNodeType, RagNode> nodes = new TypedObjects<>(RagNodeType.class);

    private final TypedObjects<RagEdgeType, RagEdge> edges = new TypedObjects<>(RagEdgeType.class);

    private final MissingValueColumns missingValueColumns = new MissingValueColumns();

    private final RagNode missingValuesSource;

    // the Consumer node representing the final virtual table
    private RagNode root;

    public RagGraph()  {
        missingValuesSource = addNode(new TableTransform(//
                Collections.emptyList(),//
                new MissingValuesSourceTransformSpec(missingValueColumns.unmodifiable)));
    }

    public RagNode addNode(final TableTransform transform) {
        final RagNode node = new RagNode(transform);
        nodes.add(node);
        return node;
    }

    public RagEdge addEdge(final RagNode source, final RagNode target, final RagEdgeType type) {
        final RagEdge edge = new RagEdge(source, target, type);
        source.outgoing.add(edge);
        target.incoming.add(edge);
        edges.add(edge);
        return edge;
    }

    /**
     * Returns the directed edge (of the given {@code edgeType}) from {@code source}
     * node to {@code target} node. Creates the edge if it doesn't exist.
     *
     * @param source the source of the directed edge.
     * @param target the target of the directed edge.
     * @return the directed edge from {@code source} to {@code target}.
     */
    public RagEdge getOrAddEdge(final RagNode source, final RagNode target, final RagEdgeType type) {
        final RagEdge existing = getEdge(source, target, type);
        if ( existing != null )
            return existing;
        return addEdge(source, target, type);
    }

    /**
     * Remove {@code edge} and replace it with a new edge from {@code source} to
     * {@code edge}'s target. The new edge will be of the same {@code EdgeType}.
     * It will be appended to the {@code source}'s outgoing edge list, and the
     * target's incoming edge list.
     *
     * @param edge the edge to replace
     * @param source the source for the new edge (target remains the same)
     * @return the directed edge from {@code source} to {@code edge}'s target.
     */
    public RagEdge replaceEdgeSource(final RagEdge edge, final RagNode source) {
        final RagNode target = edge.getTarget();
        remove(edge);
        return getOrAddEdge(source, target, edge.type());
    }

    /**
     * Remove {@code edge} and replace it with a new edge from {@code edge}'s
     * source to {@code target}. The new edge will be of the same {@code
     * EdgeType}. It will be appended to the source's outgoing edge list, and
     * the {@code target}'s incoming edge list.
     *
     * @param edge the edge to replace
     * @param target the target for the new edge (source remains the same)
     * @return the directed edge from {@code edge}'s source to {@code target}.
     */
    public RagEdge replaceEdgeTarget(final RagEdge edge, final RagNode target) {
        final RagNode source = edge.getSource();
        remove(edge);
        return getOrAddEdge(source, target, edge.type());
    }

    /**
     * Returns the directed edge (of the given {@code edgeType}) from {@code source}
     * node to {@code target} node if it exists, or {@code null} otherwise.
     *
     * @param source the source of the directed edge.
     * @param target the target of the directed edge.
     * @return the directed edge from {@code source} to {@code target} if it exists, or
     * {@code null} otherwise.
     */
    public RagEdge getEdge(final RagNode source, final RagNode target, final RagEdgeType edgeType) {
        for (final RagEdge edge : source.outgoingEdges(edgeType)) {
            if (edge.getTarget().equals(target)) {
                return edge;
            }
        }
        return null;
    }

    public void remove(final RagEdge edge) {
        edge.getSource().outgoing.remove(edge);
        edge.getTarget().incoming.remove(edge);
        edges.remove(edge);
    }

    public void remove(final RagNode node) {
        for (final RagEdge edge : node.incoming.unmodifiable()) {
            edge.getSource().outgoing.remove(edge);
            edges.remove(edge);
        }
        for (final RagEdge edge : node.outgoing.unmodifiable()) {
            edge.getTarget().incoming.remove(edge);
            edges.remove(edge);
        }
        nodes.remove(node);
    }

    public Collection<RagNode> nodes() {
        return nodes.unmodifiable();
    }

    public Collection<RagNode> nodes(RagNodeType type) {
        return nodes.unmodifiable(type);
    }

    public Collection<RagEdge> edges() {
        return edges.unmodifiable();
    }

    public RagNode getRoot() {
        return root;
    }

    public void setRoot(final RagNode root) {
        this.root = root;
    }

    public RagNode getMissingValuesSource() {
        return missingValuesSource;
    }

    /**
     * Trim unnecessary nodes and edges.
     * <p>
     * Removes {@code SPEC} edges, and then removes orphaned nodes.
     * Obviously this should only be called after {@code SPEC} edges are no longer needed.
     */
    void trim() {
        final List<RagEdge> edgesToRemove = new ArrayList<>(edges.unmodifiable(SPEC));
        edgesToRemove.forEach(this::remove);

        final List<RagNode> nodesToRemove = new ArrayList<>();
        for (final RagNode node : nodes()) {
            if (node.type() != MISSING //
                    && node.incoming.unmodifiable().isEmpty() //
                    && node.outgoing.unmodifiable().isEmpty()) {
                nodesToRemove.add(node);
            }
        }
        nodesToRemove.forEach(this::remove);
    }

    /**
     * Perform a transitive reduction wrt the given {@code edgeType}.
     * <p>
     * That is, remove edges (of {@code edgeType}) that are transitively implied
     * by other edges (of {@code edgeType}).
     *
     * @param edgeType
     */
    void transitiveReduction(final RagEdgeType edgeType) {
        final List<RagNode> vertices = new ArrayList<>(nodes());
        ArrayList<RagEdge> oldEdges = new ArrayList<>(edges.unmodifiable(edgeType));
        final int n = vertices.size();
        final boolean[] adjacency = RagGraphUtils.transitiveReduction(RagGraphUtils.adjacency(vertices, edgeType));
        if (adjacency.length != n * n)
            throw new IllegalArgumentException("adjacency size doesn't match");
        for (int i = 0; i < n; ++i) {
            for (int j = 0; j < n; ++j) {
                if (adjacency[i * n + j])
                    oldEdges.remove( getOrAddEdge(vertices.get(i), vertices.get(j), edgeType) );
            }
        }
        oldEdges.forEach(this::remove);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName());
        sb.append("{\n");
        sb.append("  nodes = {\n");
        for (final RagNode node : nodes()) {
            sb.append("    " + node + ",\n");
        }
        sb.append("  },\n");
        sb.append("  edges = {\n");
        for (final RagEdge edge : edges()) {
            sb.append("    " + edge + ",\n");
        }
        sb.append("  }\n");
        sb.append("}");
        return sb.toString();
    }

    /**
     * Copy this RagGraph.
     * <p>
     * <em>This method is intended to make a copy of the SPEC graph (before
     * access tracing and optimization). It will currently not work to make a
     * full copy of a RagGraph after access tracing and optimization, or in
     * intermediate stages.</em>
     * <p>
     * All {@code RagNode}s are copied with their respective {@code TableTransformSpec}.
     * All {@code RagEdge}s are copied.
     * {@code AccessIds} are NOT copied.
     *
     * @return a copy of this graph
     */
    public RagGraph copy() {
        final RagGraph copy = new RagGraph();
        final Map<RagNode, RagNode> nodeMap = new HashMap<>();
        nodes().forEach(node -> {
            final RagNode nodeCopy;
            if (node == missingValuesSource) {
                nodeCopy = copy.missingValuesSource;
            } else {
                nodeCopy = new RagNode(node.getTransformSpec(), node.getInputssArray().length);
                copy.nodes.add(nodeCopy);
            }
            nodeCopy.setNumColumns(node.numColumns());
            nodeMap.put(node, nodeCopy);
        });
        final Map<RagEdge, RagEdge> edgeMap = new HashMap<>();
        edges().forEach(edge -> {
            final var sourceCopy = nodeMap.get(edge.getSource());
            final var targetCopy = nodeMap.get(edge.getTarget());
            final var edgeCopy = new RagEdge(sourceCopy, targetCopy, edge.type());
            edgeMap.put(edge, edgeCopy);
            copy.edges.add(edgeCopy);
        });
        nodes().forEach(node -> {
            final var nodeCopy = nodeMap.get(node);
            node.incoming.unmodifiable().forEach(edge -> nodeCopy.incoming.add(edgeMap.get(edge)));
            node.outgoing.unmodifiable().forEach(edge -> nodeCopy.outgoing.add(edgeMap.get(edge)));
        });
        missingValueColumns.unmodifiable.forEach(copy.missingValueColumns::add);
        copy.root = nodeMap.get(root);
        return copy;
    }

    /**
     * Create an {@code AccessId} for a missing-value column with the given {@code spec}.
     * The producer of the {@code AccessId} is {@code missingValuesSource}, and
     * validity is tied to the given {@link AccessValidity}.
     */
    AccessId getMissingValuesAccessId(final DataSpecs.DataSpecWithTraits spec, final AccessValidity validity) {
        final int columnIndex = missingValueColumns.add(spec);
        final AccessId accessId = missingValuesSource.getOrCreateOutput(columnIndex);
        accessId.setValidity(validity);
        return accessId;
    }

    /**
     * For each successor (of the specified {@code edgeType}) of {@code
     * oldNode}: Replace the edge linking {@code oldNode} to successor with an
     * edge linking {@code newNode} to successor.
     */
    public void relinkSuccessorsToNewSource(final RagNode oldNode, final RagNode newNode, final RagEdgeType... edgeTypes)
    {
        for (final var edgeType : edgeTypes) {
            final List<RagEdge> edges = new ArrayList<>(oldNode.outgoingEdges(edgeType));
            edges.forEach(edge -> replaceEdgeSource(edge, newNode));
        }
    }

    /**
     * For each predecessor (of the specified {@code edgeType}) of {@code
     * oldNode}: Replace the edge linking predecessor to {@code oldNode} with an
     * edge linking predecessor to {@code newNode}.
     */
    public void relinkPredecessorsToNewTarget(final RagNode oldNode, final RagNode newNode, final RagEdgeType... edgeTypes)
    {
        for (final var edgeType : edgeTypes) {
            final List<RagEdge> edges = new ArrayList<>(oldNode.incomingEdges(edgeType));
            edges.forEach(edge -> replaceEdgeTarget(edge, newNode));
        }
    }
}
