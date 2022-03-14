package org.knime.core.table.virtual.graph.rag;

import java.util.Collection;

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

    // the Consumer node representing the final virtual table
    private RagNode root;

    private RagNode missingValuesSource;

    public RagGraph()  {
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

    public void setMissingValuesSource(final RagNode missingValuesSource) {
        this.missingValuesSource = missingValuesSource;
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
}
