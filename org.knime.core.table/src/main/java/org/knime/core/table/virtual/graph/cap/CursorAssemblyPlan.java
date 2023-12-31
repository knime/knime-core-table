package org.knime.core.table.virtual.graph.cap;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.knime.core.table.schema.ColumnarSchema;

/**
 * Instructions for building a {@code Cursor<ReadAccessRow>}.
 * <p>
 * The instructions comprise a list of {@code CapNode}s. There are subclasses of
 * {@code CapNode} for each {@code CapNodeType} (source tables and various
 * operations). The idea is that {@code CapNode}s only comprise {@code int}s,
 * {@code String}s etc. that can be easily serialized.
 * <p>
 * {@code CapNode}s can have predecessor nodes, which are stored as indices in the
 * {@link #nodes() node list}. “Append” and “Concatenate” are the only operations
 * that require more than one predecessor. Predecessor indices are guaranteed to
 * only reference earlier entries in the node list. Thus, when constructing a
 * cursor we can just walk the list front to back.
 * <p>
 * Input {@code ReadAccess}es that a node requires are given as
 * <em>(producer_node_index, slot index)</em> pairs. Here, slots are the array of
 * accesses that a node produces. This can be different to the original
 * VirtualTable column indices, because it only contains columns that are actually
 * consumed somewhere. So, if we filter only column 3 of a source, then that would
 * be at slot 0 of the source node. Also <em>(producer_node_index, slot index)</em>
 * pairs are guaranteed to only reference earlier entries in the list.
 */
public class CursorAssemblyPlan {

    private final List<CapNode> nodes;

    private final boolean supportsLookahead;

    private final long numRows;

    private final Map<UUID, ColumnarSchema> schemas;

    /**
     * Create a {@code CursorAssemblyPlan} with the sequence of {@code nodes}.
     *
     * @param nodes sequence of nodes representing source tables and various operations, which can be processed
     *            front-to-back to instantiate a {@code Cursor<ReadAccessRow>}. Each node may only refer back to earlier
     *            nodes in the sequence.
     * @param supportsLookahead whether the constructed cursor should be a {@code LookaheadCursor}.
     * @param numRows number of rows. {@code numRows<0} if the number of rows is unknown.
     * @param schemas the {@code ColumnarSchema}s of source and sink {@code RowAccessible}s, for schema verification during execution.
     */
    public CursorAssemblyPlan(final List<CapNode> nodes, final boolean supportsLookahead, final long numRows, final Map<UUID, ColumnarSchema> schemas) {
        this.nodes = nodes;
        this.supportsLookahead = supportsLookahead;
        this.numRows = numRows;
        this.schemas = schemas;
    }

    /**
     * Get the sequence of {@code CapNode}s.
     *
     * @return the sequence of {@code CapNode}s
     */
    public List<CapNode> nodes() {
        return nodes;
    }

    /**
     * Returns {@code true} if the assembled Cursor should be a {@code LookaheadCursor}.
     *
     * @return {@code true} if the assembled Cursor should be a {@code LookaheadCursor}.
     */
    public boolean supportsLookahead() {
        return supportsLookahead;
    }

    /**
     * Get the number of rows.
     * {@code numRows()<0} if the number of rows is unknown.
     *
     * @return number of rows
     */
    public long numRows() {
        return numRows;
    }

    /**
     * Get the {@code ColumnarSchema}s of source and sink {@code
     * RowAccessible}s, for schema verification during execution.
     *
     * @return a map from table {@code UUID} to schema
     */
    public Map<UUID, ColumnarSchema> schemas() {
        return schemas;
    }
}
