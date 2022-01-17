package org.knime.core.table.virtual.graph.cap;

/**
 * A node in the Cursor Assembly Plan (CAP).
 * <p>
 * Each {@code CapNode} has an index, which is just the index of the node in the CAP list.
 */
public abstract class CapNode {

    private final int index;
    private final CapNodeType type;

    public CapNode(final int index, final CapNodeType type) {
        this.index = index;
        this.type = type;
    }

    public int index() {
        return index;
    }

    public CapNodeType type() {
        return type;
    }
}
