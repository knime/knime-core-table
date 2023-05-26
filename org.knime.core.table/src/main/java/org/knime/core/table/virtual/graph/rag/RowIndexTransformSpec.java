package org.knime.core.table.virtual.graph.rag;

import org.knime.core.table.virtual.spec.TableTransformSpec;

/**
 * Artificial {@link TableTransformSpec} that is inserted into the RagGraph at
 * points where row indices should be tracked. It is created when the RagGraph
 * is build.
 */
public class RowIndexTransformSpec implements TableTransformSpec {

    private long offset;

    /**
     * Return the offset that is added to row index.
     * <p>
     * This is used to maintain correct row indices when re-arranging slice
     * operations before the RowIndexTransform.
     */
    public long getOffset() {
        return offset;
    }

    /**
     * Set the offset that is added to row index.
     * <p>
     * This is used to maintain correct row indices when re-arranging slice
     * operations before the RowIndexTransform.
     */
    public void setOffset(long offset) {
        this.offset = offset;
    }

    @Override
    public String toString() {
        return "RowIndex" + (offset != 0 ? " offset=" + offset : "");
    }
}
