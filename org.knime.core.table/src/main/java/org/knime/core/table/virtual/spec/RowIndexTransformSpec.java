package org.knime.core.table.virtual.spec;

import org.knime.core.table.virtual.spec.TableTransformSpec;

/**
 * A {@link TableTransformSpec} that appends a LONG columns containing the row
 * index. Row index starts at 0 and (conceptually) is incremented for every row
 * that is passed through this point in the virtual table construct.
 * <p>
 * In the actual execution, rows do not have to pass through in order. For
 * example, a {@code RandomAccessCursor} that is directly moved to row 10 will
 * see row index 10 (without having seen indices 0 through 9). Cursors that only
 * cover a row-range selection will only see the corresponding indices, and so
 * on.
 */
public class RowIndexTransformSpec implements TableTransformSpec {

    private final long offset;

    /**
     * Create a {@code RowIndexTransformSpec}.
     *
     * @param offset the offset that is added to row index
     */
    public RowIndexTransformSpec(final long offset)
    {
        this.offset = offset;
    }

    /**
     * Create a {@code RowIndexTransformSpec} (without an offset).
     */
    public RowIndexTransformSpec()
    {
        this(0);
    }

    /**
     * Return the offset that is added to row index.
     * <p>
     * This is used to maintain correct row indices when re-arranging slice
     * operations before the RowIndexTransform.
     */
    public long getOffset() {
        return offset;
    }

    @Override
    public String toString() {
        return "RowIndex" + (offset != 0 ? " offset=" + offset : "");
    }
}
