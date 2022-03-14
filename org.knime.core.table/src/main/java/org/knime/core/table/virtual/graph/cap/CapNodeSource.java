package org.knime.core.table.virtual.graph.cap;

import java.util.Arrays;
import java.util.UUID;

import org.knime.core.table.row.Selection.RowRangeSelection;

/**
 * Represents a source table in the CAP.
 * <p>
 * A {@code CapNodeSource} knows the UUID of its source table, the indices of
 * {@code n} selected columns, which will be assigned output slot indices {@code
 * [0, ..., n-1]}, and the selected row range.
 */
public class CapNodeSource extends CapNode {

    private final UUID uuid;
    private final int[] cols;
    private final long fromRow;
    private final long toRow;

    /**
     * @param index index of this node in the CAP list.
     * @param uuid  the UUID of the source table
     * @param cols  the column indices of the selected source columns.
     * @param range the row range selected on the source table
     */
    public CapNodeSource(final int index, final UUID uuid, final int[] cols, final RowRangeSelection range) {
        super(index, CapNodeType.SOURCE);
        this.uuid = uuid;
        this.cols = cols;
        this.fromRow = range.fromIndex();
        this.toRow = range.toIndex();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SOURCE(");
        sb.append("uuid=").append(uuid);
        sb.append(", cols=").append(Arrays.toString(cols));
        sb.append(", fromRow=").append(fromRow);
        sb.append(", toRow=").append(toRow);
        sb.append(')');
        return sb.toString();
    }

    /**
     * @return the UUID of the source table represented by this {@code CapNodeSource}.
     */
    public UUID uuid() {
        return uuid;
    }

    /**
     * @return the column indices of the source columns that should be output by this {@code CapNodeSource}.
     * The index in the returned array is the slot index of the respective output column.
     */
    public int[] cols() {
        return cols;
    }

    /**
     * Get start of row range (inclusive) that should be output by this {@code CapNodeSource}.
     * <p>
     * If all source rows should be output, then {@code fromRow()<0}.
     * The specification of a row range does not imply that all (or any) rows in the range exist.
     *
     * @return start of row range (inclusive), or a negative value if the range contains all rows
     */
    public long fromRow() {
        return fromRow;
    }

    /**
     * Get the end of row range (exclusive) that should be output by this {@code CapNodeSource}.
     * <p>
     * If all source rows should be output, this is indicated by {@code fromRow()<0},
     * and {@code toRow()} should be ignored.
     * The specification of a row range does not imply that all (or any) rows in the range exist.
     *
     * @return end of row range (exclusive). (If {@code fromRow()<0}, this value should be ignored.)
     */
    public long toRow() {
        return toRow;
    }
}
