package org.knime.core.table.virtual.graph.cap;

import java.util.Arrays;
import java.util.UUID;

/**
 * Represents a source table in the CAP.
 * <p>
 * A {@code CapNodeSource} knows the UUID of its source table, and the indices of
 * {@code n} selected columns, which will be assigned output slot indices {@code
 * [0, ..., n-1]}.
 */
public class CapNodeSource extends CapNode {

    private final UUID uuid;
    private final int[] cols;

    /**
     * @param index index of this node in the CAP list.
     * @param uuid  the UUID of the source table
     * @param cols  the column indices of the selected source columns.
     */
    public CapNodeSource(final int index, UUID uuid, int[] cols) {
        super(index, CapNodeType.SOURCE);
        this.uuid = uuid;
        this.cols = cols;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("SOURCE(");
        sb.append("uuid=").append(uuid);
        sb.append(", cols=").append(Arrays.toString(cols));
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
}
