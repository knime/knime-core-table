package org.knime.core.table.virtual.graph.cap;

/**
 * Represents a slice operation in the CAP.
 * <p>
 * A {@code CapNodeSlice} knows the row index range {@code [from, to)}, and the
 * index of the predecessor {@code CapNode}.
 */
public class CapNodeSlice extends CapNode {

    private final int predecessor;
    private final long from;
    private final long to;

    /**
     * @param index index of this node in the CAP list.
     * @param predecessor index of the predecessor node in the CAP list.
     * @param from start (row) index of the slice (inclusive).
     * @param to end (row) index of the slice (exclusive).
     */
    public CapNodeSlice(final int index, final int predecessor, final long from, final long to) {
        super(index, CapNodeType.SLICE);
        this.predecessor = predecessor;
        this.from = from;
        this.to = to;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SLICE(");
        sb.append("predecessor=").append(predecessor);
        sb.append(", from=").append(from);
        sb.append(", to=").append(to);
        sb.append(')');
        return sb.toString();
    }

    /**
     * A {@code CapNodeSlice} has exactly one predecessor. Calling {@code forward()} on
     * the (instantiation of the) Slice will call {@code forward()} on the
     * (instantiation of the) predecessor: The first {@code forward()} advances the
     * predecessor {@link #from()} times. Each subsequent {@code forward()} advances
     * the predecessor once, until predecessor has been advanced {@link #to()}-1 times
     * in total.
     *
     * @return the index of the predecessor node in the CAP list.
     */
    public int predecessor() {
        return predecessor;
    }

    /**
     * @return start (row) index of the slice (inclusive).
     */
    public long from() {
        return from;
    }

    /**
     * @return end (row) index of the slice (exclusive).
     */
    public long to() {
        return to;
    }
}
