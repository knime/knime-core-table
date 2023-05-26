package org.knime.core.table.virtual.graph.cap;

/**
 * Represents a row index operation in the CAP.
 * <p>
 * A {@code CapNodeRowIndex} knows the index of the predecessor {@code CapNode}.
 * TODO: will probably evolve to have starting index
 */
public class CapNodeRowIndex extends CapNode {

    private final int predecessor;
    private final long offset;

    /**
     * @param index index of this node in the CAP list.
     * @param offset offset to add to row index.
     * @param predecessor index of the predecessor node in the CAP list.
     */
    public CapNodeRowIndex(final int index, final int predecessor, final long offset) {
        super(index, CapNodeType.ROWINDEX);
        this.predecessor = predecessor;
        this.offset = offset;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ROWINDEX(");
        sb.append("predecessor=").append(predecessor);
        sb.append(", offset=").append(offset);
        sb.append(')');
        return sb.toString();
    }

    /**
     * A {@code CapNodeRowIndex} has exactly one predecessor. Calling {@code forward()} on
     * the (instantiation of the) Slice will call {@code forward()} on the
     * (instantiation of the) predecessor.
     *
     * @return the index of the predecessor node in the CAP list.
     */
    public int predecessor() {
        return predecessor;
    }

    /**
     * @return offset to add to row index.
     */
    public long offset() {
        return offset;
    }
}
