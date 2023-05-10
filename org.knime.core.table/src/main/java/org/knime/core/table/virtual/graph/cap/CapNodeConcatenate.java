package org.knime.core.table.virtual.graph.cap;

import java.util.Arrays;

/**
 * TODO javadoc
 */
public class CapNodeConcatenate extends CapNode {

    private final CapAccessId[][] inputs;
    private final int[] predecessors;
    private final long[] predecessorSizes;

    public CapNodeConcatenate(final int index, final CapAccessId[][] inputs, final int[] predecessors, final long[] predecessorSizes) {
        super(index, CapNodeType.CONCATENATE);
        this.inputs = inputs;
        this.predecessors = predecessors;
        this.predecessorSizes = predecessorSizes;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CONCATENATE(");
        sb.append("inputs=").append(Arrays.deepToString(inputs));
        sb.append(", predecessors=").append(Arrays.toString(predecessors));
        sb.append(", predecessorSizes=").append(Arrays.toString(predecessorSizes));
        sb.append(')');
        return sb.toString();
    }

    /**
     * TODO javadoc
     *   first index is predecessor
     *   second index is slot
     *   Forward outputs[i] to inputs[0][i] until first predecessor is exhausted
     *   Then go on to inputs[1]i[] until the second predecessor is exhausted, and so on.
     */
    public CapAccessId[][] inputs() {
        return inputs;
    }

    /**
     * The {@link #index() index} of each predecessor in the CAP list.
     * These are always all {@code < this.index()}.
     */
    public int[] predecessors() {
        return predecessors;
    }

    /**
     * The number of rows for each predecessor (or negative number if unknown).
     */
    public long[] predecessorSizes() {
        return predecessorSizes;
    }
}
