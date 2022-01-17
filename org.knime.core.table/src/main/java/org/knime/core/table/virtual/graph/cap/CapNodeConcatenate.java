package org.knime.core.table.virtual.graph.cap;

import java.util.Arrays;

/**
 * TODO javadoc
 */
public class CapNodeConcatenate extends CapNode {

    private final CapAccessId[][] inputs;
    private final int[] predecessors;

    public CapNodeConcatenate(final int index, final CapAccessId[][] inputs, final int[] predecessors) {
        super(index, CapNodeType.CONCATENATE);
        this.inputs = inputs;
        this.predecessors = predecessors;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("CONCATENATE(");
        sb.append("inputs=").append(Arrays.deepToString(inputs));
        sb.append(", predecessors=").append(Arrays.toString(predecessors));
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
     * TODO javadoc
     */
    public int[] predecessors() {
        return predecessors;
    }
}
