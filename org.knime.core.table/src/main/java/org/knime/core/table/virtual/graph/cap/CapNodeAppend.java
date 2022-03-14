package org.knime.core.table.virtual.graph.cap;

import java.util.Arrays;

/**
 * TODO javadoc
 */
public class CapNodeAppend extends CapNode {

    private final CapAccessId[] inputs;
    private final int[] predecessors;
    private final int[][] predecessorOutputIndices;

    public CapNodeAppend(final int index, final CapAccessId[] inputs, final int[] predecessors,
            final int[][] predecessorOutputIndices) {
        super(index, CapNodeType.APPEND);
        this.inputs = inputs;
        this.predecessors = predecessors;
        this.predecessorOutputIndices = predecessorOutputIndices;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("APPEND(");
        sb.append("inputs=").append(inputs == null ? "[]" : Arrays.asList(inputs).toString());
        sb.append(", predecessors=").append(Arrays.toString(predecessors));
        sb.append(", predecessorOutputIndices=").append(Arrays.deepToString(predecessorOutputIndices));
        sb.append(')');
        return sb.toString();
    }

    /**
     * TODO javadoc
     */
    public CapAccessId[] inputs() {
        return inputs;
    }

    /**
     * TODO javadoc
     *   fwd() on this CapNode calls fwd() on all predecessors
     *   outputs corresponding to exhausted predecessors (fwd() == false) are switched to missing values
     */
    public int[] predecessors() {
        return predecessors;
    }

    /**
     * TODO javadoc
     *   {@code predecessorOutputIndices()[i]} is the array of output slots to switch to missing when the i-th predecessor is exhausted.
     */
    public int[][] predecessorOutputIndices() {
        return predecessorOutputIndices;
    }
}
