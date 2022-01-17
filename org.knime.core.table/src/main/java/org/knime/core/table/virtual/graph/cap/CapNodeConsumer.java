package org.knime.core.table.virtual.graph.cap;

import java.util.Arrays;

/**
 * Represents the result of a CAP.
 * <p>
 * A {@code CapNodeConsumer} knows the {@code CapAccessId}s (producer-slot pairs)
 * of the {@code ReadAccess}es that should be assembled into the output {@code
 * ReadAccessRow}, and the index of the predecessor {@code CapNode}.
 */
public class CapNodeConsumer extends CapNode {

    private final CapAccessId[] inputs;
    private final int predecessor;

    public CapNodeConsumer(final int index, CapAccessId[] inputs, final int predecessor) {
        super(index, CapNodeType.CONSUMER);
        this.inputs = inputs;
        this.predecessor = predecessor;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("CONSUMER(");
        sb.append("predecessor=").append(predecessor);
        sb.append(", inputs=").append(Arrays.toString(inputs));
        sb.append(')');
        return sb.toString();
    }

    /**
     * @return the {@code CapAccessId}s (producer-slot pairs) of the {@code ReadAccess}es
     *         that should be assembled into the output {@code ReadAccessRow}
     */
    public CapAccessId[] inputs() {
        return inputs;
    }

    /**
     * A {@code CapNodeConsumer} has exactly one predecessor. Calling {@code forward()} on
     * the (instantiation of the) consumer will call {@code forward()} on the (instantiation
     * of the) predecessor.
     *
     * @return the index of the predecessor node in the CAP list.
     */
    public int predecessor() {
        return predecessor;
    }
}
