package org.knime.core.table.virtual.graph.cap;

import java.util.Arrays;

import org.knime.core.table.virtual.spec.RowFilterTransformSpec;
import org.knime.core.table.virtual.spec.RowFilterTransformSpec.RowFilterFactory;

/**
 * Represents a row-filter operation in the CAP.
 * <p>
 * A {@code CapNodeRowFilter} knows the {@code CapAccessId}s (producer-slot pairs)
 * of the {@code ReadAccess}es required by the filter operation, the filter
 * operation, and the index of the predecessor {@code CapNode}.
 */
public class CapNodeRowFilter extends CapNode {

    private final CapAccessId[] inputs;
    private final int predecessor;
    private final RowFilterFactory filterFactory;

    public CapNodeRowFilter(final int index, final CapAccessId[] inputs, final int predecessor,
            RowFilterFactory filterFactory) {
        super(index, CapNodeType.ROWFILTER);
        this.inputs = inputs;
        this.predecessor = predecessor;
        this.filterFactory = filterFactory;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ROWFILTER(");
        sb.append("inputs=").append(Arrays.toString(inputs));
        sb.append(", predecessor=").append(predecessor);
        sb.append(", filterFactory=").append(filterFactory);
        sb.append(')');
        return sb.toString();
    }

    /**
     * @return the {@code CapAccessId}s (producer-slot pairs) of the {@code ReadAccess}es required by the filter operation
     */
    public CapAccessId[] inputs() {
        return inputs;
    }

    /**
     * A {@code CapNodeRowFilter} has exactly one predecessor. Calling {@code
     * forward()} on the (instantiation of the) row-filter will repeatedly call {@code
     * forward()} on the (instantiation of the) predecessor and evaluate the filter
     * predicate, until the filter predicated passes or the predecessor is exhausted.
     *
     * @return the index of the predecessor node in the CAP list.
     */
    public int predecessor() {
        return predecessor;
    }

    /**
     * @return the filter factory
     */
    public RowFilterFactory filterFactory() {
        return filterFactory;
    }
}
