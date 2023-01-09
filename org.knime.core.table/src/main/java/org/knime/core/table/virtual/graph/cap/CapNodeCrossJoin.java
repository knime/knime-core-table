package org.knime.core.table.virtual.graph.cap;

import java.util.Arrays;

/**
 * TODO javadoc
 */
public class CapNodeCrossJoin extends CapNode {

    private final int[] predecessors;

    public CapNodeCrossJoin(final int index, final int[] predecessors) {
        super(index, CapNodeType.CROSSJOIN);
        this.predecessors = predecessors;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CROSSJOIN(");
        sb.append("predecessors=").append(Arrays.toString(predecessors));
        sb.append(')');
        return sb.toString();
    }

    /**
     * TODO javadoc
     *   fwd() on this CapNode interleaves fwd() calls on predecessors, such
     *   that all row combinations are visited. For example, with two predecessors:
     *   <ol>
     *      <li>First fwd() will fwd on both predecessors</li></li>
     *      <li>Subsequent fwd() calls will fwd the first predecessor, unless it
     *      is exhausted. In this case, the second predecessor will be fwded and
     *      the first predecessor will be reset</li>
     *   </ol>
     */
    public int[] predecessors() {
        return predecessors;
    }

}
