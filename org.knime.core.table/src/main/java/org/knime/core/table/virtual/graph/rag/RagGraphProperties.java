package org.knime.core.table.virtual.graph.rag;

import static org.knime.core.table.virtual.graph.rag.RagEdgeType.EXEC;

import java.util.function.LongBinaryOperator;

import org.knime.core.table.row.Selection;
import org.knime.core.table.virtual.spec.SliceTransformSpec;
import org.knime.core.table.virtual.spec.SourceTransformSpec;

// TODO (TP) make this class package-private?
public class RagGraphProperties {

    /**
     * Recursively trace along reverse EXEC edges to determine number of rows.
     */
    public static long numRows(final RagNode node) {

        switch (node.type()) {
            case SOURCE: {
                return node.<SourceTransformSpec>getTransformSpec().numRows();
            }
            case SLICE: {
                final SliceTransformSpec spec = node.getTransformSpec();
                final long from = spec.getRowRangeSelection().fromIndex();
                final long to = spec.getRowRangeSelection().toIndex();
                // If any predecessor doesn't know its size, the size of this node is also unknown.
                // Otherwise, the size of this node is max of its predecessors.
                final long s = accPredecessorNumRows(node, Math::max);
                return s < 0 ? s : Math.max(0, Math.min(s, to) - from);
            }
            case CONSUMER:
            case MATERIALIZE:
            case WRAPPER:
            case ROWINDEX:
            case OBSERVER:
            case APPEND: {
                // If any predecessor doesn't know its size, the size of this node is also unknown.
                // Otherwise, the size of this node is max of its predecessors.
                // If any predecessor doesn't know its size, the size of this node is also unknown.
                // Otherwise, the size of this node is max of its predecessors.
                return accPredecessorNumRows(node, Math::max);
            }
            case CONCATENATE: {
                // If any predecessor doesn't know its size, the size of this node is also unknown.
                // Otherwise, the size of this is the sum of its predecessors.
                return accPredecessorNumRows(node, Long::sum);
            }
            case ROWFILTER:
                return -1;
            case MISSING:
            case APPENDMISSING:
            case COLFILTER:
            case MAP:
            case IDENTITY:
                throw new IllegalArgumentException(
                        "Unexpected RagNode type " + node.type() + ".");
            default:
                throw new IllegalStateException("Unexpected value: " + node.type());
        }
    }

    /**
     * Accumulate numRows of EXEC predecessors of {@code node}.
     * Returns -1, if at least one predecessor doesn't know its numRows.
     * Returns 0 if there are no predecessors.
     */
    private static long accPredecessorNumRows(final RagNode node, final LongBinaryOperator acc) {
        // If any predecessor doesn't know its size, the size of this node
        // is also unknown (return -1).
        // Otherwise, the size of this node is the accumulated size of its
        // predecessors (via the specified acc operator).
        long size = 0;
        for (final RagNode predecessor : node.predecessors(EXEC)) {
            final long s = numRows(predecessor);
            if ( s < 0 ) {
                return -1;
            }
            size = acc.applyAsLong(size, s);
        }
        return size;
    }




















    // TODO (TP) move supportsRandomAccess here

}
