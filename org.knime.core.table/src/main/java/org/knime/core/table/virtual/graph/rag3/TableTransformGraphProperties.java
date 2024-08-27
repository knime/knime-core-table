package org.knime.core.table.virtual.graph.rag3;

import static org.knime.core.table.virtual.spec.SourceTableProperties.CursorType.BASIC;
import static org.knime.core.table.virtual.spec.SourceTableProperties.CursorType.LOOKAHEAD;
import static org.knime.core.table.virtual.spec.SourceTableProperties.CursorType.RANDOMACCESS;

import java.util.function.LongBinaryOperator;

import org.knime.core.table.virtual.graph.rag3.TableTransformGraph.Node;
import org.knime.core.table.virtual.graph.rag3.TableTransformGraph.Port;
import org.knime.core.table.virtual.spec.SliceTransformSpec;
import org.knime.core.table.virtual.spec.SourceTableProperties.CursorType;
import org.knime.core.table.virtual.spec.SourceTransformSpec;

public class TableTransformGraphProperties {

    static long numRows(final Port port) {
        return numRows(port.controlFlowTarget(0));
    }

    private static long numRows(final Node node) {
        return switch (node.type()) {
            case SOURCE -> node.<SourceTransformSpec>getTransformSpec().numRows();
            case ROWFILTER -> -1;
            case SLICE -> {
                final SliceTransformSpec spec = node.getTransformSpec();
                final long from = spec.getRowRangeSelection().fromIndex();
                final long to = spec.getRowRangeSelection().toIndex();
                final long s = accPredecessorNumRows(node, Math::max);
                yield s < 0 ? s : Math.max(0, Math.min(s, to) - from);
            }
            case ROWINDEX, OBSERVER, APPEND ->
                // If any predecessor doesn't know its size, the size of this node is also unknown.
                // Otherwise, the size of this node is max of its predecessors.
                accPredecessorNumRows(node, Math::max);
            case CONCATENATE ->
                // If any predecessor doesn't know its size, the size of this node is also unknown.
                // Otherwise, the size of this is the sum of its predecessors.
                accPredecessorNumRows(node, Long::sum);
            case COLSELECT, MAP -> throw new IllegalArgumentException("Unexpected SpecType: " + node.type());
        };
    }

    /**
     * Accumulate numRows of EXEC predecessors of {@code node}.
     * Returns -1, if at least one predecessor doesn't know its numRows.
     * Returns 0 if there are no predecessors.
     */
    private static long accPredecessorNumRows(final Node node, final LongBinaryOperator acc) {
        long size = 0;
        for (Port port : node.in()) {
            final long s = numRows(port.controlFlowTarget(0));
            if ( s < 0 ) {
                return -1;
            }
            size = acc.applyAsLong(size, s);
        }
        return size;
    }

    static CursorType supportedCursorType(final TableTransformGraph graph) {
        return supportedCursorType(graph.terminal.controlFlowTarget(0));
    }

    private static CursorType supportedCursorType(final Node node) {
        return switch (node.type()) {
            case SOURCE -> node.<SourceTransformSpec>getTransformSpec().getProperties().cursorType();
            case ROWFILTER -> BASIC;
            case SLICE, APPEND, ROWINDEX, OBSERVER -> {
                var cursorType = RANDOMACCESS;
                for (Port port : node.in()) {
                    cursorType = min(cursorType, supportedCursorType(port.controlFlowTarget(0)));
                    if (cursorType == BASIC) {
                        break;
                    }
                }
                yield cursorType;
            }
            case CONCATENATE -> {
                // all predecessors need to support random-access AND
                // all predecessors except the last one need to know numRows()
                var cursorType = RANDOMACCESS;
                for (int i = 0; i < node.in().size(); i++) {
                    Port port = node.in(i);
                    final Node predecessor = port.controlFlowTarget(0);
                    cursorType = min(cursorType, supportedCursorType(predecessor));
                    if (i != node.in().size() - 1 && numRows(predecessor) < 0) {
                        cursorType = min(cursorType, LOOKAHEAD);
                    }
                    if (cursorType == BASIC ) {
                        break;
                    }
                }
                yield cursorType;
            }
            case COLSELECT, MAP -> throw new IllegalArgumentException("Unexpected SpecType: " + node.type());
        };
    }

    private static CursorType min(
            CursorType arg0, CursorType arg1) {
        return switch (arg0) {
            case BASIC -> BASIC;
            case LOOKAHEAD -> arg1.supportsLookahead() ? LOOKAHEAD : BASIC;
            case RANDOMACCESS -> arg1;
        };
    }
}
