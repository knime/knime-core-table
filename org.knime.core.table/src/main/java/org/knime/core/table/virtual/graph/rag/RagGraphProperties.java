/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ---------------------------------------------------------------------
 */
package org.knime.core.table.virtual.graph.rag;

import static org.knime.core.table.virtual.graph.rag.RagBuilder.sinkNodeTypes;
import static org.knime.core.table.virtual.graph.rag.RagEdgeType.EXEC;

import java.util.List;
import java.util.function.LongBinaryOperator;

import org.knime.core.table.cursor.Cursors;
import org.knime.core.table.virtual.spec.SliceTransformSpec;
import org.knime.core.table.virtual.spec.SourceTableProperties.CursorType;
import org.knime.core.table.virtual.spec.SourceTransformSpec;

/**
 * Analyze properties of a linearized {@code RagGraph}, for example the number
 * of rows, the ability to support {@code LookaheadCursor}, and the ability to
 * support {@code RandomAccessCursor}.
 */
public class RagGraphProperties {

    /**
     * Returns the number of rows of the given linearized {@code RagGraph}, or a negative value
     * if the number of rows is unknown.
     *
     * @param orderedRag a linearized {@code RagGraph}
     * @return number of rows at the consumer node of the {@code orderedRag}
     */
    public static long numRows(final List<RagNode> orderedRag) {
        final RagNode node = orderedRag.get(orderedRag.size() - 1);
        if (!sinkNodeTypes.contains(node.type())) {
            throw new IllegalArgumentException();
        }
        return RagGraphProperties.numRows(node);
    }

    /**
     * Returns the {@link CursorType} supported by the given linearized {@code
     * RagGraph} (without additional prefetching and buffering).
     * The result is determined by the {@code CursorType} of the sources, and
     * the presence of ROWFILTER operations, etc.
     *
     * @param orderedRag a linearized {@code RagGraph}
     * @return cursor supported at the consumer node of the {@code orderedRag}
     */
    public static CursorType supportedCursorType(final List<RagNode> orderedRag) {
        final RagNode node = orderedRag.get(orderedRag.size() - 1);
        if (!sinkNodeTypes.contains(node.type())) {
            throw new IllegalArgumentException();
        }
        if (RagGraphProperties.supportsRandomAccess(node))
            return CursorType.RANDOMACCESS;
        else if (RagGraphProperties.supportsLookahead(node))
            return CursorType.LOOKAHEAD;
        else
            return CursorType.BASIC;
    }

    /**
     * Recursively trace along reverse EXEC edges to determine number of rows.
     */
    // TODO (TP) add javadoc on illegal node types
    static long numRows(final RagNode node) {

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
            case CONSUMER, MATERIALIZE, WRAPPER, ROWINDEX, OBSERVER, APPEND ->
                // If any predecessor doesn't know its size, the size of this node is also unknown.
                // Otherwise, the size of this node is max of its predecessors.
                    accPredecessorNumRows(node, Math::max);
            case CONCATENATE ->
                // If any predecessor doesn't know its size, the size of this node is also unknown.
                // Otherwise, the size of this is the sum of its predecessors.
                    accPredecessorNumRows(node, Long::sum);
            case MISSING, APPENDMISSING, COLFILTER, MAP, IDENTITY ->
                    throw new IllegalArgumentException("Unexpected RagNode type " + node.type() + ".");
        };
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

    /**
     * Returns {@code true} if the given {@code node} supports {@code
     * LookaheadCursor}s without additional prefetching and buffering (see
     * {@link Cursors#toLookahead}).
     * <p>
     * This is possible, if all sources provide {@code LookaheadCursor}s and
     * there are no row-filters (or other nodes that would destroy lookahead
     * capability) on the path to the {@code node}.
     *
     * @return {@code true} if lookahead can be supported at {@code node}.
     */
    // TODO (TP) add javadoc on illegal node types
    private static boolean supportsLookahead(final RagNode node) {
        return switch (node.type()) {
            case SOURCE -> node.<SourceTransformSpec>getTransformSpec().getProperties().supportsLookahead();
            case ROWFILTER -> false;
            case SLICE, APPEND, CONCATENATE, CONSUMER, MATERIALIZE, WRAPPER, ROWINDEX, OBSERVER -> {
                for (RagNode predecessor : node.predecessors(EXEC)) {
                    if (!supportsLookahead(predecessor)) {
                        yield false;
                    }
                }
                yield true;
            }
            case APPENDMISSING, COLFILTER, MISSING, MAP, IDENTITY ->
                    throw new IllegalArgumentException("Unexpected RagNode type " + node.type() + ".");
        };
    }

    /**
     * Returns {@code true} if the given {@code node} supports {@code
     * RandomAccessCursor}s without additional prefetching and buffering.
     * <p>
     * This is possible, if all sources are {@code RandomRowAccessible}s and
     * there are no row-filters (or other nodes that would destroy random-access
     * capability.)
     *
     * @return {@code true} if random-access can be supported at {@code node}.
     */
    // TODO (TP) add javadoc on illegal node types
    private static boolean supportsRandomAccess(final RagNode node) {
        return switch (node.type()) {
            case SOURCE -> node.<SourceTransformSpec>getTransformSpec().getProperties().supportsRandomAccess();
            case ROWFILTER -> false;
            case SLICE, APPEND, CONSUMER, MATERIALIZE, WRAPPER, ROWINDEX, OBSERVER -> {
                for (RagNode predecessor : node.predecessors(EXEC)) {
                    if (!supportsRandomAccess(predecessor)) {
                        yield false;
                    }
                }
                yield true;
            }
            case CONCATENATE -> {
                // all predecessors need to support random-access AND
                // all predecessors except the last one need to know numRows()
                List<RagNode> predecessors = node.predecessors(EXEC);
                for (int i = 0; i < predecessors.size(); i++) {
                    RagNode predecessor = predecessors.get(i);
                    if (!supportsRandomAccess(predecessor)) {
                        yield false;
                    }
                    if (i != predecessors.size() - 1 && numRows(predecessor) < 0) {
                        yield false;
                    }
                }
                yield true;
            }
            case APPENDMISSING, COLFILTER, MISSING, MAP, IDENTITY ->
                    throw new IllegalArgumentException("Unexpected RagNode type " + node.type() + ".");
        };
    }
}
