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
 *
 * History
 *   3 Dec 2024 (pietzsch): created
 */
package org.knime.core.table.virtual.graph.rag;

import static org.knime.core.table.virtual.spec.SourceTableProperties.CursorType.BASIC;
import static org.knime.core.table.virtual.spec.SourceTableProperties.CursorType.LOOKAHEAD;
import static org.knime.core.table.virtual.spec.SourceTableProperties.CursorType.RANDOMACCESS;

import java.util.Arrays;
import java.util.List;
import java.util.function.LongBinaryOperator;

import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.schema.DataSpecs;
import org.knime.core.table.schema.DataSpecs.DataSpecWithTraits;
import org.knime.core.table.virtual.graph.rag.TableTransformGraph.Node;
import org.knime.core.table.virtual.graph.rag.TableTransformGraph.Port;
import org.knime.core.table.virtual.spec.MapTransformSpec;
import org.knime.core.table.virtual.spec.SliceTransformSpec;
import org.knime.core.table.virtual.spec.SourceTableProperties.CursorType;
import org.knime.core.table.virtual.spec.SourceTransformSpec;

public final class TableTransformGraphProperties {

    public static long numRows(final Port port) {
        return numRows(port.controlFlowTarget(0));
    }

    private static long numRows(final Node node) {
        return switch (node.type()) {
            case SOURCE -> node.<SourceTransformSpec>getTransformSpec().numRows();
            case ROWFILTER -> -1;
            case SLICE -> { // NOSONAR
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
            case COLSELECT, MAP, APPENDMAP, APPENDMISSING -> throw new IllegalArgumentException(
                "Unexpected SpecType: " + node.type());
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
        return supportedCursorType(graph.terminal().controlFlowTarget(0));
    }

    private static CursorType supportedCursorType(final Node node) { //NOSONAR This method is not too complex
        return switch (node.type()) {
            case SOURCE -> node.<SourceTransformSpec>getTransformSpec().getProperties().cursorType();
            case ROWFILTER -> BASIC;
            case SLICE, APPEND, ROWINDEX, OBSERVER -> { // NOSONAR
                var cursorType = RANDOMACCESS;
                for (Port port : node.in()) {
                    cursorType = min(cursorType, supportedCursorType(port.controlFlowTarget(0)));
                    if (cursorType == BASIC) {
                        break;
                    }
                }
                yield cursorType;
            }
            case CONCATENATE -> { // NOSONAR
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
            case COLSELECT, MAP, APPENDMAP, APPENDMISSING -> throw new IllegalArgumentException(
                "Unexpected SpecType: " + node.type());
        };
    }

    private static CursorType min(
            final CursorType arg0, final CursorType arg1) {
        return switch (arg0) {
            case BASIC -> BASIC;
            case LOOKAHEAD -> arg1.supportsLookahead() ? LOOKAHEAD : BASIC;
            case RANDOMACCESS -> arg1;
        };
    }

    static ColumnarSchema schema(final TableTransformGraph graph) {
        final List<AccessId> accesses = graph.terminal().accesses();
        final DataSpecWithTraits[] specs = new DataSpecWithTraits[accesses.size()];
        Arrays.setAll(specs, i -> getSpecWithTraits(accesses.get(i)));
        return ColumnarSchema.of(specs);
    }

    private static DataSpecWithTraits getSpecWithTraits(final AccessId access) {
        final AccessId.Producer producer = access.find().producer();
        final Node node = producer.node();
        return switch (node.type()) {
            case SOURCE -> {
                final SourceTransformSpec spec = node.getTransformSpec();
                yield spec.getSchema().getSpecWithTraits(producer.index());
            }
            case APPEND -> getSpecWithTraits(TableTransformUtil.AppendAccesses.find(access).input());
            case CONCATENATE -> {
                final int i = node.out().accesses().indexOf(access.find());
                yield getSpecWithTraits(node.in(0).access(i));
            }
            case MAP -> {
                final MapTransformSpec spec = node.getTransformSpec();
                yield spec.getMapperFactory().getOutputSchema().getSpecWithTraits(producer.index());
            }
            case ROWINDEX -> DataSpecs.LONG;
            default -> throw new IllegalArgumentException("unexpected node type " + node.type());
        };
    }

    private TableTransformGraphProperties() {
        // no instances, just static utility methods
    }
}
