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

import static org.knime.core.table.virtual.graph.rag.SpecType.APPEND;
import static org.knime.core.table.virtual.graph.rag.SpecType.CONCATENATE;
import static org.knime.core.table.virtual.graph.rag.SpecType.ROWFILTER;
import static org.knime.core.table.virtual.graph.rag.SpecType.ROWINDEX;
import static org.knime.core.table.virtual.graph.rag.SpecType.SLICE;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.knime.core.table.row.Selection;
import org.knime.core.table.row.Selection.RowRangeSelection;
import org.knime.core.table.virtual.graph.debug.VirtualTableDebugging;
import org.knime.core.table.virtual.graph.debug.VirtualTableDebugging.TableTransformGraphLogger;
import org.knime.core.table.virtual.graph.rag.AccessId.Producer;
import org.knime.core.table.virtual.graph.rag.TableTransformGraph.ControlFlowEdge;
import org.knime.core.table.virtual.graph.rag.TableTransformGraph.Node;
import org.knime.core.table.virtual.graph.rag.TableTransformGraph.Port;
import org.knime.core.table.virtual.spec.RowIndexTransformSpec;
import org.knime.core.table.virtual.spec.SelectColumnsTransformSpec;
import org.knime.core.table.virtual.spec.SliceTransformSpec;
import org.knime.core.table.virtual.spec.SourceTransformSpec;
import org.knime.core.table.virtual.spec.TableTransformSpec;

public class TableTransformUtil { // TODO (TP) rename

    /**
     * Creates a copy of {@code graph}, with SLICE and COLFILTER operations
     * appended that implement the given {@code selection}.
     * <p>
     * If {@link Selection#allSelected()}, then {@code graph} is returned as-is
     * (no copy).
     *
     * @param graph a TableTransformGraph
     * @param selection the selection (columns and row range) to append
     * @return copy of TableTransformGraph with appended selection
     */
    public static TableTransformGraph appendSelection(TableTransformGraph graph, final Selection selection) {
        if (!selection.rows().allSelected()) {
            graph = graph.append(new SliceTransformSpec(selection.rows()));
        }
        if (!selection.columns().allSelected()) {
            graph = graph.append(new SelectColumnsTransformSpec(selection.columns().getSelected()));
        }
        return graph;
    }


    // --------------------------------------------------------------------
    // optimize()

    public static void optimize(final TableTransformGraph graph) {
        optimize(graph, new VirtualTableDebugging.NullLogger());
    }

    public static void optimize(final TableTransformGraph graph, final TableTransformGraphLogger logger) {
        PruneAccesses.pruneAccesses(graph);
        logger.appendGraph("optimize()", "trim unused nodes and edges", graph);
        boolean changed = true;
        while (changed) {
            changed = false;
            List<Node> nodes = nodes(graph);
            if (moveSlices(nodes)) {
                logger.appendGraph("moveSlices", "(optimize step)", graph);
                changed = true;
            }
            else if (mergeSlices(nodes)) {
                logger.appendGraph("mergeSlices", "(optimize step)", graph);
                changed = true;
            }
            else if (eliminateSingletonConcatenates(nodes)) {
                logger.appendGraph("eliminateSingletonConcatenates", "(optimize step)", graph);
                changed = true;
            }
            else if (eliminateUnusedRowIndexes(nodes)) {
                logger.appendGraph("eliminateUnusedRowIndexes", "(optimize step)", graph);
                changed = true;
            }
            else if (mergeRowIndexSequences(nodes)) {
                logger.appendGraph("mergeRowIndexSequences", "(optimize step)", graph);
                changed = true;
            }
        }
    }

    /**
     * Get all nodes in the given {@code TableTransformGraph}. Starting from
     * {@code graph.terminal()}, recursively follows control-flow and data
     * dependencies.
     * <p>
     * The returned {@code List} contains no duplicates.
     *
     * @param graph the TableTransformGraph
     * @return list of all nodes in {@code graph}
     */
    static List<Node> nodes(final TableTransformGraph graph) {
        return new ArrayList<>(new CollectNodes(graph).m_nodes);
    }

    private static class CollectNodes {

        final Set<Node> m_nodes = new LinkedHashSet<>();

        CollectNodes(final TableTransformGraph graph) {
            addRecursively(graph.terminal());
        }

        private void addRecursively(final Port port) {
            port.controlFlowEdges().forEach(e -> addRecursively(e.to().owner()));
            port.accesses().forEach(a -> addRecursively(a.find().producer().node()));
        }

        private void addRecursively(final Node node) {
            if (!m_nodes.contains(node)) {
                m_nodes.add(node);
                node.in().forEach(this::addRecursively);
            }
        }
    }


    /**
     * Relates output access of an APPEND node to the corresponding input access.
     * <p>
     * Use {@code AppendAccesses.find(a)} (where {@code AccessId a} is an output
     * of a APPEND node) to get the corresponding input port and index.
     *
     * @param outPort out port of the APPEND node
     * @param outAccessIndex index of an access in {@code outPort}
     * @param inPort in port of the APPEND node that contains the corresponding access
     * @param inAccessIndex index of the corresponding access in {@code inPort}
     */
    record AppendAccesses(Port outPort, int outAccessIndex, Port inPort, int inAccessIndex) {
        static AppendAccesses find(final AccessId a) {
            AccessId access = a.find();
            Node node = access.producer().node();
            if (node.type() != APPEND) {
                throw new IllegalArgumentException("the specified AccessId must be an APPEND node output");
            }
            final int i = node.out().accesses().indexOf(access);
            if (i < 0) {
                throw new IllegalStateException("Access not found in the outputs of its producer");
            }
            int j = 0;
            for (Port port : node.in()) {
                final int n = port.accesses().size();
                if (j + n > i) {
                    return new AppendAccesses(node.out(), i, port, i - j);
                }
                j += n;
            }
            throw new IllegalStateException("Number of inputs and outputs of APPEND node differ");
        }

        AccessId input() {
            return inPort.access(inAccessIndex);
        }

        void remove() {
            outPort.accesses().remove(outAccessIndex);
            inPort.accesses().remove(inAccessIndex);
        }
    }


    // --------------------------------------------------------------------
    // mergeSlices()

    public static boolean mergeSlices(final List<Node> nodes) {
        for (Node node : nodes) {
            if (node.type() == SLICE && tryMergeSlice(node)) {
                return true;
            }
        }
        return false;
    }

    private static boolean tryMergeSlice(final Node slice) {
        final Node predecessor = slice.in(0).controlFlowTarget(0);
        return switch (predecessor.type()) {
            case SOURCE -> mergeSliceToSource(slice);
            case SLICE -> mergeSliceToSlice(slice);
            default -> false;
        };
    }

    private static boolean mergeSliceToSource(final Node slice) {
        final Node source = slice.in(0).controlFlowTarget(0);

        // check whether the source supports efficient row range slicing
        final SourceTransformSpec sourceSpec = source.getTransformSpec();
        if ( !sourceSpec.getProperties().supportsRowRange() ) {
            return false;
        }

        // merge indices from predecessor and slice
        final SliceTransformSpec sliceSpec = slice.getTransformSpec();
        final RowRangeSelection sourceRange = sourceSpec.getRowRange();
        final RowRangeSelection sliceRange = sliceSpec.getRowRangeSelection();
        final RowRangeSelection mergedRange = sourceRange.retain(sliceRange);

        // create new merged SOURCE Node
        final SourceTransformSpec mergedSpec =
            new SourceTransformSpec(sourceSpec.getSourceIdentifier(), sourceSpec.getProperties(), mergedRange);
        final Node merged = new Node(mergedSpec);
        source.out().accesses().forEach(access -> {
            final int i = access.producer().index();
            final String label = "delta^" + i + "_v" + merged.id();
            final AccessId output = new AccessId(new Producer(merged, i), label);
            access.union(output);
            merged.out().accesses().add(output);
        });
        slice.out().forEachControlFlowEdge(edge -> edge.relinkTo(merged.out()));
        return true;
    }

    private static boolean mergeSliceToSlice(final Node slice) {
        final Node predecessor = slice.in(0).controlFlowTarget(0);

        // merge row ranges from predecessor and node
        final SliceTransformSpec predecessorSpec = predecessor.getTransformSpec();
        final SliceTransformSpec sliceSpec = slice.getTransformSpec();
        final RowRangeSelection predecessorRange = predecessorSpec.getRowRangeSelection();
        final RowRangeSelection sliceRange = sliceSpec.getRowRangeSelection();
        final RowRangeSelection mergedRange = predecessorRange.retain(sliceRange);

        // replace slice and predecessor by merged SLICE node
        final Node merged = new Node(new SliceTransformSpec(mergedRange));
        merged.in().add(new Port(merged));
        predecessor.in(0).forEachControlFlowEdge(edge -> edge.relinkFrom(merged.in(0)));
        slice.out().forEachControlFlowEdge(edge -> edge.relinkTo(merged.out()));
        return true;
    }


    // --------------------------------------------------------------------
    // moveSlices()

    public static boolean moveSlices(final List<Node> nodes) {
        for (Node node : nodes) {
            if (node.type() == SLICE && tryMoveSlice(node)) {
                return true;
            }
        }
        return false;
    }

    private static boolean tryMoveSlice(final Node slice) {
        final Node predecessor = slice.in(0).controlFlowTarget(0);
        return switch (predecessor.type()) {
            case APPEND -> moveSliceBeforeAppend(slice);
            case CONCATENATE -> moveSliceBeforeConcatenate(slice);
            case ROWINDEX -> moveSliceBeforeRowIndex(slice);
            default -> false;
        };
    }

    private static boolean moveSliceBeforeAppend(final Node slice) {
        final SliceTransformSpec sliceSpec = slice.getTransformSpec();

        final Node append = slice.in(0).controlFlowTarget(0);
        append.in().forEach(port -> {
            final Node preslice = new Node(sliceSpec);
            preslice.in().add(new Port(preslice));
            port.forEachControlFlowEdge(edge -> edge.relinkFrom(preslice.in(0)));
            port.linkTo(preslice);
        });

        slice.out().forEachControlFlowEdge(edge -> edge.relinkTo(append.out()));
        return true;
    }

    private static boolean moveSliceBeforeConcatenate(final Node slice) {
        final SliceTransformSpec sliceSpec = slice.getTransformSpec();
        final RowRangeSelection sliceRange = sliceSpec.getRowRangeSelection();
        final long s0 = sliceRange.fromIndex();
        final long s1 = sliceRange.toIndex();

        final Node concatenate = slice.in(0).controlFlowTarget(0);

        // make a copy because we might modify the conatenate.in() list
        final List<Port> inPorts = new ArrayList<>(concatenate.in());
        // row index of first row of current predecessor
        long r0 = 0;
        // how many rows (from the front) have been removed by eliminating or slicing predecessors
        long numRemovedRows = 0;
        for (int i = 0; i < inPorts.size(); i++) {
            final Port port = inPorts.get(i);
            final long r = TableTransformGraphProperties.numRows(port);
            if (r < 0) {
                // Cannot move the SLICE because slicing into a predecessor with
                // unknown number of rows.
                //
                // Note that although the SLICE can't be moved, we may still have
                // removed input tables that fall before the start of the sliced range,
                if ( i > 0 ) {
                    // The SLICE needs to be modified with a new RowRange,
                    // shifted by numRemovedRows.
                    // That is, [0, s1 - numRemovedRows).
                    replaceSpec(slice, new SliceTransformSpec(RowRangeSelection.all().retain(0, s1 - numRemovedRows)));
                    return true;
                }
                return false;
            }
            final long r1 = r0 + r; // row index of first row of next predecessor
            if (r1 <= s0) {
                // remove this predecessor
                concatenate.in().remove(port);
                numRemovedRows += r;
            } else {
                final long t0 = Math.max(0, s0 - r0);
                final long t1 = Math.min(r, s1 - r0);
                if (t0 != 0 || t1 != r) {
                    // keep only slice(t0, t1) of this predecessor
                    final Node preslice = new Node(new SliceTransformSpec(RowRangeSelection.all().retain(t0, t1)));
                    preslice.in().add(new Port(preslice));
                    port.forEachControlFlowEdge(edge -> edge.relinkFrom(preslice.in(0)));
                    port.linkTo(preslice);
                    numRemovedRows += t0;
                }
                // otherwise just keep the predecessor (no slicing)
            }
            r0 = r1;
            if (r0 >= s1) {
                // All remaining predecessors can be removed
                concatenate.in().removeAll(inPorts.subList(i + 1, inPorts.size()));
                break;
            }
        }

        // remove the slice
        slice.out().forEachControlFlowEdge(edge -> edge.relinkTo(concatenate.out()));
        return true;
    }

    private static Node replaceSpec(final Node oldNode, final TableTransformSpec newSpec ) {
        var newNode = new Node(newSpec);
        for (Port oldInPort : oldNode.in()) {
            var newInPort = new Port(newNode);
            newNode.in().add(newInPort);
            newInPort.accesses().addAll(oldInPort.accesses());
            oldInPort.forEachControlFlowEdge(edge -> edge.relinkFrom(newInPort));
        }
        var oldOutPort = oldNode.out();
        var newOutPort = newNode.out();
        for (AccessId oldAccess : oldOutPort.accesses()) {
            final AccessId newAccess =
                new AccessId(new Producer(newNode, oldAccess.producer().index()), oldAccess.label());
            oldAccess.union(newAccess);
            newOutPort.accesses().add(newAccess);
        }
        oldOutPort.forEachControlFlowEdge(edge -> edge.relinkTo(newOutPort));
        return newNode;
    }

    private static boolean moveSliceBeforeRowIndex(final Node slice) {
        final ControlFlowEdge edge = slice.in(0).controlFlowEdges().get(0);
        final Node rowIndex = edge.to().owner();
        edge.remove();

        rowIndex.in(0).forEachControlFlowEdge(e -> e.relinkFrom(slice.in(0)));
        slice.out().forEachControlFlowEdge(e -> e.relinkTo(rowIndex.out()));
        rowIndex.in(0).linkTo(slice);

        // add offset to rowIndex to compensate for slice
        final SliceTransformSpec sliceSpec = slice.getTransformSpec();
        final RowIndexTransformSpec rowIndexSpec = rowIndex.getTransformSpec();
        final long offset = rowIndexSpec.getOffset() + Math.max(sliceSpec.getRowRangeSelection().fromIndex(), 0);
        replaceSpec(rowIndex, new RowIndexTransformSpec(offset));

        return true;
    }


    // --------------------------------------------------------------------
    // eliminateSingletonConcatenates()

    public static boolean eliminateSingletonConcatenates(final List<Node> nodes) {
        for (Node node : nodes) {
            if (node.type() == CONCATENATE && node.in().size() == 1) {
                eliminate(node);
                return true;
            }
        }
        return false;
    }

    private static void eliminate(final Node node) {
        if (node.in().size() != 1) {
            throw new IllegalArgumentException("node must have exactly one input port");
        }

        // union output accesses to input accesses
        unionAccesses(node.out(), node.in(0));

        // relink controlFlowEdges
        //
        // (This handles correctly cases where multiple ROWFILTERs come
        // before and/or after the node.)

        final Node predecessor = node.in(0).controlFlowTarget(0);
        final Node successor = node.out().controlFlowSource(0);

        final Port relinkTarget;
        if ( predecessor.type() == ROWFILTER) {
            relinkTarget = predecessor.in(0).controlFlowTarget(0).out();
        } else {
            relinkTarget = predecessor.out();
            // remove in edge to avoid duplicate
            node.in(0).controlFlowEdges().get(0).remove();
        }

        final Port relinkSource;
        if ( successor != null && successor.type() == ROWFILTER) {
            relinkSource = successor.out().controlFlowEdges().get(0).from();
        } else {
            relinkSource = node.out().controlFlowEdges().get(0).from();
            if (predecessor.type() == ROWFILTER) {
                // remove out edge to avoid duplicate
                node.out().controlFlowEdges().get(0).remove();
            }
        }

        node.in(0).forEachControlFlowEdge(edge -> edge.relinkFrom(relinkSource));
        node.out().forEachControlFlowEdge(edge -> edge.relinkTo(relinkTarget));
    }

    private static void unionAccesses(final Port from, final Port to) {
        final int n = from.accesses().size();
        for (int i = 0; i < n; i++) {
            from.access(i).union(to.access(i));
        }
    }


    // --------------------------------------------------------------------
    // eliminateUnusedRowIndexes()

    public static boolean eliminateUnusedRowIndexes(final List<Node> nodes) {
        for (Node node : nodes) {
            if (node.type() == ROWINDEX && node.out().accesses().isEmpty()) {
                eliminate(node);
                return true;
            }
        }
        return false;
    }


    // --------------------------------------------------------------------
    // mergeRowIndexSequences()

    public static boolean mergeRowIndexSequences(final List<Node> nodes) {
        for (Node node : nodes) {
            if (node.type() == ROWINDEX && tryMergeRowIndexSequence(node)) {
                return true;
            }
        }
        return false;
    }

    private static boolean tryMergeRowIndexSequence(final Node rowIndex) {
        final Node predecessor = rowIndex.in(0).controlFlowTarget(0);
        if ( predecessor.type() == ROWINDEX ) {
            final long o1 = rowIndex.<RowIndexTransformSpec>getTransformSpec().getOffset();
            final long o2 = predecessor.<RowIndexTransformSpec>getTransformSpec().getOffset();
            if ( o1 == o2 ) {
                if (predecessor.out().accesses().isEmpty()) {
                    eliminate(predecessor);
                } else {
                    rowIndex.out().accesses().remove(0).union(predecessor.out().access(0));
                    eliminate(rowIndex);
                }
                return true;
            }
        }
        return false;
    }

}

