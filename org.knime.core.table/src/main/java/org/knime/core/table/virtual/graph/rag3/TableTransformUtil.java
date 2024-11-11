package org.knime.core.table.virtual.graph.rag3;

import static org.knime.core.table.virtual.graph.rag3.SpecType.APPEND;
import static org.knime.core.table.virtual.graph.rag3.SpecType.SLICE;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.knime.core.table.row.Selection;
import org.knime.core.table.virtual.graph.rag3.AccessId.Producer;
import org.knime.core.table.virtual.graph.rag3.TableTransformGraph.Node;
import org.knime.core.table.virtual.graph.rag3.TableTransformGraph.Port;
import org.knime.core.table.virtual.spec.SelectColumnsTransformSpec;
import org.knime.core.table.virtual.spec.SliceTransformSpec;
import org.knime.core.table.virtual.spec.SourceTransformSpec;

public class TableTransformUtil { // TODO (TP) rename

    static UnsupportedOperationException unhandledNodeType() { // TODO: handle or remove OBSERVER case
        return new UnsupportedOperationException("not handled yet. needs to be implemented or removed");
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
    static List<Node> nodes(TableTransformGraph graph) {
        class Nodes {
            final Set<Node> nodes = new LinkedHashSet<>();

            Nodes(TableTransformGraph graph) {
                addRecursively(graph.terminal());
            }

            private void addRecursively(final Port port) {
                port.controlFlowEdges().forEach(e -> {
                    addRecursively(e.to().owner());
                });
                port.accesses().forEach(a -> {
                    addRecursively(a.find().producer().node());
                });
            }

            private void addRecursively(final Node node) {
                if (!nodes.contains(node)) {
                    nodes.add(node);
                    node.in().forEach(port -> addRecursively(port));
                }
            }
        }
        return new ArrayList<>(new Nodes(graph).nodes);
    }






    public static void optimize(TableTransformGraph graph)
    {
        // TODO (TP): optionally, for debugging, save intermediate steps as mermaid graphs

        TableTransformUtil.pruneAccesses(graph);
        while(TableTransformUtil.mergeSlices(graph)) {
        }
    }


    // --------------------------------------------------------------------
    // mergeSlices()

    public static boolean mergeSlices(TableTransformGraph graph) {
        for (Node node : nodes(graph)) {
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
        final Node source = slice.in(0).controlFlowTarget(0);;

        // check whether the source supports efficient row range slicing
        final SourceTransformSpec sourceSpec = source.getTransformSpec();
        if ( !sourceSpec.getProperties().supportsRowRange() ) {
            return false;
        }

        // merge indices from predecessor and slice
        final SliceTransformSpec sliceSpec = slice.getTransformSpec();
        final Selection.RowRangeSelection sourceRange = sourceSpec.getRowRange();
        final Selection.RowRangeSelection sliceRange = sliceSpec.getRowRangeSelection();
        final Selection.RowRangeSelection mergedRange = sourceRange.retain(sliceRange);

        // create new merged SOURCE Node
        final SourceTransformSpec mergedSpec = new SourceTransformSpec(sourceSpec.getSourceIdentifier(), sourceSpec.getProperties(), mergedRange);
        final Node merged = new Node(mergedSpec);
        source.out().accesses().forEach(access -> {
            final int i = access.producer().index();
            final String label = "delta^" + i + "_v" + merged.id();
            final AccessId output = new AccessId(new Producer(merged, i), label);
            access.union(output);
            merged.out().accesses().add(output);
        });
        slice.out().controlFlowEdges().forEach(edge -> edge.relinkTo(merged.out()));
        return true;
    }

    private static boolean mergeSliceToSlice(final Node slice) {
        final Node predecessor = slice.in(0).controlFlowTarget(0);

        // merge row ranges from predecessor and node
        final SliceTransformSpec predecessorSpec = predecessor.getTransformSpec();
        final SliceTransformSpec sliceSpec = slice.getTransformSpec();
        final Selection.RowRangeSelection predecessorRange = predecessorSpec.getRowRangeSelection();
        final Selection.RowRangeSelection sliceRange = sliceSpec.getRowRangeSelection();
        final Selection.RowRangeSelection mergedRange = predecessorRange.retain(sliceRange);

        // TODO: Maybe this is a common operation?
        final Node merged = new Node(new SliceTransformSpec(mergedRange));
        merged.in().add(new Port(merged));
        predecessor.in(0).controlFlowEdges().forEach(edge -> edge.relinkFrom(merged.in(0)));
        slice.out().controlFlowEdges().forEach(edge -> edge.relinkTo(merged.out()));
        return true;
    }







    record AppendAccesses(Port outPort, int outAccessIndex, Port inPort, int inAccessIndex) {
        static AppendAccesses find(AccessId a) {
            AccessId access = a.find();
            Node node = access.producer().node();
            if (node.type() == APPEND) {
                final int i = node.out().accesses().indexOf(access);
                if (i >= 0) {
                    int j = 0;
                    for (Port port : node.in()) {
                        final int n = port.accesses().size();
                        if (j + n > i) {
                            return new AppendAccesses(node.out(), i, port, i - j);
                        }
                        j += n;
                    }
                }
            }
            throw new IllegalArgumentException();
        }

        AccessId input() {
            return inPort.access(inAccessIndex);
        }

        void remove() {
            outPort.accesses().remove(outAccessIndex);
            inPort.accesses().remove(inAccessIndex);
        }
    }

    /**
     * Identify and remove {@code AccessId} that are produced but never used in the {@code graph}.
     *
     * @return {@code true} if any unused access was found and removed
     */
    public static boolean pruneAccesses(TableTransformGraph graph) {
        // recursively find "required" AccessId
        // every input of the terminal port is required.
        // every control flow dependency of the terminal port is required,
        class Required {
            Required(TableTransformGraph graph) {
                final Port port = graph.terminal();
                port.accesses().forEach(this::addRequired);
                port.controlFlowEdges().forEach(edge -> addRequired(edge.to().owner()));
            }

            private void addRequired(Node node) {
                if (requiredNodes.add(node)) {
                    switch (node.type()) {
                        case SOURCE, SLICE, ROWINDEX, APPEND, CONCATENATE -> {
                        }
                        case ROWFILTER, OBSERVER -> node.in(0).accesses().forEach(this::addRequired);
                        default -> throw new IllegalArgumentException();
                    }
                    node.in().forEach( //
                            port -> port.controlFlowEdges().forEach( //
                                    edge -> addRequired(edge.to().owner())));
                }
            }

            private final Set<AccessId> requiredAccessIds = new HashSet<>();

            private final Set<Node> requiredNodes = new HashSet<>();

            private void addRequired(AccessId a) {
                final AccessId access = a.find();
                if (requiredAccessIds.add(access)) {
                    final Node node = access.producer().node();
                    switch (node.type()) {
                        case SOURCE, ROWINDEX -> {
                        }
                        case APPEND -> addRequired(AppendAccesses.find(access).input());
                        case CONCATENATE -> {
                            final int i = node.out().accesses().indexOf(access);
                            node.in().forEach(in -> addRequired(in.access(i)));
                        }
                        case MAP -> {
                            requiredNodes.add(node);
                            node.in(0).accesses().forEach(this::addRequired);
                        }
                        default -> throw new IllegalArgumentException();
                    }
                }
            }

            /**
             * @return true if any unused access was found and removed
             */
            boolean pruneAccesses() {
                boolean pruned = false;
                for (Node node : requiredNodes) {
                    final ArrayList<AccessId> unused = new ArrayList<>();
                    node.out().accesses().forEach(a -> {
                        AccessId access = a.find();
                        if (!requiredAccessIds.contains(access)) {
                            unused.add(access);
                        }
                    });
                    if (!unused.isEmpty()) {
                        pruned = true;
                        unused.forEach(access -> {
                            switch (node.type()) {
                                case SOURCE, MAP, ROWINDEX -> node.out().accesses().remove(access);
                                case APPEND -> AppendAccesses.find(access).remove();
                                case CONCATENATE -> {
                                    final int i = node.out().accesses().indexOf(access);
                                    node.out().accesses().remove(i);
                                    node.in().forEach(in -> in.accesses().remove(i));
                                }
                                case SLICE, ROWFILTER -> {
                                }
                                case OBSERVER -> throw TableTransformUtil.unhandledNodeType();
                                default -> throw new IllegalArgumentException();
                            }
                        });
                    }
                }
                return pruned;
            }
        }

        return new Required(graph).pruneAccesses();
    }














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
}
