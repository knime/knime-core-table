package org.knime.core.table.virtual.graph.rag3;

import static org.knime.core.table.virtual.graph.rag3.SpecType.COLSELECT;
import static org.knime.core.table.virtual.graph.rag3.SpecType.ROWFILTER;

import java.util.ArrayList;
import java.util.List;
import java.util.function.IntFunction;
import java.util.function.IntUnaryOperator;

import org.knime.core.table.virtual.spec.MapTransformSpec;
import org.knime.core.table.virtual.spec.RowFilterTransformSpec;
import org.knime.core.table.virtual.spec.SelectColumnsTransformSpec;
import org.knime.core.table.virtual.spec.SourceTransformSpec;
import org.knime.core.table.virtual.spec.TableTransformSpec;

public class SpecGraph {

    // -----------------------------------------------------------------------------------------------------------------
    //
    // AccessIds
    //

    record Producer(Node node, int index) {}

    static class AccessId {
        private final Producer producer;

        private AccessId parent;

        private final String label; // TODO (for debugging only)

        public AccessId(final Producer producer, String label) {
            this.producer = producer;
            this.parent = this;
            this.label = label;
        }

        public AccessId(String label) {
            this(null, label);
        }

        public Producer producer() {
            return producer;
        }

        public AccessId find() {
            if (parent != this) {
                final var p = parent.find();
                if (parent != p) {
                    parent = p;
                }
            }
            return parent;
        }

        public void union(final AccessId other) {
            parent = other.find();
        }
    }

    static List<AccessId> createAccessIds(final Node producerNode, final int n, final IntFunction<String> label) {
        final List<AccessId> cols = new ArrayList<>( n );
        for (int i = 0; i < n; i++) {
            Producer producer = producerNode == null ? null : new Producer(producerNode, i);
            cols.add(new AccessId(producer, label.apply(i)));
        }
        return cols;
    }

    // TODO (for debugging only)
    static String accessLabel(final String varName, final int nodeId, final int predecessorIndex, final int i) {
        String label = varName + "^" + i + "_v" + nodeId;
        if (predecessorIndex >= 0)
            label += "(" + predecessorIndex + ")";
        return label;
    }

    //
    // AccessIds
    //
    // -----------------------------------------------------------------------------------------------------------------



    record ControlFlowEdge(Port from, Port to) {}

    record Port(Node owner, List<AccessId> accesses, List<ControlFlowEdge> controlFlowEdges) {
        AccessId access(int i) {
            return accesses.get(i);
        }
    }

    static class Node {
        private final SpecType type;
        private final List<Port> in = new ArrayList<>();
        private final Port out;

        Node(final TableTransformSpec spec, final int numColumns, final List<Terminal> predecessors) {
            type = SpecType.forSpec(spec);

            for (int p = 0; p < predecessors.size(); p++) {
                final Terminal predecessor = predecessors.get(p);
                final int numInputs = switch (type) {
                    case SOURCE, SLICE, ROWINDEX  -> 0;
                    case MAP -> ((MapTransformSpec)spec).getColumnSelection().length;
                    case ROWFILTER -> ((RowFilterTransformSpec)spec).getColumnSelection().length;
                    case APPEND, CONCATENATE -> predecessor.numColumns();
                    case OBSERVER -> throw unhandledNodeType();
                    // TODO: COLSELECT shouldn't be a possible value here --> SpecType vs NodeType
                    case COLSELECT -> throw new IllegalArgumentException();
                };

                // link inputs to predecessor outCols
                final IntUnaryOperator selection = switch (type) {
                    case MAP -> i -> ((MapTransformSpec)spec).getColumnSelection()[i];
                    case ROWFILTER -> i -> ((RowFilterTransformSpec)spec).getColumnSelection()[i];
                    default -> i -> i;
                };
                final int fp = p;
                final List<AccessId> inputs = createAccessIds(this, numInputs, i -> accessLabel("gamma", id, fp, i));
                for (int i = 0; i < numInputs; i++) {
                    inputs.get(i).union(predecessor.outCol(selection.applyAsInt(i)));
                }

                final Port inPort = new Port(this, inputs, new ArrayList<>());
                in.add(inPort);

                final List<ControlFlowEdge> predecessorControlFlowEdges = predecessor.port.controlFlowEdges();
                switch(type) {
                    case ROWFILTER -> {
                        final Node predecessorNode = predecessorControlFlowEdges.get(0).to().owner();
                        if (predecessorNode.type == ROWFILTER) {
                            // if predecessor links to a ROWFILTER (one or more)
                            // link this node to the target of that ROWFILTER's controlFlowEdge
                            // the outside handler should then link to all predecessor ROWFILTERs and this one too (TODO)
                            final Port target = predecessorNode.in.get(0).controlFlowEdges().get(0).to();
                            final ControlFlowEdge edge = new ControlFlowEdge(inPort, target);
                            inPort.controlFlowEdges().add(edge);
                            target.controlFlowEdges().add(edge);
                        } else {
                            // otherwise
                            // re-link the predecessor controlFlowEdge (there is only one) to this node
                            final ControlFlowEdge edge =
                                    new ControlFlowEdge(inPort, predecessorControlFlowEdges.get(0).to());
                            inPort.controlFlowEdges().add(edge);
                            predecessorControlFlowEdges.clear();
                            predecessorControlFlowEdges.add(edge);
                        }
                    }
                    case SLICE, ROWINDEX, APPEND, CONCATENATE -> {
                        // look at predecessor controlFlowEdges
                        // re-link them to this Node
                        // the outside handler should then link to this one (TODO)
                        predecessorControlFlowEdges.forEach(e -> {
                            ControlFlowEdge edge = new ControlFlowEdge(inPort, e.to());
                            inPort.controlFlowEdges().add(edge);
                            e.to().controlFlowEdges().clear();
                            e.to().controlFlowEdges().add(edge);
                        });
                    }
                    case OBSERVER -> throw unhandledNodeType();
                    default -> {
                    }
                }
            }

            final int numOutputs = switch (type) {
                case SOURCE, MAP, APPEND, CONCATENATE -> numColumns;
                case ROWINDEX -> 1;
                case SLICE, ROWFILTER -> 0;
                case OBSERVER -> throw unhandledNodeType();
                case COLSELECT -> throw new IllegalArgumentException();
            };
            final List<AccessId> outputs = createAccessIds(this, numOutputs, i -> accessLabel("delta", id, -1, i));
            out = new Port(this, outputs, new ArrayList<>());
        }

        AccessId output(int i) {
            return out.access(i);
        }

        AccessId input(int i) {
            return input(0, i);
        }

        AccessId input(int p, int i) {
            return in.get(p).access(i);
        }

        // ids are just for printing ...
        private static int nextNodeId = 0;
        private final int id = nextNodeId++;
    }

    static class Terminal {

        final Port port;

        Terminal(final TableTransformSpec spec, final List<Terminal> predecessors) {
            final SpecType type = SpecType.forSpec(spec);
            final int numColumns = switch (type) {
                case SOURCE -> ((SourceTransformSpec)spec).getSchema().numColumns();
                case MAP -> ((MapTransformSpec)spec).getMapperFactory().getOutputSchema().numColumns();
                case ROWINDEX -> predecessors.get(0).numColumns() + 1;
                case COLSELECT -> ((SelectColumnsTransformSpec)spec).getColumnSelection().length;
                case APPEND -> predecessors.stream().mapToInt(Terminal::numColumns).sum();
                case SLICE, ROWFILTER, CONCATENATE -> predecessors.get(0).numColumns();
                case OBSERVER -> throw unhandledNodeType();
            };

            final Node owner = null; // TODO: use dummy consumer node as owner?
            final List<AccessId> accessIds = createAccessIds(null, numColumns, i -> "terminal_" + i);
            final ArrayList<ControlFlowEdge> controlFlowEdges = new ArrayList<>();
            port = new Port(owner, accessIds, controlFlowEdges);

            final Node node = (type == COLSELECT) //
                    ? null // don't create a new node, just permute predecessor's outCols
                    : new Node(spec, numColumns, predecessors);

            // access tracing
            switch(type) {
                case SOURCE, MAP, APPEND, CONCATENATE -> {
                    // link outCols to node's outputs
                    for (int i = 0; i < numColumns; i++) {
                        outCol(i).union(node.output(i));
                    }
                }
                case SLICE, ROWFILTER -> {
                    // link outCols to the predecessor's outCols
                    // (there is exactly one predecessor)
                    final Terminal predecessor = predecessors.get(0);
                    for (int i = 0; i < numColumns; i++) {
                        outCol(i).union(predecessor.outCol(i));
                    }
                }
                case ROWINDEX -> {
                    // link outCols (except the last one) to the predecessor's outCols
                    // (there is exactly one predecessor)
                    final Terminal predecessor = predecessors.get(0);
                    for (int i = 0; i < numColumns - 1; i++) {
                        outCol(i).union(predecessor.outCol(i));
                    }
                    // link the last outCol to the node's output
                    outCol(numColumns - 1).union(node.output(0));
                }
                case COLSELECT -> {
                    // apply selection to predecessor's outCols
                    // (there is exactly one predecessor)
                    final int[] selection = ((SelectColumnsTransformSpec)spec).getColumnSelection();
                    final Terminal predecessor = predecessors.get(0);
                    for (int i = 0; i < numColumns - 1; i++) {
                        outCol(i).union(predecessor.outCol(selection[i]));
                    }
                }
            }

            // control flow:
            switch (type) {
                case SOURCE, SLICE, ROWINDEX, APPEND, CONCATENATE -> {
                    // link to the new node
                    final ControlFlowEdge e = new ControlFlowEdge(port, node.out);
                    port.controlFlowEdges().add(e);
                    node.out.controlFlowEdges().add(e);
                }
                case MAP, COLSELECT -> {
                    // link to everything that was linked to by predecessor
                    // (there is exactly one predecessor)
                    final Terminal predecessor = predecessors.get(0);
                    predecessor.port.controlFlowEdges().forEach(edge -> {
                        // TODO: the following is a candidate for method extraction
                        //       could become: edge.to().reLinkFrom(port)
                        //       or similar
                        final ControlFlowEdge e = new ControlFlowEdge(port, edge.to());
                        port.controlFlowEdges().add(e);
                        edge.to().controlFlowEdges().clear();
                        edge.to().controlFlowEdges().add(e);
                    });
                }
                case ROWFILTER -> {
                    // link to the new node, and all other ROWFILTERs that were linked to by predecessor
                    // (there is exactly one predecessor)
                    final Terminal predecessor = predecessors.get(0);
                    if ( predecessor.port.controlFlowEdges().get(0).to().owner().type == ROWFILTER ) {
                        predecessor.port.controlFlowEdges().forEach(edge -> {
                            final ControlFlowEdge e = new ControlFlowEdge(port, edge.to());
                            port.controlFlowEdges().add(e);
                            edge.to().controlFlowEdges().clear();
                            edge.to().controlFlowEdges().add(e);
                        });
                    }
                    final ControlFlowEdge e = new ControlFlowEdge(port, node.out);
                    port.controlFlowEdges().add(e);
                    node.out.controlFlowEdges().add(e);
                }
            }
        }

        int numColumns() {
            return port.accesses().size();
        }

        AccessId outCol(int i) {
            return port.access(i);
        }

    }



    private static UnsupportedOperationException unhandledNodeType() {
        return new UnsupportedOperationException("not handled yet. needs to be implemented or removed");
    }
}
