package org.knime.core.table.virtual.graph.rag3;

import static org.knime.core.table.virtual.graph.rag3.SpecGraph.DependencyGraph.DependencyType.CONTROL;
import static org.knime.core.table.virtual.graph.rag3.SpecGraph.DependencyGraph.DependencyType.DATA;
import static org.knime.core.table.virtual.graph.rag3.SpecType.COLSELECT;
import static org.knime.core.table.virtual.graph.rag3.SpecType.ROWFILTER;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.IntFunction;
import java.util.function.IntUnaryOperator;

import org.knime.core.table.virtual.TableTransform;
import org.knime.core.table.virtual.graph.rag.ConsumerTransformSpec;
import org.knime.core.table.virtual.graph.rag3.SpecGraph.DependencyGraph.DependencyEdge;
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

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
            sb.append(label);
            final AccessId setLeader = find();
            if (setLeader != this) {
                sb.append(" -> ").append(setLeader);
            }
            return sb.toString();
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
    static IntFunction<String> accessLabel(final String varName, final int nodeId, final int predecessorIndex) {
        return i -> {
            String label = varName + "^" + i + "_v" + nodeId;
            if (predecessorIndex >= 0)
                label += "(" + predecessorIndex + ")";
            return label;
        };
    }




    //
    // AccessIds
    //
    // -----------------------------------------------------------------------------------------------------------------



    record ControlFlowEdge(Port from, Port to) {
        /**
         * Remove this {@code ControlFlowEdge} from its target {@code Port} and
         * replace it with a new edge from {@code from}.
         *
         * @return the new edge
         * @throws IllegalStateException if the target {@code Port} does not have exactly one edge.
         */
        ControlFlowEdge relinkFrom(Port from) throws IllegalStateException {
            if (to.controlFlowEdges.size() != 1)
                throw new IllegalStateException();
            final ControlFlowEdge e = new ControlFlowEdge(from, to);
            from.controlFlowEdges.add(e);
            to.controlFlowEdges.clear();
            to.controlFlowEdges.add(e);
            return e;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("ControlFlowEdge{");
            sb.append("from=").append(from.owner() == null ? "terminal" : from.owner().id());
            sb.append(", to=").append(to.owner().id());
            sb.append('}');
            return sb.toString();
        }
    }

    record Port(Node owner, List<AccessId> accesses, List<ControlFlowEdge> controlFlowEdges) {
        Port(Node owner, List<AccessId> accesses) {
            this(owner, accesses, new ArrayList<>());
        }

        AccessId access(int i) {
            return accesses.get(i);
        }

        void linkTo(Port to) {
            final ControlFlowEdge e = new ControlFlowEdge(this, to);
            controlFlowEdges.add(e);
            to.controlFlowEdges().add(e);
        }
    }

    static class Node {
        private final TableTransformSpec spec;
        private final SpecType type;
        private final List<Port> in = new ArrayList<>();
        private final Port out;

        // TODO consumer-like artificial node to anchor DependencyGraph
        Node(Port inPort) {
            spec = new ConsumerTransformSpec();
            type = null;
            in.add(inPort);
            out = null;
        }

        Node(final TableTransformSpec spec, final int numColumns, final List<Terminal> predecessors) {
            this.spec = spec;
            type = SpecType.forSpec(spec);

            for (int p = 0; p < predecessors.size(); p++) {
                final Terminal predecessor = predecessors.get(p);
                final int numInputs = switch (type) {
                    case SOURCE, SLICE, ROWINDEX  -> 0;
                    case MAP, ROWFILTER -> getColumnSelection(spec).length;
                    case APPEND, CONCATENATE -> predecessor.numColumns();
                    case OBSERVER -> throw unhandledNodeType();
                    // TODO: COLSELECT shouldn't be a possible value here --> SpecType vs NodeType
                    case COLSELECT -> throw new IllegalArgumentException();
                };

                final List<AccessId> inputs = createAccessIds(null, numInputs, accessLabel("gamma", id, p));
                final Port inPort = new Port(this, inputs);

                // access tracing:
                // link inputs to predecessor outCols
                switch (type) {
                    case MAP, ROWFILTER -> {
                        final int[] selection = getColumnSelection(spec);
                        unionAccesses(inPort, predecessor.port, numInputs, i -> selection[i]);
                    }
                    default -> unionAccesses(inPort, predecessor.port,numInputs);
                }

                // control flow:
                switch(type) {
                    case ROWFILTER -> {
                        final ControlFlowEdge predecessorEdge = predecessor.port.controlFlowEdges().get(0);
                        final Node predecessorNode = predecessorEdge.to().owner();
                        if (predecessorNode.type == ROWFILTER) {
                            // if predecessor links to a ROWFILTER (one or more)
                            // link this node to the target of that ROWFILTER's controlFlowEdge
                            final Port target = predecessorNode.in.get(0).controlFlowEdges().get(0).to();
                            inPort.linkTo(target);
                        } else {
                            // otherwise re-link the predecessor controlFlowEdge
                            // (there is only one) to this node
                            predecessorEdge.relinkFrom(inPort);
                        }
                    }
                    case SLICE, ROWINDEX, APPEND, CONCATENATE -> {
                        // re-link the predecessor controlFlowEdges to this Node
                        predecessor.port.controlFlowEdges().forEach(e -> e.relinkFrom(inPort));
                    }
                    case OBSERVER -> throw unhandledNodeType();
                    default -> {
                    }
                }

                in.add(inPort);
            }

            final int numOutputs = switch (type) {
                case SOURCE, MAP, APPEND, CONCATENATE -> numColumns;
                case ROWINDEX -> 1;
                case SLICE, ROWFILTER -> 0;
                case OBSERVER -> throw unhandledNodeType();
                case COLSELECT -> throw new IllegalArgumentException();
            };
            final List<AccessId> outputs = createAccessIds(this, numOutputs, accessLabel("delta", id, -1));
            out = new Port(this, outputs);
        }

        AccessId output(int i) {
            return out.access(i);
        }

        // TODO unused? remove?
        AccessId input(int i) {
            return input(0, i);
        }

        // TODO unused? remove?
        AccessId input(int p, int i) {
            return in.get(p).access(i);
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("Node{");
            sb.append("type=").append(type);
            sb.append(", id=").append(id);
            sb.append('}');
            return sb.toString();
        }

        // ids are just for printing ...
        private static int nextNodeId = 0;
        private final int id = nextNodeId++;

        /**
         * To make it easier to identify nodes in debug output, each node is assigned a
         * unique id on construction.
         * <p>
         * <em>This might be removed later. Please do not rely on uniqueness of {@code
         * id()}!</em>
         */
        public int id() {
            return id;
        }
    }

    static class Terminal {
        final Port port;

        Terminal(final TableTransform t) {
            this(t.getSpec(), t.getPrecedingTransforms().stream().map(Terminal::new).toList());
        }

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

            // access tracing:
            switch (type) {
                case SOURCE, MAP, APPEND, CONCATENATE -> {
                    // link outCols to node's outputs
                    unionAccesses(port, node.out, numColumns);
                }
                case SLICE, ROWFILTER -> {
                    // link outCols to the predecessor's outCols
                    // (there is exactly one predecessor)
                    unionAccesses(port, predecessors.get(0).port, numColumns);
                }
                case ROWINDEX -> {
                    // link outCols (except the last one) to the predecessor's outCols
                    // (there is exactly one predecessor)
                    unionAccesses(port, predecessors.get(0).port, numColumns - 1);
                    // link the last outCol to the node's output
                    outCol(numColumns - 1).union(node.output(0));
                }
                case COLSELECT -> {
                    // apply selection to predecessor's outCols
                    // (there is exactly one predecessor)
                    final int[] selection = getColumnSelection(spec);
                    unionAccesses(port, predecessors.get(0).port, numColumns, i -> selection[i]);
                }
            }

            // control flow:
            switch (type) {
                case SOURCE, SLICE, ROWINDEX, APPEND, CONCATENATE -> {
                    // link to the new node
                    port.linkTo(node.out);
                }
                case MAP, COLSELECT -> {
                    // link to everything that was linked to by predecessor
                    // (there is exactly one predecessor)
                    final List<ControlFlowEdge> predecessorControlFlowEdges = predecessors.get(0).port.controlFlowEdges();
                    predecessorControlFlowEdges.forEach(e -> e.relinkFrom(port));
                }
                case ROWFILTER -> {
                    // link to other ROWFILTERs that were linked to by predecessor
                    // (there is exactly one predecessor)
                    final List<ControlFlowEdge> predecessorControlFlowEdges = predecessors.get(0).port.controlFlowEdges();
                    if (predecessorControlFlowEdges.get(0).to().owner().type == ROWFILTER) {
                        predecessorControlFlowEdges.forEach(e -> e.relinkFrom(port));
                    }
                    // link to the new node
                    port.linkTo(node.out);
                }
            }
        }

        int numColumns() {
            return port.accesses().size();
        }

        AccessId outCol(int i) {
            return port.access(i);
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("Terminal{");
            sb.append("port=").append(port);
            sb.append('}');
            return sb.toString();
        }
    }

    private static void unionAccesses(Port from, Port to, int n) {
        for (int i = 0; i < n; i++) {
            from.access(i).union(to.access(i));
        }
    }

    private static void unionAccesses(Port from, Port to, int n, IntUnaryOperator indexMapper) {
        for (int i = 0; i < n; i++) {
            from.access(i).union(to.access(indexMapper.applyAsInt(i)));
        }
    }

    private static int[] getColumnSelection(TableTransformSpec spec) {
        return switch (SpecType.forSpec(spec)) {
            case MAP -> ((MapTransformSpec)spec).getColumnSelection();
            case ROWFILTER -> ((RowFilterTransformSpec)spec).getColumnSelection();
            case COLSELECT -> ((SelectColumnsTransformSpec)spec).getColumnSelection();
            default -> throw new IllegalArgumentException();
        };
    }

    private static UnsupportedOperationException unhandledNodeType() {
        return new UnsupportedOperationException("not handled yet. needs to be implemented or removed");
    }



    static class DependencyGraph {
        enum DependencyType {DATA, CONTROL}
        record DependencyEdge(DependencyType type, Node from, Node to) {}

        private final Node consumer;
        private final Set<Node> nodes = new HashSet<>();
        private final Set<DependencyEdge> edges = new HashSet<>();

        DependencyGraph(Terminal terminal)
        {
            consumer = new Node(terminal.port);
            addRecursively(consumer);
        }

        void addRecursively(Node node) {
            if (nodes.add(node)) {
                node.in.forEach(port -> {
                    port.controlFlowEdges().forEach(e -> {
                        final Node target = e.to().owner();
                        addRecursively(target);
                        edges.add(new DependencyEdge(CONTROL, node, target));
                    });
                    port.accesses().forEach(a -> {
                        final Node target = a.find().producer().node();
                        addRecursively(target);
                        edges.add(new DependencyEdge(DATA, node, target));
                    });
                });
            }
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("DependencyGraph{");
            sb.append("nodes=").append(nodes);
            sb.append(", edges=").append(edges);
            sb.append('}');
            return sb.toString();
        }
    }

    public static String mermaid(final DependencyGraph graph, final boolean darkMode) {
        final var sb = new StringBuilder("graph BT\n");
        for (final Node node : graph.nodes) {
            final String name = "<" + node.id() + "> " + node.spec;
            sb.append("  " + node.id() + "(\"" + name + "\")\n");
        }
        int edgeId = 0;
        for (final DependencyEdge edge : graph.edges) {
            sb.append("  " + edge.from().id() + "--> " + edge.to().id() + "\n");
            sb.append("  linkStyle " + edgeId + " stroke:");
            sb.append(switch (edge.type()) {
                case DATA -> (darkMode ? "blue" : "#8888FF,anything");
                case CONTROL -> (darkMode ? "red" : "#FF8888,anything");
            });
            sb.append(";\n");
            ++edgeId;
        }
        return sb.toString();
    }

    /**
     * Builds a spec graph from {@code tableTransform}.
     *
     * @param tableTransform the producingTransform of the table
     * @return spec graph representing the given table
     */
    public static Terminal buildSpecGraph(final TableTransform tableTransform) {
        return new Terminal(tableTransform);
    }

}
