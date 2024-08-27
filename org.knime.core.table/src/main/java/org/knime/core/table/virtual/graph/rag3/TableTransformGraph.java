package org.knime.core.table.virtual.graph.rag3;

import static org.knime.core.table.virtual.graph.rag3.SpecType.COLSELECT;
import static org.knime.core.table.virtual.graph.rag3.SpecType.ROWFILTER;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;
import java.util.function.IntUnaryOperator;

import org.knime.core.table.virtual.TableTransform;
import org.knime.core.table.virtual.graph.rag.ConsumerTransformSpec;
import org.knime.core.table.virtual.spec.MapTransformSpec;
import org.knime.core.table.virtual.spec.RowFilterTransformSpec;
import org.knime.core.table.virtual.spec.SelectColumnsTransformSpec;
import org.knime.core.table.virtual.spec.SourceTransformSpec;
import org.knime.core.table.virtual.spec.TableTransformSpec;

public class TableTransformGraph {

    /**
     * Builds a spec graph from {@code tableTransform}.
     *
     * @param tableTransform the producingTransform of the table
     * @return spec graph representing the given table
     */
    public static TableTransformGraph of(final TableTransform tableTransform) {
        return new TableTransformGraph(tableTransform);
    }

    /**
     * Bundle incoming/outgoing accesses and control-flow edges.
     * <p>
     * Every {@code Node} has exactly one {@link Node#out() out} port.
     * <p>
     * Every {@code Node} has exactly one {@link Node#in() in} port, except SOURCE which has none, and APPEND/CONCATENATE which have one or more.
     *
     * @param owner            the node which this port belongs to (as in or out port)
     * @param accesses         the input or output accesses (depending on whether this is an in or out port)
     * @param controlFlowEdges control-flow edges from or to this port (depending on whether this is an in or out port)
     */
    record Port(Node owner, List<AccessId> accesses, List<ControlFlowEdge> controlFlowEdges) {
        Port(Node owner, List<AccessId> accesses) {
            this(owner, accesses, new ArrayList<>());
        }

        Port(Node owner) {
            this(owner, new ArrayList<>());
        }

        AccessId access(int i) {
            return accesses.get(i);
        }

        /**
         * Add a new control-flow edge from this port to the {@link Node#out() out} port of the given {@code Node}.
         * <p>
         * <em>This should be only called on an {@link Node#in() in} port!</em>
         */
        void linkTo(Node to) {
            final ControlFlowEdge e = new ControlFlowEdge(this, to.out());
            controlFlowEdges.add(e);
            to.out().controlFlowEdges().add(e);
        }
    }

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
            return "ControlFlowEdge{" + //
                    "from=" + (from.owner() == null ? "terminal" : from.owner().id()) + //
                    ", to=" + to.owner().id() + "}";
        }
    }

    /**
     * A node in the TableTransformGraph.
     */
    static class Node {
        private final TableTransformSpec spec;
        private final SpecType type;
        private final List<Port> in = new ArrayList<>();
        private final Port out;

        /**
         * Create a dummy CONSUMER node. Only needed for visualization.
         */
        Node() {
            spec = new ConsumerTransformSpec();
            type = null;
            out = null;
        }

        /**
         * Create a new node with the given {@code TableTransformSpec} but without populated inputs or outputs.
         * This is needed to implement {@link #copy()}.
         */
        Node(final TableTransformSpec spec) {
            this.spec = spec;
            type = SpecType.forSpec(spec);
            out = new Port(this);
        }

        Node(final TableTransformSpec spec, final int numColumns, final List<TableTransformGraph> predecessors) {
            this.spec = spec;
            type = SpecType.forSpec(spec);

            for (int p = 0; p < predecessors.size(); p++) {
                final TableTransformGraph predecessor = predecessors.get(p);
                final int numInputs = switch (type) {
                    case SOURCE, SLICE, ROWINDEX -> 0;
                    case MAP, ROWFILTER -> getColumnSelection(spec).length;
                    case APPEND, CONCATENATE -> predecessor.numColumns();
                    case OBSERVER -> throw SpecGraph.unhandledNodeType();
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
                        unionAccesses(inPort, predecessor.terminal, numInputs, i -> selection[i]);
                    }
                    default -> unionAccesses(inPort, predecessor.terminal, numInputs);
                }

                // control flow:
                switch (type) {
                    case ROWFILTER -> {
                        final ControlFlowEdge predecessorEdge = predecessor.terminal.controlFlowEdges().get(0);
                        final Node predecessorNode = predecessorEdge.to().owner();
                        if (predecessorNode.type == ROWFILTER) {
                            // if predecessor links to a ROWFILTER (one or more)
                            // link this node to the target of that ROWFILTER's controlFlowEdge
                            final Port target = predecessorNode.in.get(0).controlFlowEdges().get(0).to();
                            inPort.linkTo(target.owner());
                        } else {
                            // otherwise re-link the predecessor controlFlowEdge
                            // (there is only one) to this node
                            predecessorEdge.relinkFrom(inPort);
                        }
                    }
                    case SLICE, ROWINDEX, APPEND, CONCATENATE -> {
                        // re-link the predecessor controlFlowEdges to this Node
                        predecessor.terminal.controlFlowEdges().forEach(e -> e.relinkFrom(inPort));
                    }
                    case OBSERVER -> throw SpecGraph.unhandledNodeType();
                    default -> {
                    }
                }

                in.add(inPort);
            }

            final int numOutputs = switch (type) {
                case SOURCE, MAP, APPEND, CONCATENATE -> numColumns;
                case ROWINDEX -> 1;
                case SLICE, ROWFILTER -> 0;
                case OBSERVER -> throw SpecGraph.unhandledNodeType();
                case COLSELECT -> throw new IllegalArgumentException();
            };
            final List<AccessId> outputs = createAccessIds(this, numOutputs, accessLabel("delta", id, -1));
            out = new Port(this, outputs);
        }

        public SpecType type() {
            return type;
        }

        public List<Port> in() {
            return in;
        }

        public Port out() {
            return out;
        }

        public <T extends TableTransformSpec> T getTransformSpec() {
            return (T)spec;
        }

        @Override
        public String toString() {
            return "(<" + id + ">, " + type + ", " + spec + ")";
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



    /**
     * Control-flow and accesses at the root (output, CONSUMER, sink, ...) of this {@code TableTransformGraph}.
     */
    final Port terminal;

    TableTransformGraph(final Port terminal) {
        this.terminal = terminal;
    }

    TableTransformGraph(final TableTransform t) {
        this(t.getSpec(), t.getPrecedingTransforms().stream().map(TableTransformGraph::new).toList());
    }

    TableTransformGraph(final TableTransformSpec spec, final List<TableTransformGraph> predecessors) {
        final SpecType type = SpecType.forSpec(spec);
        final int numColumns = switch (type) {
            case SOURCE -> ((SourceTransformSpec)spec).getSchema().numColumns();
            case MAP -> ((MapTransformSpec)spec).getMapperFactory().getOutputSchema().numColumns();
            case ROWINDEX -> predecessors.get(0).numColumns() + 1;
            case COLSELECT -> ((SelectColumnsTransformSpec)spec).getColumnSelection().length;
            case APPEND -> predecessors.stream().mapToInt(TableTransformGraph::numColumns).sum();
            case SLICE, ROWFILTER, CONCATENATE -> predecessors.get(0).numColumns();
            case OBSERVER -> throw SpecGraph.unhandledNodeType();
        };

        final List<AccessId> accessIds = createAccessIds(null, numColumns, i -> "beta^" + i);
        final ArrayList<ControlFlowEdge> controlFlowEdges = new ArrayList<>();
        terminal = new Port(null, accessIds, controlFlowEdges);

        final Node node = (type == COLSELECT) //
                ? null // don't create a new node, just permute predecessor's outCols
                : new Node(spec, numColumns, predecessors);

        // access tracing:
        switch (type) {
            case SOURCE, MAP, APPEND, CONCATENATE -> {
                // link outCols to node's outputs
                unionAccesses(terminal, node.out, numColumns);
            }
            case SLICE, ROWFILTER -> {
                // link outCols to the predecessor's outCols
                // (there is exactly one predecessor)
                unionAccesses(terminal, predecessors.get(0).terminal, numColumns);
            }
            case ROWINDEX -> {
                // link outCols (except the last one) to the predecessor's outCols
                // (there is exactly one predecessor)
                unionAccesses(terminal, predecessors.get(0).terminal, numColumns - 1);
                // link the last outCol to the node's output
                terminal.access(numColumns - 1).union(node.out.access(0));
            }
            case COLSELECT -> {
                // apply selection to predecessor's outCols
                // (there is exactly one predecessor)
                final int[] selection = getColumnSelection(spec);
                unionAccesses(terminal, predecessors.get(0).terminal, numColumns, i -> selection[i]);
            }
        }

        // control flow:
        switch (type) {
            case SOURCE, SLICE, ROWINDEX, APPEND, CONCATENATE -> {
                // link to the new node
                terminal.linkTo(node);
            }
            case MAP, COLSELECT -> {
                // link to everything that was linked to by predecessor
                // (there is exactly one predecessor)
                final List<ControlFlowEdge> predecessorControlFlowEdges =
                        predecessors.get(0).terminal.controlFlowEdges();
                predecessorControlFlowEdges.forEach(e -> e.relinkFrom(terminal));
            }
            case ROWFILTER -> {
                // link to other ROWFILTERs that were linked to by predecessor
                // (there is exactly one predecessor)
                final List<ControlFlowEdge> predecessorControlFlowEdges =
                        predecessors.get(0).terminal.controlFlowEdges();
                if (predecessorControlFlowEdges.get(0).to().owner().type == ROWFILTER) {
                    predecessorControlFlowEdges.forEach(e -> e.relinkFrom(terminal));
                }
                // link to the new node
                terminal.linkTo(node);
            }
        }
    }

    int numColumns() {
        return terminal.accesses().size();
    }

    @Override
    public String toString() {
        return "TableTransformGraph{terminal=" + terminal + "}";
    }

    /**
     * TODO javadoc
     */
    public TableTransformGraph copy() {
        class Copier {
            private final Map<Node, Node> nodes = new HashMap<>();

            private final Map<AccessId, AccessId> accessIds = new HashMap<>();

            private Port copyInPort(final Port port, final Node owner) {
                final Port portCopy = new Port(owner);
                port.accesses().forEach(a -> portCopy.accesses().add(copyOf(a.find())));
                port.controlFlowEdges.forEach(e -> portCopy.linkTo(copyOf(e.to().owner())));
                return portCopy;
            }

            private Node copyOf(final Node node) {
                final Node n = nodes.get(node);
                if (n != null) {
                    return n;
                }
                final Node nodeCopy = new Node((TableTransformSpec)node.getTransformSpec()); // TODO cast necessary because of Node(Port) constructor
                nodes.put(node, nodeCopy);
                node.out.accesses().forEach(a -> nodeCopy.out.accesses().add(copyOf(a.find())));
                node.in.forEach(port -> nodeCopy.in.add(copyInPort(port, nodeCopy)));
                return nodeCopy;
            }

            private AccessId copyOf(final AccessId access) {
                final AccessId a = accessIds.get(access);
                if (a != null) {
                    return a;
                }
                final AccessId.Producer producerCopy = copyOf(access.producer());
                return accessIds.computeIfAbsent(access, ac -> new AccessId(producerCopy, access.label()));
            }

            private AccessId.Producer copyOf(AccessId.Producer producer) {
                return new AccessId.Producer(copyOf(producer.node()), producer.index());
            }
        }
        return new TableTransformGraph(new Copier().copyInPort(terminal, null));
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

    /**
     * Create {@code n} new {@code AccessId}s with the given {@code
     * producerNode} (may be {@code null}). The {@code AccessId}s will be
     * labeled by applying the given {@code label} function to {@code 0, 1, ...,
     * n-1}.
     *
     * @param producerNode
     * @param n
     * @param label
     * @return
     */
    private static List<AccessId> createAccessIds(final Node producerNode, final int n,
            final IntFunction<String> label) {
        final List<AccessId> cols = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            AccessId.Producer producer = producerNode == null ? null : new AccessId.Producer(producerNode, i);
            cols.add(new AccessId(producer, label.apply(i)));
        }
        return cols;
    }

    /**
     * Create an AccessId labeling function to produce labels like
     * "alpha^i_v1(0)" (this would be created for {@code varName="alpha"},
     * {@code nodeId=1}, {@code predecessorIndex=0}).
     */
    private static IntFunction<String> accessLabel(final String varName, final int nodeId, final int predecessorIndex) {
        return i -> {
            String label = varName + "^" + i + "_v" + nodeId;
            if (predecessorIndex >= 0)
                label += "(" + predecessorIndex + ")";
            return label;
        };
    }
}
