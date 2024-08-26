package org.knime.core.table.virtual.graph.rag3;

import static org.knime.core.table.virtual.graph.rag3.SpecGraph.MermaidGraph.EdgeType.CONTROL;
import static org.knime.core.table.virtual.graph.rag3.SpecGraph.MermaidGraph.EdgeType.DATA;
import static org.knime.core.table.virtual.graph.rag3.SpecType.COLSELECT;
import static org.knime.core.table.virtual.graph.rag3.SpecType.ROWFILTER;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.IntUnaryOperator;

import org.knime.core.table.row.Selection;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.virtual.TableTransform;
import org.knime.core.table.virtual.graph.cap.CapAccessId;
import org.knime.core.table.virtual.graph.cap.CapNode;
import org.knime.core.table.virtual.graph.cap.CapNodeAppend;
import org.knime.core.table.virtual.graph.cap.CapNodeConcatenate;
import org.knime.core.table.virtual.graph.cap.CapNodeConsumer;
import org.knime.core.table.virtual.graph.cap.CapNodeMap;
import org.knime.core.table.virtual.graph.cap.CapNodeRowFilter;
import org.knime.core.table.virtual.graph.cap.CapNodeRowIndex;
import org.knime.core.table.virtual.graph.cap.CapNodeSlice;
import org.knime.core.table.virtual.graph.cap.CapNodeSource;
import org.knime.core.table.virtual.graph.cap.CursorAssemblyPlan;
import org.knime.core.table.virtual.graph.rag.ConsumerTransformSpec;
import org.knime.core.table.virtual.graph.rag3.SpecGraph.DependencyGraph.DepNode;
import org.knime.core.table.virtual.graph.rag3.SpecGraph.MermaidGraph.Edge;
import org.knime.core.table.virtual.spec.MapTransformSpec;
import org.knime.core.table.virtual.spec.RowFilterTransformSpec;
import org.knime.core.table.virtual.spec.RowIndexTransformSpec;
import org.knime.core.table.virtual.spec.SelectColumnsTransformSpec;
import org.knime.core.table.virtual.spec.SliceTransformSpec;
import org.knime.core.table.virtual.spec.SourceTableProperties;
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
            return label + (parent == this ? "" : "->" + find());
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

        public <T extends TableTransformSpec> T getTransformSpec() {
            return (T)spec;
        }

        // TODO shorten?
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

            final List<AccessId> accessIds = createAccessIds(null, numColumns, i -> "terminal_" + i);
            final ArrayList<ControlFlowEdge> controlFlowEdges = new ArrayList<>();
            port = new Port(null, accessIds, controlFlowEdges);

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
            return "Terminal{port=" + port + "}";
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



    /**
     * Builds a spec graph from {@code tableTransform}.
     *
     * @param tableTransform the producingTransform of the table
     * @return spec graph representing the given table
     */
    public static Terminal buildSpecGraph(final TableTransform tableTransform) {
        return new Terminal(tableTransform);
    }







    // -----------------------------------------------------------------------------------------------------------------
    //
    // CapBuilder
    //

    static class DependencyGraph {

        interface DepNode {
            Node node();
        }

        record SeqNode(Node node, Set<DepNode> dependencies) implements DepNode {
        }

        record BranchNode(Node node, List<Branch> branches) implements DepNode {
        }

        record Branch(List<SeqNode> innerNodes, BranchNode target) {
        }

        void sequentialize(final Branch branch) {
            branch.target().branches().forEach(this::sequentialize);
            final Set<SeqNode> todo = new HashSet<>(branch.innerNodes);
            branch.innerNodes.clear();
            while(!todo.isEmpty()) {
                var candidates =
                        todo.stream().filter(node -> node.dependencies().stream().noneMatch(todo::contains)).toList();
                var next = policy.apply(candidates);
                branch.innerNodes().add(next);
                todo.remove(next);
            }
        }

        static final Function<List<SeqNode>, SeqNode> policy = nodes -> nodes.get(0);

        private final Map<Node, DepNode> depNodes = new HashMap<>();

        private final Terminal terminal;

        private final Branch rootBranch;

        public DependencyGraph(final Terminal terminal) {
            this.terminal = terminal;
            rootBranch = getBranch(terminal.port);
            sequentialize(rootBranch);
        }

        private DepNode getDepNode(Node node, List<SeqNode> innerNodes, AtomicReference<BranchNode> branchTarget)
        {
            final DepNode depNode = depNodes.get(node);
            if (depNode != null) {
                return depNode;
            }
            switch (node.type) {
                case SOURCE, APPEND, CONCATENATE -> {
                    final ArrayList<Branch> branches = new ArrayList<>();
                    node.in.forEach(port -> branches.add(getBranch(port)));
                    final BranchNode branchNode = new BranchNode(node, branches);
                    branchTarget.setPlain(branchNode);
                    depNodes.put(node, branchNode);
                    return branchNode;
                }
                case SLICE, MAP, ROWFILTER, ROWINDEX -> {
                    final Port port = node.in.get(0);
                    final Set<DepNode> dependencies = getDependencies(port, innerNodes, branchTarget);
                    final SeqNode seqNode = new SeqNode(node, dependencies);
                    innerNodes.add(seqNode);
                    depNodes.put(node, seqNode);
                    return seqNode;
                }
                case OBSERVER -> throw unhandledNodeType();
                default -> throw new IllegalArgumentException();
            }
        }

        private Branch getBranch(final Port port) {
            final List<SeqNode> inner = new ArrayList<>();
            final AtomicReference<BranchNode> target = new AtomicReference<>();
            getDependencies(port, inner, target);
            return new Branch(inner, target.get());
        }

        private Set<DepNode> getDependencies(final Port port, final List<SeqNode> innerNodes,
                final AtomicReference<BranchNode> branchTarget) {
            final Set<DepNode> dependencies = new HashSet<>();
            port.controlFlowEdges().forEach(e -> {
                final Node target = e.to().owner();
                final DepNode depTarget = getDepNode(target, innerNodes, branchTarget);
                dependencies.add(depTarget);
            });
            port.accesses().forEach(a -> {
                final Node target = a.find().producer().node();
                final DepNode depTarget = getDepNode(target, innerNodes, branchTarget);
                dependencies.add(depTarget);
            });
            return dependencies;
        }
    }


    static class BuildCap {

        private final List<CapNode> cap;
        private final Map<Node, CapNode> capNodes;
        private final Map<AccessId, CapAccessId> capAccessIds;
        private final Map<UUID, ColumnarSchema> sourceSchemas;

        BuildCap(final DependencyGraph sequentializedGraph) {
            cap = new ArrayList<>();
            capNodes = new HashMap<>();
            capAccessIds = new HashMap<>();
            sourceSchemas = new HashMap<>();

            DependencyGraph.Branch branch = sequentializedGraph.rootBranch;

            final CapNode capNode = appendBranch(branch);
            final CapAccessId[] inputs = capAccessIdsFor(sequentializedGraph.terminal.port.accesses());
            cap.add(new CapNodeConsumer(index++, inputs, capNode.index()));
        }

        public static CursorAssemblyPlan getCursorAssemblyPlan(final DependencyGraph sequentializedGraph) {
            final BuildCap builder = new BuildCap(sequentializedGraph);
            // TODO: compute numRows and cursorType ... TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
            return new CursorAssemblyPlan(builder.cap, SourceTableProperties.CursorType.BASIC, -1,
                    builder.sourceSchemas);
        }

        private int index = 0;
        private CapNode appendBranch(final DependencyGraph.Branch branch) {
            // append predecessor branches
            final List<CapNode> heads = new ArrayList<>();
            branch.target().branches().forEach(b -> heads.add(appendBranch(b)));

            // current head while building this branch
            CapNode capNode = null;

            // append branch target
            final Node target = branch.target().node();
            switch (target.type) {
                case SOURCE -> {
                    final SourceTransformSpec spec = target.getTransformSpec();
                    final UUID uuid = spec.getSourceIdentifier();
                    final List<AccessId> outputs = target.out.accesses();
                    final int[] columns = outputs.stream().mapToInt(a -> a.find().producer().index()).toArray();
                    final Selection.RowRangeSelection range = spec.getRowRange();
                    capNode = new CapNodeSource(index++, uuid, columns, range);
                    createCapAccessIdsFor(outputs, capNode);
                    append(target, capNode);
                    sourceSchemas.put(uuid, spec.getSchema());
                }
                case APPEND -> {
                    final int numPredecessors = heads.size();
                    final int[] predecessors = new int[numPredecessors];
                    final int[][] predecessorOutputIndices = new int[numPredecessors][];
                    final long[] predecessorSizes = new long[numPredecessors];
                    final List<AccessId> inputs = new ArrayList<>();
                    for ( int i = 0; i < numPredecessors; ++i ) {
                        predecessors[i] = heads.get(i).index();
                        predecessorSizes[i] = -1; // TODO implement numRows... TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
                        final List<AccessId> branchInputs = target.in.get(i).accesses();
                        predecessorOutputIndices[i] = new int[branchInputs.size()];
                        Arrays.setAll(predecessorOutputIndices[i], j -> j + inputs.size());
                        inputs.addAll(branchInputs);
                    }
                    final CapAccessId[] capInputs = capAccessIdsFor(inputs);
                    capNode = new CapNodeAppend(index++, capInputs, predecessors, predecessorOutputIndices, predecessorSizes);
                    createCapAccessIdsFor(target.out.accesses(), capNode);
                    append(target, capNode);
                }
                case CONCATENATE -> {
                    final int numPredecessors = heads.size();
                    final int[] predecessors = new int[numPredecessors];
                    final CapAccessId[][] capInputs = new CapAccessId[numPredecessors][];
                    final long[] predecessorSizes = new long[numPredecessors];
                    for ( int i = 0; i < numPredecessors; ++i ) {
                        predecessors[i] = heads.get(i).index();
                        capInputs[i] = capAccessIdsFor(target.in.get(i).accesses());
                        predecessorSizes[i] = -1; // TODO implement numRows... TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
                    }
                    capNode = new CapNodeConcatenate(index++, capInputs, predecessors, predecessorSizes);
                    createCapAccessIdsFor(target.out.accesses(), capNode);
                    append(target, capNode);
                }
                default -> throw new IllegalStateException();
            }

            // append inner nodes
            for (DepNode depNode : branch.innerNodes) {
                final Node node = depNode.node();
                final List<AccessId> inputs = node.in.get(0).accesses();
                final CapAccessId[] capInputs = capAccessIdsFor(inputs);

                final int predecessor = capNode.index();
                switch (node.type) {
                    case MAP -> {
                        final MapTransformSpec spec = node.getTransformSpec();
                        final List<AccessId> outputs = node.out.accesses();
                        final int[] columns = outputs.stream().mapToInt(a -> a.find().producer().index()).toArray();
                        capNode = new CapNodeMap(index++, capInputs, predecessor, columns, spec.getMapperFactory());
                        createCapAccessIdsFor(outputs, capNode);
                        append(node, capNode);
                    }
                    case SLICE -> {
                        final SliceTransformSpec spec = node.getTransformSpec();
                        final long from = spec.getRowRangeSelection().fromIndex();
                        final long to = spec.getRowRangeSelection().toIndex();
                        capNode = new CapNodeSlice(index++, predecessor, from, to);
                        append(node, capNode);
                    }
                    case ROWFILTER -> {
                        final RowFilterTransformSpec spec = node.getTransformSpec();
                        capNode = new CapNodeRowFilter(index++, capInputs, predecessor, spec.getFilterFactory());
                        append(node, capNode);
                    }
                    case ROWINDEX -> {
                        final RowIndexTransformSpec spec = node.getTransformSpec();
                        final List<AccessId> outputs = node.out.accesses();
                        capNode = new CapNodeRowIndex(index++, predecessor, spec.getOffset());
                        createCapAccessIdsFor(outputs, capNode);
                        append(node, capNode);
                    }
                    case OBSERVER -> throw unhandledNodeType();
                    default -> throw new IllegalStateException();
                }
            }

            return capNode;
        }

        /**
         * Append the given {@code capNode} to the plan.
         * Remember the association to corresponding {@code ragNode}.
         */
        private void append(final Node ragNode, final CapNode capNode) {
            cap.add(capNode);
            capNodes.put(ragNode, capNode);
        }

        /**
         * Create a new CapAccessId with the given producer and slot indices starting from 0 for the given {@code outputs} AccessIds.
         */
        private void createCapAccessIdsFor(final Iterable<AccessId> outputs, final CapNode producer) {
            int i = 0;
            for (AccessId output : outputs) {
                capAccessIds.put(output.find(), new CapAccessId(producer, i++));
            }
        }

        /**
         * Get CapAccessId[] array corresponding to (Rag)AccessIds collection.
         */
        private CapAccessId[] capAccessIdsFor(final Collection<AccessId> ids) {
            final CapAccessId[] imps = new CapAccessId[ids.size()];
            int i = 0;
            for (AccessId id : ids) {
                imps[i++] = capAccessIds.get(id.find());
            }
            return imps;
        }


    }

    //
    // CapBuilder
    //
    // -----------------------------------------------------------------------------------------------------------------


















    // -----------------------------------------------------------------------------------------------------------------
    //
    // Mermaid
    //

    static class MermaidGraph {
        enum EdgeType {DATA, CONTROL}
        record Edge(EdgeType type, Node from, Node to) {}

        private final Node consumer;
        private final Set<Node> nodes = new HashSet<>();
        private final Set<Edge> edges = new HashSet<>();

        MermaidGraph(Terminal terminal)
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
                        edges.add(new Edge(CONTROL, node, target));
                    });
                    port.accesses().forEach(a -> {
                        final Node target = a.find().producer().node();
                        addRecursively(target);
                        edges.add(new Edge(DATA, node, target));
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

    public static String mermaid(final MermaidGraph graph, final boolean darkMode) {
        final var sb = new StringBuilder("graph BT\n");
        for (final Node node : graph.nodes) {
            final String name = "<" + node.id() + "> " + node.spec;
            sb.append("  " + node.id() + "(\"" + name + "\")\n");
        }
        int edgeId = 0;
        for (final Edge edge : graph.edges) {
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

    //
    // Mermaid
    //
    // -----------------------------------------------------------------------------------------------------------------
}
