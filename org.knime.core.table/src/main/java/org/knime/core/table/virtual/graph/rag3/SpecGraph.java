package org.knime.core.table.virtual.graph.rag3;

import static org.knime.core.table.virtual.graph.rag3.SpecGraph.MermaidGraph.EdgeType.CONTROL;
import static org.knime.core.table.virtual.graph.rag3.SpecGraph.MermaidGraph.EdgeType.DATA;
import static org.knime.core.table.virtual.graph.rag3.SpecType.COLSELECT;
import static org.knime.core.table.virtual.graph.rag3.SpecType.ROWFILTER;
import static org.knime.core.table.virtual.spec.SourceTableProperties.CursorType.BASIC;
import static org.knime.core.table.virtual.spec.SourceTableProperties.CursorType.LOOKAHEAD;
import static org.knime.core.table.virtual.spec.SourceTableProperties.CursorType.RANDOMACCESS;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.IntUnaryOperator;
import java.util.function.LongBinaryOperator;

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
import org.knime.core.table.virtual.graph.rag3.AccessId.Producer;
import org.knime.core.table.virtual.graph.rag3.SpecGraph.DependencyGraph.DepNode;
import org.knime.core.table.virtual.graph.rag3.SpecGraph.MermaidGraph.Edge;
import org.knime.core.table.virtual.spec.MapTransformSpec;
import org.knime.core.table.virtual.spec.RowFilterTransformSpec;
import org.knime.core.table.virtual.spec.RowIndexTransformSpec;
import org.knime.core.table.virtual.spec.SelectColumnsTransformSpec;
import org.knime.core.table.virtual.spec.SliceTransformSpec;
import org.knime.core.table.virtual.spec.SourceTableProperties.CursorType;
import org.knime.core.table.virtual.spec.SourceTransformSpec;
import org.knime.core.table.virtual.spec.TableTransformSpec;

public class SpecGraph {

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
    private static List<AccessId> createAccessIds(final Node producerNode, final int n, final IntFunction<String> label) {
        final List<AccessId> cols = new ArrayList<>( n );
        for (int i = 0; i < n; i++) {
            Producer producer = producerNode == null ? null : new Producer(producerNode, i);
            cols.add(new AccessId(producer, label.apply(i)));
        }
        return cols;
    }

    /**
     * Create an AccessId labeling function
     * TODO: javadoc
     */
    private static IntFunction<String> accessLabel(final String varName, final int nodeId, final int predecessorIndex) {
        return i -> {
            String label = varName + "^" + i + "_v" + nodeId;
            if (predecessorIndex >= 0)
                label += "(" + predecessorIndex + ")";
            return label;
        };
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

        Port(Node owner) {
            this(owner, new ArrayList<>());
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

        Node(final TableTransformSpec spec, final SpecType type) {
            this.spec = spec;
            this.type = type;
            out = new Port(this);
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

        public SpecType type() {
            return type;
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

    static class Terminal {
        final Port port;

        Terminal(final Port port) {
            this.port = port;
        }

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
                    outCol(numColumns - 1).union(node.out.access(0));
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




    static Terminal copy(final Terminal terminal) {
        return new Terminal(new TerminalCopier().copyInPort(terminal.port, null));
    }

    private static class TerminalCopier {
        private final Map<Node, Node> nodes = new HashMap<>();
        private final Map<AccessId, AccessId> accessIds = new HashMap<>();

        private Port copyInPort(final Port port, final Node owner) {
//            System.out.println("TerminalCopier.copyInPort");
//            System.out.println("  port = " + port + ", owner = " + owner);
            final Port portCopy = new Port(owner);
            port.accesses().forEach(a -> portCopy.accesses().add(copyOf(a.find())));
            port.controlFlowEdges.forEach(e -> portCopy.linkTo(copyOf(e.to().owner()).out));
            return portCopy;
        }

        private Node copyOf(final Node node) {
//            System.out.println("TerminalCopier.copyOf");
//            System.out.println("  node = " + node);
            final Node n = nodes.get(node);
            if (n != null) {
                return n;
            }
            final Node nodeCopy = new Node(node.getTransformSpec(), node.type());
            nodes.put(node, nodeCopy);
            node.out.accesses().forEach(a -> nodeCopy.out.accesses().add(copyOf(a.find())));
            node.in.forEach(port -> nodeCopy.in.add(copyInPort(port, nodeCopy)));
            return nodeCopy;
        }

        private AccessId copyOf(final AccessId access) {
//            System.out.println("TerminalCopier.copyOf");
//            System.out.println("  access = " + access);
            final AccessId a = accessIds.get(access);
            if (a != null) {
                return a;
            }
            final Producer producerCopy = copyOf(access.producer());
            return accessIds.computeIfAbsent(access, ac -> new AccessId(producerCopy, access.label()));
        }

        private Producer copyOf(Producer producer) {
//            System.out.println("TerminalCopier.copyOf");
//            System.out.println("  producer = " + producer);
            return new Producer(copyOf(producer.node()), producer.index());
        }
    }






    // -----------------------------------------------------------------------------------------------------------------
    //
    // RagGraphProperties
    //

    // TODO: make member of Terminal?
    static long numRows(final Terminal terminal)
    {
        final Node node = terminal.port.controlFlowEdges().get(0).to().owner();
        return numRows(node);
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
        // TODO accessor in()
        for (Port port : node.in) {
            // TODO: shortcut candidate:
            final Node predecessor = port.controlFlowEdges().get(0).to().owner();
            final long s = numRows(predecessor);
            if ( s < 0 ) {
                return -1;
            }
            size = acc.applyAsLong(size, s);
        }
        return size;
    }




    /**
     * Returns the {@link CursorType} supported by the given linearized {@code
     * RagGraph} (without additional prefetching and buffering).
     * The result is determined by the {@code CursorType} of the sources, and
     * the presence of ROWFILTER operations, etc.
     *
     * @return cursor supported at consumer node of the {@code orderedRag}
     */
    // TODO: make member of Terminal?
    private static CursorType supportedCursorType(final Terminal terminal) {
        final Node node = terminal.port.controlFlowEdges().get(0).to().owner();
        return supportedCursorType(node);
    }

    private static CursorType supportedCursorType(final Node node) {
        return switch (node.type()) {
            case SOURCE -> node.<SourceTransformSpec>getTransformSpec().getProperties().cursorType();
            case ROWFILTER -> BASIC;
            case SLICE, APPEND, ROWINDEX, OBSERVER -> {
                var cursorType = RANDOMACCESS;
                for (Port port : node.in) {
                    // TODO: shortcut candidate:
                    final Node predecessor = port.controlFlowEdges().get(0).to().owner();
                    cursorType = min(cursorType, supportedCursorType(predecessor));
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
                for (int i = 0; i < node.in.size(); i++) {
                    Port port = node.in.get(i);
                    // TODO: shortcut candidate:
                    final Node predecessor = port.controlFlowEdges().get(0).to().owner();
                    cursorType = min(cursorType, supportedCursorType(predecessor));
                    if (i != node.in.size() - 1 && numRows(predecessor) < 0) {
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

    private static CursorType min(CursorType arg0, CursorType arg1) {
        return switch (arg0) {
            case BASIC -> BASIC;
            case LOOKAHEAD -> arg1.supportsLookahead() ? LOOKAHEAD : BASIC;
            case RANDOMACCESS -> arg1;
        };
    }


    //
    // RagGraphProperties
    //
    // -----------------------------------------------------------------------------------------------------------------







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
            final CursorType cursorType = supportedCursorType(sequentializedGraph.terminal);
            System.out.println("cursorType = " + cursorType);
            final long numRows = numRows(sequentializedGraph.terminal);
            System.out.println("numRows = " + numRows);
            return new CursorAssemblyPlan(builder.cap, cursorType, numRows, builder.sourceSchemas);
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

        record Edge(EdgeType type, Node from, Node to) {
            @Override
            public String toString() {
                return "(" + from.id() + " -> " + to.id() + ", " + type + ")";
            }
        }

        private final Set<Node> nodes = new LinkedHashSet<>();
        private final Set<Edge> edges = new LinkedHashSet<>();

        MermaidGraph(Terminal terminal)
        {
            addRecursively(new Node(terminal.port));
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
            final StringBuilder sb = new StringBuilder("MermaidGraph{");
            sb.append("\n  nodes=");
            nodes.forEach(node -> sb.append("\n    ").append(node));
            sb.append("\n  edges=");
            edges.forEach(edge -> sb.append("\n    ").append(edge));
            sb.append("\n}");
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
