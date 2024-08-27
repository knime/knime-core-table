package org.knime.core.table.virtual.graph.rag3;

import static org.knime.core.table.virtual.graph.rag3.SpecGraph.MermaidGraph.EdgeType.CONTROL;
import static org.knime.core.table.virtual.graph.rag3.SpecGraph.MermaidGraph.EdgeType.DATA;
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
import java.util.function.LongBinaryOperator;

import org.knime.core.table.row.Selection;
import org.knime.core.table.schema.ColumnarSchema;
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
import org.knime.core.table.virtual.graph.rag3.SpecGraph.DependencyGraph.DepNode;
import org.knime.core.table.virtual.graph.rag3.SpecGraph.MermaidGraph.Edge;
import org.knime.core.table.virtual.graph.rag3.TableTransformGraph.Node;
import org.knime.core.table.virtual.graph.rag3.TableTransformGraph.Port;
import org.knime.core.table.virtual.spec.MapTransformSpec;
import org.knime.core.table.virtual.spec.RowFilterTransformSpec;
import org.knime.core.table.virtual.spec.RowIndexTransformSpec;
import org.knime.core.table.virtual.spec.SliceTransformSpec;
import org.knime.core.table.virtual.spec.SourceTableProperties.CursorType;
import org.knime.core.table.virtual.spec.SourceTransformSpec;

public class SpecGraph {

    static UnsupportedOperationException unhandledNodeType() { // TODO: handle or remove OBSERVER case
        return new UnsupportedOperationException("not handled yet. needs to be implemented or removed");
    }

    // -----------------------------------------------------------------------------------------------------------------
    //
    // RagGraphProperties
    //

    // TODO: make member of TableTransformGraph?
    static long numRows(final TableTransformGraph graph)
    {
        return numRows(graph.terminal);
    }

    private static long numRows(final Port port) {
        return numRows(port.controlFlowEdges().get(0).to().owner());
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
    // TODO: make member of TableTransformGraph?
    private static CursorType supportedCursorType(final TableTransformGraph tableTransformGraph) {
        final Node node = tableTransformGraph.terminal.controlFlowEdges().get(0).to().owner();
        return supportedCursorType(node);
    }

    private static CursorType supportedCursorType(final Node node) {
        return switch (node.type()) {
            case SOURCE -> node.<SourceTransformSpec>getTransformSpec().getProperties().cursorType();
            case ROWFILTER -> BASIC;
            case SLICE, APPEND, ROWINDEX, OBSERVER -> {
                var cursorType = RANDOMACCESS;
                for (Port port : node.in()) {
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
                for (int i = 0; i < node.in().size(); i++) {
                    Port port = node.in().get(i);
                    // TODO: shortcut candidate:
                    final Node predecessor = port.controlFlowEdges().get(0).to().owner();
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

        private final TableTransformGraph tableTransformGraph;

        private final Branch rootBranch;

        public DependencyGraph(final TableTransformGraph tableTransformGraph) {
            this.tableTransformGraph = tableTransformGraph;
            rootBranch = getBranch(tableTransformGraph.terminal);
            sequentialize(rootBranch);
        }

        private DepNode getDepNode(Node node, List<SeqNode> innerNodes, AtomicReference<BranchNode> branchTarget)
        {
            final DepNode depNode = depNodes.get(node);
            if (depNode != null) {
                return depNode;
            }
            switch (node.type()) {
                case SOURCE, APPEND, CONCATENATE -> {
                    final ArrayList<Branch> branches = new ArrayList<>();
                    node.in().forEach(port -> branches.add(getBranch(port)));
                    final BranchNode branchNode = new BranchNode(node, branches);
                    branchTarget.setPlain(branchNode);
                    depNodes.put(node, branchNode);
                    return branchNode;
                }
                case SLICE, MAP, ROWFILTER, ROWINDEX -> {
                    final Port port = node.in().get(0);
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
            final CapAccessId[] inputs = capAccessIdsFor(sequentializedGraph.tableTransformGraph.terminal.accesses());
            cap.add(new CapNodeConsumer(index++, inputs, capNode.index()));
        }

        public static CursorAssemblyPlan getCursorAssemblyPlan(final DependencyGraph sequentializedGraph) {
            final BuildCap builder = new BuildCap(sequentializedGraph);
            final CursorType cursorType = supportedCursorType(sequentializedGraph.tableTransformGraph);
            System.out.println("cursorType = " + cursorType);
            final long numRows = numRows(sequentializedGraph.tableTransformGraph);
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
            switch (target.type()) {
                case SOURCE -> {
                    final SourceTransformSpec spec = target.getTransformSpec();
                    final UUID uuid = spec.getSourceIdentifier();
                    final List<AccessId> outputs = target.out().accesses();
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
                        predecessorSizes[i] = numRows(target.in().get(i));
                        final List<AccessId> branchInputs = target.in().get(i).accesses();
                        predecessorOutputIndices[i] = new int[branchInputs.size()];
                        Arrays.setAll(predecessorOutputIndices[i], j -> j + inputs.size());
                        inputs.addAll(branchInputs);
                    }
                    final CapAccessId[] capInputs = capAccessIdsFor(inputs);
                    capNode = new CapNodeAppend(index++, capInputs, predecessors, predecessorOutputIndices, predecessorSizes);
                    createCapAccessIdsFor(target.out().accesses(), capNode);
                    append(target, capNode);
                }
                case CONCATENATE -> {
                    final int numPredecessors = heads.size();
                    final int[] predecessors = new int[numPredecessors];
                    final CapAccessId[][] capInputs = new CapAccessId[numPredecessors][];
                    final long[] predecessorSizes = new long[numPredecessors];
                    for ( int i = 0; i < numPredecessors; ++i ) {
                        predecessors[i] = heads.get(i).index();
                        capInputs[i] = capAccessIdsFor(target.in().get(i).accesses());
                        predecessorSizes[i] = numRows(target.in().get(i));
                    }
                    capNode = new CapNodeConcatenate(index++, capInputs, predecessors, predecessorSizes);
                    createCapAccessIdsFor(target.out().accesses(), capNode);
                    append(target, capNode);
                }
                default -> throw new IllegalStateException();
            }

            // append inner nodes
            for (DepNode depNode : branch.innerNodes) {
                final Node node = depNode.node();
                final List<AccessId> inputs = node.in().get(0).accesses();
                final CapAccessId[] capInputs = capAccessIdsFor(inputs);

                final int predecessor = capNode.index();
                switch (node.type()) {
                    case MAP -> {
                        final MapTransformSpec spec = node.getTransformSpec();
                        final List<AccessId> outputs = node.out().accesses();
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
                        final List<AccessId> outputs = node.out().accesses();
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

        MermaidGraph(TableTransformGraph tableTransformGraph)
        {
            addRecursively(new Node(tableTransformGraph.terminal));
        }

        void addRecursively(Node node) {
            if (nodes.add(node)) {
                node.in().forEach(port -> {
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
            final String name = "<" + node.id() + "> " + node.getTransformSpec();
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
