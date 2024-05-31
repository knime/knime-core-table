package org.knime.core.table.virtual.graph.rag;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.knime.core.table.virtual.graph.rag.debug.DependencyGraph;

/**
 * Sequentialize a {@link TableTransformGraph} into a tree:
 * <ul>
 * <li>{@link BranchNode Nodes} of the tree are SOURCE, CONCATENATE, and APPEND operations (the only nodes where execution splits into branches).</li>
 * <li>{@link BranchEdge Edges} between tree nodes contain all other operations (MAP, ROWFILTER, ROWINDEX, SLICE, OBSERVER).</li>
 * </ul>
 */
public class BranchGraph {

    public static final Comparator<InnerNode> DEFAULT_POLICY = //
            Comparator.comparingInt(node -> switch (node.type()) {
                case MAP -> 1;
                default -> 0;
            });

    private final Comparator<InnerNode> policy = DEFAULT_POLICY;

    public sealed interface AbstractNode {
        TableTransformGraph.Node node();

        default SpecType type() {
            return node().type();
        }
    }

    public record InnerNode(TableTransformGraph.Node node, Set<AbstractNode> dependencies) implements AbstractNode {
    }

    public record BranchNode(TableTransformGraph.Node node, List<BranchEdge> branches) implements AbstractNode {
    }

    // if sequentialized: execute target first, then innerNodes in order
    public record BranchEdge(List<InnerNode> innerNodes, BranchNode target) {
    }

    public final TableTransformGraph tableTransformGraph;

    public final BranchEdge rootBranch;

    private final Map<TableTransformGraph.Node, AbstractNode> depNodes = new HashMap<>();

    public BranchGraph(final TableTransformGraph tableTransformGraph) {
        this.tableTransformGraph = tableTransformGraph;
        rootBranch = getBranch(tableTransformGraph.terminal());
        sequentialize(rootBranch);
    }

    private BranchEdge getBranch(final TableTransformGraph.Port port) {
        final List<InnerNode> inner = new ArrayList<>();
        final AtomicReference<BranchNode> target = new AtomicReference<>();
        getDependencies(port, inner, target);
        return new BranchEdge(inner, target.getPlain());
    }

    /**
     *
     * @param port node in-port to trace dependencies of
     * @param innerNodes all {@link AbstractNode} encountered while walking to the next {@link BranchNode}
     * @param branchTarget the next {@link BranchNode}
     * @return set of direct dependencies of port, respectively the node with port==node.in(i)
     */
    private Set<AbstractNode> getDependencies(final TableTransformGraph.Port port, final List<InnerNode> innerNodes,
            final AtomicReference<BranchNode> branchTarget) {
        final Set<AbstractNode> dependencies = new HashSet<>();
        port.controlFlowEdges().forEach(e -> {
            final TableTransformGraph.Node target = e.to().owner();
            final AbstractNode depTarget = getDepNode(target, innerNodes, branchTarget);
            dependencies.add(depTarget);
        });
        port.accesses().forEach(a -> {
            final TableTransformGraph.Node target = a.find().producer().node();
            final AbstractNode depTarget = getDepNode(target, innerNodes, branchTarget);
            dependencies.add(depTarget);
        });
        return dependencies;
    }

    /**
     * Create or retrieve the {@link AbstractNode} corresponding to {@code node}.
     * <p>
     * This creates or retrieves all dependencies of {@code node}, recursively.
     * <p>
     * If {@code node} is SOURCE, APPEND, or CONCATENATE the retrieved {@code AbstractNode} is set as {@code branchTarget}.
     * <p>
     * If {@code node} is SLICE, MAP, ROWFILTER, ROWINDEX, or OBSERVER the retrieved {@code AbstractNode} is added to {@code innerNodes}.
     *
     * @param node the node for which to create or retrieve the corresponding {@code AbstractNode}
     * @param innerNodes all {@code InnerNode} encountered while walking to the next {@code BranchNode}
     * @param branchTarget the next {@code BranchNode}
     * @return {@link AbstractNode} wrapping the given node
     */
    private AbstractNode getDepNode(TableTransformGraph.Node node, List<InnerNode> innerNodes,
            AtomicReference<BranchNode> branchTarget) {
        final AbstractNode depNode = depNodes.get(node);
        if (depNode != null) {
            return depNode;
        }
        switch (node.type()) {
            case SOURCE, APPEND, CONCATENATE -> {
                final ArrayList<BranchEdge> branches = new ArrayList<>();
                node.in().forEach(port -> branches.add(getBranch(port)));
                final BranchNode branchNode = new BranchNode(node, branches);
                branchTarget.setPlain(branchNode);
                depNodes.put(node, branchNode);
                return branchNode;
            }
            case SLICE, MAP, ROWFILTER, ROWINDEX, OBSERVER -> {
                final TableTransformGraph.Port port = node.in(0);
                final Set<AbstractNode> dependencies = getDependencies(port, innerNodes, branchTarget);
                final InnerNode innerNode = new InnerNode(node, dependencies);
                innerNodes.add(innerNode);
                depNodes.put(node, innerNode);
                return innerNode;
            }
            default -> throw new IllegalArgumentException();
        }
    }

    private void sequentialize(final BranchEdge branch) {
        branch.target().branches().forEach(this::sequentialize);
        final Set<InnerNode> todo = new HashSet<>(branch.innerNodes);
        branch.innerNodes.clear();
        while (!todo.isEmpty()) {
            var next = todo.stream() //
                    .filter(node -> node.dependencies().stream() //
                            .noneMatch(todo::contains)) //
                    .sorted(policy) //
                    .findFirst().get();
            branch.innerNodes().add(next);
            todo.remove(next);
        }
    }

    @Override
    public String toString() {
        return "BranchGraph" + DependencyGraph.prettyPrint(this);
    }
}
