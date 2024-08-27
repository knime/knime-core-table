package org.knime.core.table.virtual.graph.rag3;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

class BranchGraph {

    public sealed interface AbstractNode {
        TableTransformGraph.Node node();
    }

    public record InnerNode(TableTransformGraph.Node node, Set<AbstractNode> dependencies) implements AbstractNode {
    }

    public record BranchNode(TableTransformGraph.Node node, List<Branch> branches) implements AbstractNode {
    }

    public record Branch(List<InnerNode> innerNodes, BranchNode target) {
    }

    static final Function<List<InnerNode>, InnerNode> policy = nodes -> nodes.get(0);

    final TableTransformGraph tableTransformGraph;

    final Branch rootBranch;

    private final Map<TableTransformGraph.Node, AbstractNode> depNodes = new HashMap<>();

    public BranchGraph(final TableTransformGraph tableTransformGraph) {
        this.tableTransformGraph = tableTransformGraph;
        rootBranch = getBranch(tableTransformGraph.terminal());
        sequentialize(rootBranch);
    }

    private void sequentialize(final Branch branch) {
        branch.target().branches().forEach(this::sequentialize);
        final Set<InnerNode> todo = new HashSet<>(branch.innerNodes);
        branch.innerNodes.clear();
        while (!todo.isEmpty()) {
            var candidates =
                    todo.stream().filter(node -> node.dependencies().stream().noneMatch(todo::contains)).toList();
            var next = policy.apply(candidates);
            branch.innerNodes().add(next);
            todo.remove(next);
        }
    }

    private AbstractNode getDepNode(TableTransformGraph.Node node, List<InnerNode> innerNodes,
            AtomicReference<BranchNode> branchTarget) {
        final AbstractNode depNode = depNodes.get(node);
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
                final TableTransformGraph.Port port = node.in(0);
                final Set<AbstractNode> dependencies = getDependencies(port, innerNodes, branchTarget);
                final InnerNode innerNode = new InnerNode(node, dependencies);
                innerNodes.add(innerNode);
                depNodes.put(node, innerNode);
                return innerNode;
            }
            case OBSERVER -> throw SpecGraph.unhandledNodeType();
            default -> throw new IllegalArgumentException();
        }
    }

    private Branch getBranch(final TableTransformGraph.Port port) {
        final List<InnerNode> inner = new ArrayList<>();
        final AtomicReference<BranchNode> target = new AtomicReference<>();
        getDependencies(port, inner, target);
        return new Branch(inner, target.get());
    }

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
}
