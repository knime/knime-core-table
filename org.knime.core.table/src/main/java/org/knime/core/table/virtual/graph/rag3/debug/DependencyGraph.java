package org.knime.core.table.virtual.graph.rag3.debug;

import static org.knime.core.table.virtual.graph.rag3.debug.DependencyGraph.EdgeType.CONTROL;
import static org.knime.core.table.virtual.graph.rag3.debug.DependencyGraph.EdgeType.DATA;
import static org.knime.core.table.virtual.graph.rag3.debug.DependencyGraph.EdgeType.EXECUTION;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.knime.core.table.virtual.graph.rag.ConsumerTransformSpec;
import org.knime.core.table.virtual.graph.rag3.BranchGraph;
import org.knime.core.table.virtual.graph.rag3.TableTransformGraph;
import org.knime.core.table.virtual.graph.rag3.TableTransformGraph.Port;
import org.knime.core.table.virtual.spec.TableTransformSpec;

/**
 * A graph that explicitly represents execution ordering constraints between the
 * {@link Node}s in a {@link TableTransformGraph}.
 * <p>
 * Used for debugging, i.e., pretty-printing and writing {@link Mermaid}.
 */
public class DependencyGraph {

    enum EdgeType {DATA, CONTROL, EXECUTION}

    record Edge(EdgeType type, Node from, Node to) {
        @Override
        public String toString() {
            return "(" + from.id() + " -> " + to.id() + ", " + type + ")";
        }
    }

    record Node(int id, TableTransformSpec spec) {
        Node(TableTransformGraph.Node node) {
            this(node.id(), node.getTransformSpec());
        }

        @Override
        public String toString() {
            return "(<" + id + ">, " + spec + ")";
        }
    }

    private final Map<TableTransformGraph.Node, Node> nodeMap = new HashMap<>();

    final Set<Node> nodes = new LinkedHashSet<>();

    final Set<Edge> edges = new LinkedHashSet<>();

    public DependencyGraph(final TableTransformGraph tableTransformGraph) {
        final Node consumer = new Node(0, new ConsumerTransformSpec());
        nodes.add(consumer);
        addRecursively(consumer, tableTransformGraph.terminal());
    }

    private void addRecursively(final Node fromNode, final Port port) {
        port.controlFlowEdges().forEach(e -> {
            final TableTransformGraph.Node target = e.to().owner();
            Node toNode = addRecursively(target );
            edges.add(new Edge(CONTROL, fromNode, toNode));
        });
        port.accesses().forEach(a -> {
            final TableTransformGraph.Node target = a.find().producer().node();
            Node toNode = addRecursively(target );
            edges.add(new Edge(DATA, fromNode, toNode));
        });
    }

    private Node addRecursively(final TableTransformGraph.Node ttgNode) {
        final Node existing = nodeMap.get(ttgNode);
        if (existing != null) {
            return existing;
        }

        final Node node = new Node(ttgNode);
        nodeMap.put(ttgNode, node);
        nodes.add(node);
        ttgNode.in().forEach(port -> addRecursively(node, port));
        return node;
    }

    @Override
    public String toString() {
        return "DependencyGraph" + prettyPrint();
    }

    private String prettyPrint() {
        final StringBuilder sb = new StringBuilder("{");
        sb.append("\n  nodes=");
        nodes.forEach(node -> sb.append("\n    ").append(node));
        sb.append("\n  edges=");
        edges.forEach(edge -> sb.append("\n    ").append(edge));
        sb.append("\n}");
        return sb.toString();
    }

    public static String prettyPrint(final TableTransformGraph tableTransformGraph) {
        return new DependencyGraph(tableTransformGraph).prettyPrint();
    }





    public DependencyGraph(final BranchGraph branchGraph) {
        final Node consumer = new Node(0, new ConsumerTransformSpec());
        nodes.add(consumer);
        addRecursively(consumer, branchGraph.rootBranch);
    }

    private void addRecursively(Node fromNode, final BranchGraph.BranchEdge branch) {
        final List<BranchGraph.InnerNode> innerNodes = branch.innerNodes();
        for (int i = innerNodes.size() - 1; i >= 0; i--) {
            BranchGraph.InnerNode innerNode = innerNodes.get(i);
            final Node toNode = new Node(innerNode.node());
            nodes.add(toNode);
            edges.add(new Edge(EXECUTION, fromNode, toNode));
            fromNode = toNode;
        }
        final Node toNode = new Node(branch.target().node());
        nodes.add(toNode);
        edges.add(new Edge(EXECUTION, fromNode, toNode));
        branch.target().branches().forEach(b -> addRecursively(toNode, b));
    }

    public static String prettyPrint(final BranchGraph branchGraph) {
        return new DependencyGraph(branchGraph).prettyPrint();
    }
}
