package org.knime.core.table.virtual.graph.rag3;

import static org.knime.core.table.virtual.graph.rag3.DependencyGraph.EdgeType.CONTROL;
import static org.knime.core.table.virtual.graph.rag3.DependencyGraph.EdgeType.DATA;

import java.util.LinkedHashSet;
import java.util.Set;

import org.knime.core.table.virtual.graph.rag3.TableTransformGraph.Node;

/**
 * A graph that explicitly represents execution ordering constraints between the
 * {@link Node}s in a {@link TableTransformGraph}.
 * <p>
 * Used for debugging, i.e., pretty-printing and writing {@link Mermaid}.
 */
public class DependencyGraph {

    enum EdgeType {DATA, CONTROL}

    record Edge(EdgeType type, Node from, Node to) {
        @Override
        public String toString() {
            return "(" + from.id() + " -> " + to.id() + ", " + type + ")";
        }
    }

    final Set<Node> nodes = new LinkedHashSet<>();

    final Set<Edge> edges = new LinkedHashSet<>();

    public DependencyGraph(TableTransformGraph tableTransformGraph) {
        final Node consumer = new Node();
        consumer.in().add(tableTransformGraph.terminal());
        addRecursively(consumer);
    }

    private void addRecursively(Node node) {
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
        final StringBuilder sb = new StringBuilder("DependencyGraph{");
        sb.append("\n  nodes=");
        nodes.forEach(node -> sb.append("\n    ").append(node));
        sb.append("\n  edges=");
        edges.forEach(edge -> sb.append("\n    ").append(edge));
        sb.append("\n}");
        return sb.toString();
    }
}
