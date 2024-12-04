/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ---------------------------------------------------------------------
 *
 * History
 *   3 Dec 2024 (pietzsch): created
 */
package org.knime.core.table.virtual.graph.rag.prettyprint;

import static org.knime.core.table.virtual.graph.rag.prettyprint.DependencyGraph.EdgeType.CONTROL;
import static org.knime.core.table.virtual.graph.rag.prettyprint.DependencyGraph.EdgeType.DATA;
import static org.knime.core.table.virtual.graph.rag.prettyprint.DependencyGraph.EdgeType.EXECUTION;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.knime.core.table.virtual.graph.rag.BranchGraph;
import org.knime.core.table.virtual.graph.rag.TableTransformGraph;
import org.knime.core.table.virtual.graph.rag.TableTransformGraph.Port;
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
        Node(final TableTransformGraph.Node node) {
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
        addRecursively(consumer, branchGraph.rootBranch());
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
