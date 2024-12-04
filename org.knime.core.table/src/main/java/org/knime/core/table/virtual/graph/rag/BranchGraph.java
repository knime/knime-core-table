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
package org.knime.core.table.virtual.graph.rag;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.knime.core.table.virtual.graph.rag.prettyprint.DependencyGraph;

/**
 * Sequentialize a {@link TableTransformGraph} into a tree:
 * <ul>
 * <li>{@link BranchNode Nodes} of the tree are SOURCE, CONCATENATE, and APPEND operations (the only nodes where
 * execution splits into branches).</li>
 * <li>{@link BranchEdge Edges} between tree nodes contain all other operations (MAP, ROWFILTER, ROWINDEX, SLICE,
 * OBSERVER).</li>
 * </ul>
 */
public class BranchGraph {

    public static final Comparator<InnerNode> DEFAULT_POLICY = //
            Comparator.comparingInt(node -> switch (node.type()) {
                case MAP -> 1;
                default -> 0;
            });

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

    private final TableTransformGraph m_tableTransformGraph;

    private final Map<TableTransformGraph.Node, AbstractNode> m_depNodes;

    private final BranchEdge m_rootBranch;

    private final Comparator<InnerNode> m_policy;

    public BranchGraph(final TableTransformGraph tableTransformGraph) {
        m_tableTransformGraph = tableTransformGraph;
        m_depNodes = new HashMap<>();
        m_rootBranch = getBranch(tableTransformGraph.terminal());
        m_policy = DEFAULT_POLICY;
        sequentialize(m_rootBranch);
    }

    /**
     * Get the root {@code BranchEdge} (that leads to the terminal of {@link #tableTransformGraph()}).
     * (This is the entry point into the sequentialized graph.)
     */
    public BranchEdge rootBranch() {
        return m_rootBranch;
    }

    /**
     * Get the {@code TableTransformGraph} from which this {@code BranchGraph} was built.
     */
    public TableTransformGraph tableTransformGraph() {
        return m_tableTransformGraph;
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
     * If {@code node} is SOURCE, APPEND, or CONCATENATE the retrieved {@code AbstractNode} is set as
     * {@code branchTarget}.
     * <p>
     * If {@code node} is SLICE, MAP, ROWFILTER, ROWINDEX, or OBSERVER the retrieved {@code AbstractNode} is added to
     * {@code innerNodes}.
     *
     * @param node the node for which to create or retrieve the corresponding {@code AbstractNode}
     * @param innerNodes all {@code InnerNode} encountered while walking to the next {@code BranchNode}
     * @param branchTarget the next {@code BranchNode}
     * @return {@link AbstractNode} wrapping the given node
     */
    private AbstractNode getDepNode(final TableTransformGraph.Node node, final List<InnerNode> innerNodes,
            final AtomicReference<BranchNode> branchTarget) {
        final AbstractNode depNode = m_depNodes.get(node);
        if (depNode != null) {
            return depNode;
        }
        switch (node.type()) {
            case SOURCE, APPEND, CONCATENATE -> { // NOSONAR
                final ArrayList<BranchEdge> branches = new ArrayList<>();
                node.in().forEach(port -> branches.add(getBranch(port)));
                final BranchNode branchNode = new BranchNode(node, branches);
                branchTarget.setPlain(branchNode);
                m_depNodes.put(node, branchNode);
                return branchNode;
            }
            case SLICE, MAP, ROWFILTER, ROWINDEX, OBSERVER -> { // NOSONAR
                final TableTransformGraph.Port port = node.in(0);
                final Set<AbstractNode> dependencies = getDependencies(port, innerNodes, branchTarget);
                final InnerNode innerNode = new InnerNode(node, dependencies);
                innerNodes.add(innerNode);
                m_depNodes.put(node, innerNode);
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
                .sorted(m_policy) //
                .findFirst().orElseThrow();
            branch.innerNodes().add(next);
            todo.remove(next);
        }
    }

    @Override
    public String toString() {
        return "BranchGraph" + DependencyGraph.prettyPrint(this);
    }
}
