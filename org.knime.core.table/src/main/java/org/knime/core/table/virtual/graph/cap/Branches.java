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
 */
package org.knime.core.table.virtual.graph.cap;

import static org.knime.core.table.virtual.graph.rag.RagEdgeType.ORDER;
import static org.knime.core.table.virtual.graph.rag.RagBuilder.executableNodeTypes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.knime.core.table.virtual.graph.rag.RagNode;

/**
 * Every RagNode points to a BranchRef.
 * Every BranchRef points to a Branch.
 * Every Branch has a current head RagNode.
 * Every Branch has a refs list of BranchRefs pointing to it.
 * <p>
 * A new Branch is set up for every new SOURCE RagNode:
 * The branch head initially points to the SOURCE RagNode.
 * A new BranchRef is set up pointing to the new branch, and added to the branches refs list.
 * The SOURCE RagNode points to the new BranchRef.
 * <p>
 * When a RagNode is added to a Branch:
 * The branch head is pointed to the node.
 * The node is pointed to the first BranchRef in the refs list of the branch.
 * <p>
 * To find the Branch an existing RagNode belongs to (currently):
 * Get the node's BranchRef, and the Branch that ref points to.
 * <p>
 * When two branches are merged, one branch (sub) is merged into the other (main) branch.
 * For each BranchRef in the sub branch's refs list, point it to the main branch.
 * Add the sub branch's refs list to the main branches refs list.
 */
class Branches {

    /**
     * A branch of execution.
     * This is incrementally constructed.
     * The {@code head} points to the currently last operation on this branch.
     * The {@code refs} list contains every {@link BranchRef} pointing to this branch.
     */
    class Branch {
        RagNode head;

        /**
         * The currently last executable node on this branch.
         * This is used to determine {@link #numRows()} if required.
         */
        RagNode executableHead;

        final List<BranchRef> refs;

        Branch() {
            refs = new ArrayList<>();
            refs.add(new BranchRef(this));
        }

        /**
         * Add a Node to a Branch:
         * The branch head is pointed to the node.
         * The node is pointed to the first BranchRef in the refs list of the branch.
         */
        void append(final RagNode node) {
            head = node;
            if (executableNodeTypes.contains(node.type())) {
                executableHead = node;
            }
            nodeBranchRefs.put(node, refs.get(0));
        }

        long numRows() {
            return executableHead == null ? -1 : executableHead.numRows();
        }

        /**
         * When two branches are merged, one branch (other) is merged into the main (this) branch.
         * For each BranchRef in the other branches refs list, point it to the main branch.
         * Merge the other branches refs list to the main branches refs list.
         */
        void merge(final Branch other) {
            other.refs.forEach(ref -> ref.branch = this);
            refs.addAll(other.refs);
        }
    }

    static class BranchRef {
        Branch branch;

        BranchRef(final Branch branch) {
            this.branch = branch;
        }
    }

    // every Node (that is part of the execution) points to a BranchRef.
    final Map<RagNode, BranchRef> nodeBranchRefs;

    Branches() {
        nodeBranchRefs = new HashMap<>();
    }

    Branches(final int initialCapacity) {
        nodeBranchRefs = new HashMap<>(initialCapacity);
    }

    /**
     * Create a new branch for a SOURCE node.
     * <p>
     * A new Branch is set up for every new Source node:
     * The branch head initially points to the Source node.
     * A new BranchRef is set up pointing to the new branch, and added to the branches refs list.
     * The Source node points to the new BranchRef.
     */
    Branch createBranch(final RagNode node) {
        var branch = new Branch();
        branch.append(node);
        return branch;
    }

    /**
     * Find the Branch an existing Node belongs to (currently):
     * Get the node's BranchRef, and the Branch that ref points to.
     */
    Branch getBranch(final RagNode node) {
        return nodeBranchRefs.get(node).branch;
    }

    /**
     * Get the branch for all of {@code node}'s predecessors.
     * If there is no unique predecessor branch, throws {@code IllegalArgumentException}.
     */
    Branch getPredecessorBranch(final RagNode node) {
        Branch branch = null;
        for (final RagNode predecessor : node.predecessors(ORDER)) {
            if (branch == null)
                branch = getBranch(predecessor);
            else if (branch != getBranch(predecessor))
                throw new IllegalArgumentException("predecessor branch is not unique");
        }
        if (branch == null)
            throw new IllegalStateException("there is no predecessor branch");
        return branch;
    }

    /**
     * Get the branches for all of {@code node}'s predecessors.
     * The returned list has no duplicates.
     */
    List<Branch> getPredecessorBranches(final RagNode node) {
        final List<Branch> branches = new ArrayList<>();
        for (final RagNode predecessor : node.predecessors(ORDER)) {
            final Branch b = getBranch(predecessor);
            if (!branches.contains(b))
                branches.add(b);
        }
        return branches;
    }
}
