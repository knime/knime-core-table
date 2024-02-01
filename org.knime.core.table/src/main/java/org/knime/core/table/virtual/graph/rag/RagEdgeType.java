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
package org.knime.core.table.virtual.graph.rag;

public enum RagEdgeType {
    /**
     * <em>Spec Edges</em> reflect {@code TableTransform.getPrecedingTransforms()}
     * relations from the {@code VirtualTable}.
     * <p>
     * Edge direction is from data producer to data consumer, i.e., from source table
     * to resulting virtual table.
     */
    SPEC,

    /**
     * <em>Data Dependency Edges</em> link the node producing a particular access to
     * nodes consuming that access.
     * <p>
     * Edge direction is from producer to consumer.
     */
    DATA,

    /**
     * <em>Execution Dependency Edges</em> link nodes that depend on each other for
     * executing forward steps in order to determine that everybody is on the same row
     * (without looking at produced/consumed data). For example, to forward the CONSUMER
     * node, all SOURCE nodes have to forward (at least once) and all ROWFILTERs have to pass.
     * <p>
     * Edge direction is from executes-earlier to executes-later.
     * <p>
     * TODO: Find a better name! Mapping operations also need to execute,
     *       but they are not linked by EXEC edges.
     */
    EXEC,

    /**
     * <em>Execution Ordering Edges</em> form the partial order in which nodes need to
     * be executed, where "execute" means forwarding cursors, computing map results,
     * testing filters, etc, depending on the node type.
     */
    ORDER,

    /**
     * For visualization and debugging only...
     */
    FLATTENED_ORDER,
}
