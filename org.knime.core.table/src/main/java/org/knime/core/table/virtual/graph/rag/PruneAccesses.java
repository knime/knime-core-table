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
import java.util.HashSet;
import java.util.Set;

import org.knime.core.table.virtual.graph.rag.TableTransformUtil.AppendAccesses;

/**
 * Identify and remove {@code AccessId} that are produced but never used in
 * a {@code TableTransformGraph}.
 */
final class PruneAccesses {

    /**
     * Identify and remove {@code AccessId} that are produced but never used in the {@code graph}.
     *
     * @return {@code true} if any unused access was found and removed
     */
    public static boolean pruneAccesses(final TableTransformGraph graph) {
        return new PruneAccesses(graph).pruneAccesses();
    }

    /**
     * Recursively find "required" {@code AccessId}s:
     * <ul>
     * <li>Every input of the terminal port is required.</li>
     * <li>Every control flow dependency of the terminal port is required,</li>
     * </ul>
     */
    private PruneAccesses(final TableTransformGraph graph) {
        final TableTransformGraph.Port port = graph.terminal();
        port.accesses().forEach(this::addRequired);
        port.controlFlowEdges().forEach(edge -> addRequired(edge.to().owner()));
    }

    private void addRequired(final TableTransformGraph.Node node) {
        if (m_requiredNodes.add(node)) {
            switch (node.type()) {
                case SOURCE, SLICE, ROWINDEX, APPEND, CONCATENATE -> { // NOSONAR
                }
                case ROWFILTER, OBSERVER -> node.in(0).accesses().forEach(this::addRequired);
                default -> throw new IllegalArgumentException();
            }
            node.in().forEach( //
                    port -> port.controlFlowEdges().forEach( //
                            edge -> addRequired(edge.to().owner())));
        }
    }

    private final Set<AccessId> m_requiredAccessIds = new HashSet<>();

    private final Set<TableTransformGraph.Node> m_requiredNodes = new HashSet<>();

    private void addRequired(final AccessId a) {
        final AccessId access = a.find();
        if (m_requiredAccessIds.add(access)) {
            final TableTransformGraph.Node node = access.producer().node();
            switch (node.type()) {
                case SOURCE, ROWINDEX -> { // NOSONAR
                }
                case APPEND -> addRequired(AppendAccesses.find(access).input());
                case CONCATENATE -> {
                    final int i = node.out().accesses().indexOf(access);
                    node.in().forEach(in -> addRequired(in.access(i)));
                }
                case MAP -> {
                    m_requiredNodes.add(node);
                    node.in(0).accesses().forEach(this::addRequired);
                }
                default -> throw new IllegalArgumentException();
            }
        }
    }

    /**
     * @return true if any unused access was found and removed
     */
    private boolean pruneAccesses() {
        boolean pruned = false;
        for (TableTransformGraph.Node node : m_requiredNodes) {
            final ArrayList<AccessId> unused = new ArrayList<>();
            node.out().accesses().forEach(a -> {
                AccessId access = a.find();
                if (!m_requiredAccessIds.contains(access)) {
                    unused.add(access);
                }
            });
            if (!unused.isEmpty()) {
                pruned = true;
                switch (node.type()) {
                    case SOURCE, MAP, ROWINDEX -> node.out().accesses().removeAll(unused);
                    case APPEND -> unused.forEach(access -> AppendAccesses.find(access).remove());
                    case CONCATENATE -> unused.forEach(access -> {
                        final int i = node.out().accesses().indexOf(access);
                        node.out().accesses().remove(i);
                        node.in().forEach(in -> in.accesses().remove(i));
                    });
                    default -> throw new IllegalArgumentException();
                }
            }
        }
        return pruned;
    }
}
