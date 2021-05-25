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
 *   May 1, 2021 (marcel): created
 */
package org.knime.core.table.virtual;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.knime.core.table.row.RowAccessible;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
public final class TableTransformer {

    private final Map<TableTransform, List<TableTransform>> m_children = new HashMap<>();

    private final Set<TableTransform> m_sources = new HashSet<>();

    private final Map<UUID, RowAccessible> m_sourceTables;

    // TODO: current implementation is limited to graphs with a single output
    private final TableTransform m_leafTransform;

    public TableTransformer(final Map<UUID, RowAccessible> sources, final TableTransform leafTransform) {
        m_sourceTables = sources;
        m_leafTransform = leafTransform;
        traceBack(leafTransform);
    }

    private void traceBack(final TableTransform transform) {
        final List<TableTransform> parents = transform.getPrecedingTransforms();
        if (parents.isEmpty()) {
            m_sources.add(transform);
        } else {
            for (final TableTransform parent : parents) {
                List<TableTransform> children = m_children.get(parent);
                if (children == null) {
                    children = new ArrayList<>();
                    children.add(transform);
                    m_children.put(parent, children);
                    traceBack(parent);
                } else {
                    children.add(transform);
                }
            }
        }
    }

    public RowAccessible transform() {
        final Deque<TableTransform> transformStack = new ArrayDeque<>();
        final Map<TableTransform, List<RowAccessible>> tables = new HashMap<>();

        m_sources.forEach(sourceTransform -> {
            tables.put(sourceTransform, Arrays
                .asList(m_sourceTables.get(((SourceTransformSpec)sourceTransform.getSpec()).getSourceIdentifier())));
            transformStack.push(sourceTransform);
        });

        while (!transformStack.isEmpty()) {
            final TableTransform node = transformStack.pop();

            m_children.getOrDefault(node, Collections.emptyList()).forEach(transformStack::push);

            if (tables.containsKey(node)) {
                continue;
            }

            // Test for incomplete argument list of non-unary transforms.
            if (node.getPrecedingTransforms().size() > 1 &&
                node.getPrecedingTransforms().stream().anyMatch(t -> !tables.containsKey(t))) {
                // We cannot process the node yet because not all parents have been visited.
                transformStack.addLast(node);
                continue;
            }

            // TODO: here is where we need to add support for Partitioning and other 1-to-N Ops: successors of such
            // Ops generally do not have all of their predecessors' tables as inputs.
            final List<RowAccessible> precedingTables = node.getPrecedingTransforms().stream().map(tables::get)
                .flatMap(List<RowAccessible>::stream).collect(Collectors.toList());
            final List<RowAccessible> transformedTables = node.getSpec().transformTables(precedingTables);
            tables.put(node, transformedTables);
        }

        return tables.get(m_leafTransform).get(0);
    }
}
