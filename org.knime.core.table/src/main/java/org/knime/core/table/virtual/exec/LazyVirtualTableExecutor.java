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
 *   Created on May 26, 2021 by dietzc
 */
package org.knime.core.table.virtual.exec;

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
import org.knime.core.table.row.Selection.RowRangeSelection;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.virtual.RowAccessibles;
import org.knime.core.table.virtual.TableTransform;
import org.knime.core.table.virtual.spec.AppendMissingValuesTransformSpec;
import org.knime.core.table.virtual.spec.AppendTransformSpec;
import org.knime.core.table.virtual.spec.SelectColumnsTransformSpec;
import org.knime.core.table.virtual.spec.ConcatenateTransformSpec;
import org.knime.core.table.virtual.spec.IdentityTransformSpec;
import org.knime.core.table.virtual.spec.SliceTransformSpec;
import org.knime.core.table.virtual.spec.SourceTransformSpec;
import org.knime.core.table.virtual.spec.TableTransformSpec;

/*
 *   TODO
 *   1. This lazy executor just builds a lazy view on the data. However, in case we really want to "execute" the graph
 *   (e.g. with map, aggregate, predicate etc) we may want to leverage the structure of the underlying backend for
 *   efficient execution - for example that BatchStore(s)
 *   (see org.knime.core.columnar) consist of multiple batches and we can parallelize over the batches.
 *
 *   2. Some operations can be pushed further down to the backend for execution (e.g. expressions, Primitive map
 *   operations double->double or double,double->double or...).
 *   Only the backend can tell whether or not the graph (or part of the graph?) can be executed directly inside the
 *   backend.
 */
public class LazyVirtualTableExecutor implements VirtualTableExecutor {

    private final Map<TableTransform, List<TableTransform>> m_children = new HashMap<>();

    private final Set<TableTransform> m_sources = new HashSet<>();

    private final TableTransform m_leafTransform;

    // TODO: current implementation is limited to graphs with a single output (here: the leafTransform from which the
    // executor is built lazily)
    public LazyVirtualTableExecutor(final TableTransform leafTransform) {
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

    @SuppressWarnings("resource")
    @Override
    public List<RowAccessible> execute(final Map<UUID, RowAccessible> inputs) {

        final Deque<TableTransform> transformStack = new ArrayDeque<>();
        final Map<TableTransform, List<RowAccessible>> tables = new HashMap<>();

        m_sources.forEach(sourceTransform -> {
            tables.put(sourceTransform,
                Arrays.asList(inputs.get(((SourceTransformSpec)sourceTransform.getSpec()).getSourceIdentifier())));
            transformStack.push(sourceTransform);
        });

        while (!transformStack.isEmpty()) {//NOSONAR refactoring would decrease readability here
            final TableTransform node = transformStack.pop();

            m_children.getOrDefault(node, Collections.emptyList()).forEach(transformStack::push);

            if (tables.containsKey(node)) {
                continue;
            }

            // Test for incomplete argument list of non-unary transforms.
            if (node.getPrecedingTransforms().size() > 1
                && node.getPrecedingTransforms().stream().anyMatch(t -> !tables.containsKey(t))) {
                // We cannot process the node yet because not all parents have been visited.
                transformStack.addLast(node);
                continue;
            }

            final List<RowAccessible> precedingTables = node.getPrecedingTransforms().stream()//
                .map(tables::get)//
                .flatMap(List<RowAccessible>::stream)//
                .collect(Collectors.toList());

            // this is what needs to filled
            final List<RowAccessible> transformedTables = createTransformedTables(node.getSpec(), precedingTables);
            tables.put(node, transformedTables);
        }

        // TODO only single output graphs supported at the moment (as this is a lazy implementation)...
        return tables.get(m_leafTransform);
    }

    @SuppressWarnings("resource")
    private static List<RowAccessible> createTransformedTables(final TableTransformSpec spec, //NOSONAR
        final List<RowAccessible> predecessors) {
        // TODO visitor pattern?
        final RowAccessible predecessor = predecessors.get(0);
        if (spec instanceof SelectColumnsTransformSpec) {
            final int[] selection = ((SelectColumnsTransformSpec)spec).getColumnSelection();
            return List.of(RowAccessibles.filter(predecessor, selection));
        } else if (spec instanceof AppendMissingValuesTransformSpec) {
            final ColumnarSchema appendedSchema = ((AppendMissingValuesTransformSpec)spec).getAppendedSchema();
            return List.of(RowAccessibles.appendMissing(predecessor, appendedSchema));
        } else if (spec instanceof SliceTransformSpec) {
            final RowRangeSelection rowRange = ((SliceTransformSpec)spec).getRowRangeSelection();
            return List.of(RowAccessibles.slice(predecessor, rowRange));
        } else if (spec instanceof IdentityTransformSpec) {
            return List.of(predecessor);
        } else {
            final RowAccessible[] other = predecessors.subList(1, predecessors.size()).toArray(RowAccessible[]::new);
            if (spec instanceof ConcatenateTransformSpec) {
                return List.of(RowAccessibles.concatenate(predecessor, other));
            } else if (spec instanceof AppendTransformSpec) {
                return List.of(RowAccessibles.append(predecessor, other));
            } else {
                throw new IllegalArgumentException("Unsupported transformation: " + spec);
            }
        }
    }

}
