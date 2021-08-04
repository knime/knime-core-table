/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME AG, Zurich, Switzerland
 *
 * You may not modify, publish, transmit, transfer or sell, reproduce,
 * create derivative works from, distribute, perform, display, or in
 * any way exploit any of the content, in whole or in part, except as
 * otherwise expressly permitted in writing by the copyright owner or
 * as specified in the license file distributed with this product.
 *
 * If you have any questions please contact the copyright holder:
 * website: www.knime.com
 * email: contact@knime.com
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
import org.knime.core.table.virtual.RowAccessibles;
import org.knime.core.table.virtual.TableTransform;
import org.knime.core.table.virtual.spec.AppendMissingValuesTransformSpec;
import org.knime.core.table.virtual.spec.AppendTransformSpec;
import org.knime.core.table.virtual.spec.ColumnFilterTransformSpec;
import org.knime.core.table.virtual.spec.ConcatenateTransformSpec;
import org.knime.core.table.virtual.spec.IdentityTransformSpec;
import org.knime.core.table.virtual.spec.PermuteTransformSpec;
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
    private static List<RowAccessible> createTransformedTables(final TableTransformSpec spec,//NOSONAR
        final List<RowAccessible> predecessors) {
        // TODO visitor pattern?
        if (spec instanceof ColumnFilterTransformSpec) {
            return List
                .of(RowAccessibles.filter(predecessors.get(0), ((ColumnFilterTransformSpec)spec).getColumnSelection()));
        } else if (spec instanceof ConcatenateTransformSpec) {
            return List.of(RowAccessibles.concatenate(predecessors.get(0),
                predecessors.subList(1, predecessors.size()).toArray(RowAccessible[]::new)));
        } else if (spec instanceof AppendTransformSpec) {
            return List.of(RowAccessibles.append(predecessors.get(0),
                predecessors.subList(1, predecessors.size()).toArray(RowAccessible[]::new)));
        } else if (spec instanceof PermuteTransformSpec) {
            return List.of(RowAccessibles.permute(predecessors.get(0), ((PermuteTransformSpec)spec).getPermutation()));
        } else if (spec instanceof AppendMissingValuesTransformSpec) {
            return List.of(RowAccessibles.appendMissing(predecessors.get(0),
                ((AppendMissingValuesTransformSpec)spec).getAppendedSchema()));
        } else if (spec instanceof SliceTransformSpec) {
            return List
                .of(RowAccessibles.slice(predecessors.get(0), ((SliceTransformSpec)spec).getRowRangeSelection()));
        } else if (spec instanceof IdentityTransformSpec) {
            return List.of(predecessors.get(0));
        } else {
            throw new IllegalArgumentException("Unsupported transformation: " + spec);
        }
    }

}
