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
package org.knime.core.table.virtual.graph.exec;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.IntFunction;

import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.cursor.Cursor;
import org.knime.core.table.row.DefaultReadAccessRow;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.row.Selection;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.virtual.graph.cap.CursorAssemblyPlan;
import org.knime.core.table.virtual.graph.rag3.BranchGraph;
import org.knime.core.table.virtual.graph.rag3.BuildCap;
import org.knime.core.table.virtual.graph.rag3.TableTransformGraph;
import org.knime.core.table.virtual.graph.rag3.TableTransformUtil;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

class CapRowAccessible implements RowAccessible {

    private final TableTransformGraph tableTransformGraph;

    private final ColumnarSchema schema;

    private final Map<UUID, RowAccessible> availableSources;

    private final LoadingCache<Selection, CapCursorData> capCache;

    CapRowAccessible( //
            final TableTransformGraph tableTransformGraph, //
            final ColumnarSchema schema, //
            final Map<UUID, RowAccessible> availableSources) {
        this.tableTransformGraph = tableTransformGraph;
        this.schema = schema;
        this.availableSources = availableSources;

        // TODO (TP) Should we add Caffeine as a dependency?
        //           The Guava doc recommends to prefer it over com.google.common.cache
        //           https://guava.dev/releases/snapshot-jre/api/docs/com/google/common/cache/CacheBuilder.html
        this.capCache = CacheBuilder.newBuilder().softValues().build(new CacheLoader<>() {
            @Override
            public CapCursorData load(Selection selection) {
                return createCursorData(selection);
            }
        });
    }

    @Override
    public ColumnarSchema getSchema() {
        return schema;
    }

    @Override
    public Cursor<ReadAccessRow> createCursor() {
        return new CapCursor(getCursorData(Selection.all()));
    }

    @Override
    public Cursor<ReadAccessRow> createCursor(final Selection selection) {
        return new CapCursor(getCursorData(selection));
    }

    @Override
    public long size() {
        return getCursorData(Selection.all()).numRows();
    }

    @Override
    public void close() throws IOException {
        // TODO ?
    }

    record CapCursorData(CursorAssemblyPlan cap, List<RowAccessible> sources, int numColumns, int[] selectedColumns) {

        SequentialNodeImpConsumer assembleConsumer() {
            return new AssembleNodeImps(cap.nodes(), sources).getConsumer();
        }

        RandomAccessNodeImpConsumer assembleRandomAccessConsumer() {
            return new AssembleRandomAccessibleNodeImps(cap.nodes(), sources).getConsumer();
        }

        long numRows() {
            return cap.numRows();
        }

        ReadAccessRow createReadAccessRow(final IntFunction<? extends ReadAccess> generator) {
            return selectedColumns == null //
                    ? new DefaultReadAccessRow(numColumns, generator) //
                    : new DefaultReadAccessRow(numColumns, generator, selectedColumns);
        }
    }

    CapCursorData getCursorData(final Selection selection) {
        return capCache.getUnchecked(selection);
    }

    private CapCursorData createCursorData(final Selection selection) {

        final TableTransformGraph graph = TableTransformUtil.appendSelection(tableTransformGraph, selection);
        // TODO (TP) optimize graph
        final CursorAssemblyPlan cap = BuildCap.createCursorAssemblyPlan(new BranchGraph(graph));
        final int numColumns = schema.numColumns();
        final int[] selected = selection.columns().allSelected(0, numColumns) //
                ? null //
                : selection.columns().getSelected(0, numColumns);

        final List<RowAccessible> sources = CapExecutorUtils.getSources(cap, availableSources);
        return new CapCursorData(cap, sources, numColumns, selected);
    }
}
