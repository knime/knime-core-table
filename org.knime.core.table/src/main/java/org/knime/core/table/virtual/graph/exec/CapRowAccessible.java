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
import org.knime.core.table.virtual.graph.cap.CapBuilder;
import org.knime.core.table.virtual.graph.cap.CursorAssemblyPlan;
import org.knime.core.table.virtual.graph.rag.RagBuilder;
import org.knime.core.table.virtual.graph.rag.RagGraph;
import org.knime.core.table.virtual.graph.rag.RagNode;
import org.knime.core.table.virtual.graph.rag.SpecGraphBuilder;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

class CapRowAccessible implements RowAccessible {

    private final RagGraph specGraph;

    private final ColumnarSchema schema;

    private final Map<UUID, RowAccessible> availableSources;

    private final LoadingCache<Selection, CapCursorData> capCache;

    CapRowAccessible( //
            final RagGraph specGraph, //
            final ColumnarSchema schema, //
            final Map<UUID, RowAccessible> availableSources) {
        this.specGraph = specGraph;
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

        final RagGraph graph = SpecGraphBuilder.appendSelection(specGraph, selection);
        final List<RagNode> orderedRag = RagBuilder.createOrderedRag(graph, false);
        final CursorAssemblyPlan cap = CapBuilder.createCursorAssemblyPlan(orderedRag);
        final int numColumns = schema.numColumns();
        final int[] selected = selection.columns().allSelected(0, numColumns) //
                ? null //
                : selection.columns().getSelected(0, numColumns);

        final List<RowAccessible> sources = CapExecutorUtils.getSources(cap, availableSources);
        return new CapCursorData(cap, sources, numColumns, selected);
    }
}
