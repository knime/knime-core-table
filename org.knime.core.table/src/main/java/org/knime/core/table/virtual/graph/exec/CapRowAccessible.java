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

    private final CursorAssemblyPlan cap;

    private final Map<UUID, RowAccessible> availableSources;

    private final LoadingCache<Selection, CapCursorData> capCache;

    CapRowAccessible( //
            final RagGraph specGraph, //
            final ColumnarSchema schema, //
            final CursorAssemblyPlan cap, //
            final Map<UUID, RowAccessible> availableSources) {
        this.specGraph = specGraph;
        this.schema = schema;
        this.cap = cap;
        this.availableSources = availableSources;

        // TODO (TP) Should we add Caffeine as a dependency?
        //           The Guava doc recommends to prefer it over com.google.common.cache
        //           https://guava.dev/releases/snapshot-jre/api/docs/com/google/common/cache/CacheBuilder.html
        // TODO (TP) It might be better to use softValues().
        //           This would be better for caching, but I'm not sure how much
        //           pressure it would put on other caches. In practice,
        //           CapRowAccessible is probably short-lived anyway, but better
        //           to be safe for now.
        this.capCache = CacheBuilder.newBuilder().weakValues().build(new CacheLoader<>() {
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
        return cap.numRows();
    }

    @Override
    public void close() throws IOException {
        // TODO ?
    }

    record CapCursorData(CursorAssemblyPlan cap, List<RowAccessible> sources, int numColumns, int[] selectedColumns) {

        NodeImpConsumer assembleConsumer() {
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

        final int numColumns = schema.numColumns();
        final CursorAssemblyPlan scap;
        final int[] selected;

        if (selection.allSelected()) {
            scap = cap;
            selected = null;
        } else {
            final RagGraph graph = SpecGraphBuilder.appendSelection(specGraph, selection);
            final List<RagNode> orderedRag = RagBuilder.createOrderedRag(graph, false);
            scap = CapBuilder.createCursorAssemblyPlan(orderedRag);
            selected = selection.columns().allSelected(0, numColumns) //
                    ? null //
                    : selection.columns().getSelected(0, numColumns);
        }

        final List<RowAccessible> sources = CapExecutorUtils.getSources(scap, availableSources);
        return new CapCursorData(scap, sources, numColumns, selected);
    }
}
