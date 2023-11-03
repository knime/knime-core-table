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

class CapRowAccessible implements RowAccessible {

    private final RagGraph specGraph;

    private final ColumnarSchema schema;

    private final CursorAssemblyPlan cap;

    private final List<RowAccessible> sources;

    private final Map<UUID, RowAccessible> availableSources;

    CapRowAccessible( //
            final RagGraph specGraph, //
            final ColumnarSchema schema, //
            final CursorAssemblyPlan cap, //
            final Map<UUID, RowAccessible> availableSources) {
        this.specGraph = specGraph;
        this.schema = schema;
        this.cap = cap;
        this.availableSources = availableSources;
        this.sources = CapExecutorUtils.getSources(cap, availableSources);
    }

    @Override
    public ColumnarSchema getSchema() {
        return schema;
    }

    @Override
    public Cursor<ReadAccessRow> createCursor() {
        return new CapCursor(getCursorData(null));
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

        final int numColumns = schema.numColumns();

        if (selection == null || selection.allSelected()) {
            return new CapCursorData(cap, sources, numColumns, null);
        }

        final RagGraph graph = SpecGraphBuilder.appendSelection(specGraph, selection);
        final List<RagNode> orderedRag = RagBuilder.createOrderedRag(graph, false);
        final CursorAssemblyPlan scap = CapBuilder.createCursorAssemblyPlan(orderedRag);

        final Selection.ColumnSelection cols = selection.columns();
        if (cols.allSelected(0, numColumns)) {
            return new CapCursorData(scap, sources, numColumns, null);
        } else {
            final int[] selected = cols.getSelected(0, numColumns);
            final List<RowAccessible> sources = CapExecutorUtils.getSources(scap, availableSources);
            return new CapCursorData(scap, sources, numColumns, selected);
        }

    }
}
