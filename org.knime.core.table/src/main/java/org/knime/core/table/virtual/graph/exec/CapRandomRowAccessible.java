package org.knime.core.table.virtual.graph.exec;

import java.io.IOException;
import java.util.List;

import org.knime.core.table.cursor.RandomAccessCursor;
import org.knime.core.table.row.RandomRowAccessible;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.row.Selection;
import org.knime.core.table.row.Selection.ColumnSelection;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.virtual.graph.cap.CapBuilder;
import org.knime.core.table.virtual.graph.cap.CursorAssemblyPlan;
import org.knime.core.table.virtual.graph.rag.RagBuilder;
import org.knime.core.table.virtual.graph.rag.RagGraph;
import org.knime.core.table.virtual.graph.rag.RagNode;
import org.knime.core.table.virtual.graph.rag.SpecGraphBuilder;

class CapRandomRowAccessible implements RandomRowAccessible {

    private final RagGraph specGraph;

    private final ColumnarSchema schema;

    private final CursorAssemblyPlan cap;

    private final List<RowAccessible> sources;

    CapRandomRowAccessible( //
            final RagGraph specGraph, //
            final ColumnarSchema schema, //
            final CursorAssemblyPlan cap, //
            final List<RowAccessible> sources) {
        this.specGraph = specGraph;
        this.schema = schema;
        this.cap = cap;
        this.sources = sources;
    }

    @Override
    public ColumnarSchema getSchema() {
        return schema;
    }

    @Override
    public RandomAccessCursor<ReadAccessRow> createCursor() {
        return new CapRandomAccessCursor(assembleConsumer(cap), size());
    }

    @Override
    public RandomAccessCursor<ReadAccessRow> createCursor(final Selection selection) {

        if (selection.allSelected()) {
            return createCursor();
        }

        final RagGraph graph = SpecGraphBuilder.appendSelection(specGraph, selection);
        final List<RagNode> orderedRag = RagBuilder.createOrderedRag(graph);
        final CursorAssemblyPlan scap = CapBuilder.createCursorAssemblyPlan(orderedRag);

        final ColumnSelection cols = selection.columns();
        if (cols.allSelected(0, schema.numColumns())) {
            return new CapRandomAccessCursor( assembleConsumer(scap), scap.numRows() );
        } else {
            final int[] selected = cols.getSelected(0, schema.numColumns());
            return new CapRandomAccessCursor( assembleConsumer(scap), selected, scap.numRows() );
        }
    }

    @Override
    public long size() {
        return cap.numRows();
    }

    @Override
    public void close() throws IOException {
        // TODO ?
    }

    RandomAccessNodeImpConsumer assembleConsumer(final CursorAssemblyPlan cap) {
        return new AssembleRandomAccessibleNodeImps(cap.nodes(), sources).getConsumer();
    }
}
