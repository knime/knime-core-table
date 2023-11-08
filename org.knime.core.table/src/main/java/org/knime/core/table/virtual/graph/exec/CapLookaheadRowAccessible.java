package org.knime.core.table.virtual.graph.exec;

import java.util.List;

import org.knime.core.table.cursor.LookaheadCursor;
import org.knime.core.table.row.LookaheadRowAccessible;
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

class CapLookaheadRowAccessible extends CapRowAccessible implements LookaheadRowAccessible {

    CapLookaheadRowAccessible( //
            final RagGraph specGraph, //
            final ColumnarSchema schema, //
            final CursorAssemblyPlan cap, //
            final List<RowAccessible> sources) {

        super(specGraph, schema, cap, sources);
    }

    @Override
    public LookaheadCursor<ReadAccessRow> createCursor() {
        return new CapLookaheadCursor(assembleConsumer(cap));
    }

    @Override
    public LookaheadCursor<ReadAccessRow> createCursor(final Selection selection) {

        if (selection.allSelected()) {
            return createCursor();
        }

        final RagGraph graph = SpecGraphBuilder.appendSelection(specGraph, selection);
        final List<RagNode> orderedRag = RagBuilder.createOrderedRag(graph);
        final CursorAssemblyPlan scap = CapBuilder.createCursorAssemblyPlan(orderedRag);

        final Selection.ColumnSelection cols = selection.columns();
        if (cols.allSelected(0, schema.numColumns())) {
            return new CapLookaheadCursor( assembleConsumer(scap) );
        } else {
            final int[] selected = cols.getSelected(0, schema.numColumns());
            return new CapLookaheadCursor( assembleConsumer(scap), selected );
        }
    }
}
