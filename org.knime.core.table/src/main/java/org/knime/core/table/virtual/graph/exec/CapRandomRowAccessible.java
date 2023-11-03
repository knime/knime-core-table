package org.knime.core.table.virtual.graph.exec;

import java.util.Map;
import java.util.UUID;

import org.knime.core.table.cursor.RandomAccessCursor;
import org.knime.core.table.row.RandomRowAccessible;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.row.Selection;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.virtual.graph.cap.CursorAssemblyPlan;
import org.knime.core.table.virtual.graph.rag.RagGraph;

class CapRandomRowAccessible extends CapRowAccessible implements RandomRowAccessible {

    CapRandomRowAccessible( //
            final RagGraph specGraph, //
            final ColumnarSchema schema, //
            final CursorAssemblyPlan cap, //
            final Map<UUID, RowAccessible> availableSources) {

        super(specGraph, schema, cap, availableSources);
    }

    @Override
    public RandomAccessCursor<ReadAccessRow> createCursor() {
        return new CapRandomAccessCursor(getCursorData(null));
    }

    @Override
    public RandomAccessCursor<ReadAccessRow> createCursor(final Selection selection) {
        return new CapRandomAccessCursor(getCursorData(selection));
    }
}
