package org.knime.core.table.virtual.graph.exec;

import java.util.Map;
import java.util.UUID;

import org.knime.core.table.cursor.RandomAccessCursor;
import org.knime.core.table.row.RandomRowAccessible;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.row.Selection;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.virtual.graph.rag.RagGraph;

class CapRandomRowAccessible extends CapRowAccessible implements RandomRowAccessible {

    CapRandomRowAccessible( //
            final RagGraph specGraph, //
            final ColumnarSchema schema, //
            final Map<UUID, RowAccessible> availableSources) {

        super(specGraph, schema, availableSources);
    }

    @Override
    public RandomAccessCursor<ReadAccessRow> createCursor() {
        return new CapRandomAccessCursor(getCursorData(Selection.all()));
    }

    @Override
    public RandomAccessCursor<ReadAccessRow> createCursor(final Selection selection) {
        return new CapRandomAccessCursor(getCursorData(selection));
    }
}
