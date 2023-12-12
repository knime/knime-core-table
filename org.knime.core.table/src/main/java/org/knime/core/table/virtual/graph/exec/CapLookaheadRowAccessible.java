package org.knime.core.table.virtual.graph.exec;

import java.util.Map;
import java.util.UUID;

import org.knime.core.table.cursor.LookaheadCursor;
import org.knime.core.table.row.LookaheadRowAccessible;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.row.Selection;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.virtual.graph.rag.RagGraph;

class CapLookaheadRowAccessible extends CapRowAccessible implements LookaheadRowAccessible {

    CapLookaheadRowAccessible( //
            final RagGraph specGraph, //
            final ColumnarSchema schema, //
            final Map<UUID, RowAccessible> availableSources) {

        super(specGraph, schema, availableSources);
    }

    @Override
    public LookaheadCursor<ReadAccessRow> createCursor() {
        return new CapLookaheadCursor(getCursorData(Selection.all()));
    }

    @Override
    public LookaheadCursor<ReadAccessRow> createCursor(final Selection selection) {
        return new CapLookaheadCursor(getCursorData(selection));
    }
}
