package org.knime.core.table.virtual.graph.exec;

import org.knime.core.table.cursor.LookaheadCursor;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.virtual.graph.exec.CapRowAccessible.CapCursorData;

class CapLookaheadCursor extends CapCursor implements LookaheadCursor<ReadAccessRow> {

    public CapLookaheadCursor(final CapCursorData data) {
        super(data);
    }

    @Override
    public boolean canForward() {
        return node.canForward();
    }
}
