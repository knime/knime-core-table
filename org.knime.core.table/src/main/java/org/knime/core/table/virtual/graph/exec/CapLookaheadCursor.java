package org.knime.core.table.virtual.graph.exec;

import org.knime.core.table.cursor.LookaheadCursor;
import org.knime.core.table.row.ReadAccessRow;

class CapLookaheadCursor extends CapCursor implements LookaheadCursor<ReadAccessRow> {

    public CapLookaheadCursor(final NodeImpConsumer node) {
        super(node);
    }

    public CapLookaheadCursor(final NodeImpConsumer node, final int[] selected) {
        super(node, selected);
    }

    @Override
    public boolean canForward() {
        return node.canForward();
    }
}
