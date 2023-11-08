package org.knime.core.table.virtual.graph.exec;

import java.io.IOException;

import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.cursor.Cursor;
import org.knime.core.table.row.DefaultReadAccessRow;
import org.knime.core.table.row.ReadAccessRow;

class CapCursor implements Cursor<ReadAccessRow> {

    final NodeImpConsumer node;

    private final DefaultReadAccessRow access;

    public CapCursor(final NodeImpConsumer node) {
        this.node = node;
        node.create();
        access = new DefaultReadAccessRow(node.numOutputs(), node::getOutput);
    }

    public CapCursor(final NodeImpConsumer node, final int[] selected) {
        this.node = node;
        node.create();
        final ReadAccess[] accesses = new ReadAccess[node.numOutputs()];
        for (int i = 0; i < selected.length; i++) {
            accesses[selected[i]] = node.getOutput(i);
        }
        access = new DefaultReadAccessRow(accesses);
    }

    @Override
    public ReadAccessRow access() {
        return access;
    }

    @Override
    public boolean forward() {
        return node.forward();
    }

    @Override
    public void close() throws IOException {
        node.close();
    }
}
