package org.knime.core.table.virtual.graph.exec;

import java.io.IOException;

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
