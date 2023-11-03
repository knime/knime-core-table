package org.knime.core.table.virtual.graph.exec;

import java.io.IOException;

import org.knime.core.table.cursor.Cursor;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.virtual.graph.exec.CapRowAccessible.CapCursorData;

class CapCursor implements Cursor<ReadAccessRow> {

    final NodeImpConsumer node;

    private final ReadAccessRow access;

    public CapCursor(final CapCursorData data)
    {
        node = data.assembleConsumer();
        node.create();
        access = data.createReadAccessRow(node::getOutput);
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
