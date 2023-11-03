package org.knime.core.table.virtual.graph.exec;

import java.io.IOException;

import org.knime.core.table.cursor.RandomAccessCursor;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.virtual.graph.exec.CapRowAccessible.CapCursorData;

class CapRandomAccessCursor implements RandomAccessCursor<ReadAccessRow> {

    private final RandomAccessNodeImpConsumer node;

    private final ReadAccessRow access;

    private final long numRows;

    private long nextRow;

    CapRandomAccessCursor(final CapCursorData data) {
        node = data.assembleRandomAccessConsumer();
        node.create();
        access = data.createReadAccessRow(node::getOutput);
        numRows = data.numRows();
        nextRow = 0;
    }

    @Override
    public ReadAccessRow access() {
        return access;
    }

    @Override
    public boolean forward() {
        if ( canForward() ) {
            node.moveTo(nextRow++);
            return true;
        }
        return false;
    }

    @Override
    public boolean canForward() {
        return nextRow < numRows;
    }

    @Override
    public void moveTo(long row) {
        if (row < 0 || row >= numRows)
            throw new IndexOutOfBoundsException();
        node.moveTo(row);
    }

    @Override
    public void close() throws IOException {
        node.close();
    }
}
