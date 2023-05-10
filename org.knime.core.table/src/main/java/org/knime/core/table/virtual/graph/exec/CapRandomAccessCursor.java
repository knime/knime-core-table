package org.knime.core.table.virtual.graph.exec;

import java.io.IOException;

import org.knime.core.table.row.DefaultReadAccessRow;
import org.knime.core.table.cursor.RandomAccessCursor;
import org.knime.core.table.row.ReadAccessRow;

class CapRandomAccessCursor implements RandomAccessCursor<ReadAccessRow> {

    private final RandomAccessNodeImpConsumer node;

    private final ReadAccessRow access;

    private final long numRows;

    private long nextRow;

    public CapRandomAccessCursor(final RandomAccessNodeImpConsumer node, final long numRows) {
        this.node = node;
        this.numRows = numRows;
        node.create();
        access = new DefaultReadAccessRow(node.numOutputs(), node::getOutput);
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
