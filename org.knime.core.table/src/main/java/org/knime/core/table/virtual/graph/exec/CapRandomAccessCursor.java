package org.knime.core.table.virtual.graph.exec;

import java.io.IOException;
import java.util.Arrays;

import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.row.RandomRowAccessible;
import org.knime.core.table.row.ReadAccessRow;

class CapRandomAccessCursor implements RandomRowAccessible.RandomAccessCursor<ReadAccessRow> {

    private final RandomAccessNodeImpConsumer node;

    private final CapReadAccessRow access;

    private final long numRows;

    private long nextRow;

    public CapRandomAccessCursor(final RandomAccessNodeImpConsumer node, final long numRows) {
        this.node = node;
        this.numRows = numRows;
        node.create();
        access = new CapReadAccessRow(node);
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

    static class CapReadAccessRow implements ReadAccessRow {

        private final ReadAccess[] accesses;

        CapReadAccessRow(final RandomAccessNodeImpConsumer node) {
            accesses = new ReadAccess[node.numOutputs()];
            Arrays.setAll(accesses, node::getOutput);
        }

        @Override
        public int size() {
            return accesses.length;
        }

        @Override
        public <A extends ReadAccess> A getAccess(int index) {
            return (A)accesses[index];
        }
    }
}
