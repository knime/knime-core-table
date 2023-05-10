package org.knime.core.table.virtual.graph.exec;

import java.io.IOException;

import org.knime.core.table.access.ReadAccess;

class RandomAccessNodeImpSlice implements RandomAccessNodeImp {

    private final RandomAccessNodeImp predecessor;

    /**
     * Index of the first row (inclusive) to include in the slice.
     * Row indices are wrt to the rows provided by the predecessor.
     */
    private final long m_from;

    RandomAccessNodeImpSlice(final RandomAccessNodeImp predecessor, final long from) {
        this.predecessor = predecessor;
        m_from = from;
    }

    @Override
    public ReadAccess getOutput(final int i) {
        // SLICE doesn't have inputs or outputs
        throw new UnsupportedOperationException();
    }

    @Override
    public void create() {
        predecessor.create();
    }

    @Override
    public void moveTo(final long row) {
        // NB no bounds checking here, this is done in CapRandomAccessCursor
        predecessor.moveTo(m_from + row);
    }

    @Override
    public void close() throws IOException {
        predecessor.close();
    }
}
