package org.knime.core.table.virtual.graph.exec;

import java.io.IOException;

import org.knime.core.table.access.ReadAccess;

class SequentialNodeImpSlice implements SequentialNodeImp {
    private final SequentialNodeImp predecessor;

    /**
     * Index of the first row (inclusive) to include in the slice.
     * Row indices are wrt to the rows provided by the predecessor.
     */
    private final long m_from;

    /**
     * Index of the last row (exclusive) to include in the slice.
     * Row indices are wrt to the rows provided by the predecessor.
     */
    private final long m_to;

    /**
     * Index of the row that will be provided after the next call to
     * to {@code forward()}.
     * <p>
     * For example, if the slice starts at {@code m_from=0}, then before the
     * first {@code forward()} it would be 0. Then {@code forward()} provides
     * values for row 0, and increments to {@code m_nextRowIndex=1}. Row indices
     * are wrt to the rows provided by the predecessor.
     */
    private long m_nextRowIndex;

    SequentialNodeImpSlice(final SequentialNodeImp predecessor, final long from, final long to) {
        this.predecessor = predecessor;
        m_from = from;
        m_to = to;
        m_nextRowIndex = 0;
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
    public boolean forward() {
        forwardToStart();
        if (m_nextRowIndex < m_to) {
            m_nextRowIndex++;
            return predecessor.forward();
        } else {
            return false;
        }
    }

    @Override
    public boolean canForward() {
        forwardToStart();
        if (m_nextRowIndex < m_to) {
            return predecessor.canForward();
        } else {
            return false;
        }
    }

    private void forwardToStart() {
        while (m_nextRowIndex < m_from) {
            predecessor.forward();
            m_nextRowIndex++;
        }
    }

    @Override
    public void close() throws IOException {
        predecessor.close();
    }
}
