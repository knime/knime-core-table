package org.knime.core.table.virtual.graph.exec;

import java.io.IOException;

import org.knime.core.table.access.ReadAccess;

class NodeImpSlice implements NodeImp {
    private final NodeImp predecessor;

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

    private long m_currentIndex;

    public NodeImpSlice(final NodeImp predecessor, final long from, final long to) {
        this.predecessor = predecessor;
        m_from = from;
        m_to = to;
        m_currentIndex = 0;
    }

    @Override
    public ReadAccess getOutput(int i) {
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
        if (m_currentIndex < m_to) {
            m_currentIndex++;
            return predecessor.forward();
        } else {
            return false;
        }
    }

    @Override
    public boolean canForward() {
        forwardToStart();
        if (m_currentIndex < m_to) {
            return predecessor.canForward();
        } else {
            return false;
        }
    }

    private void forwardToStart() {
        while (m_currentIndex < m_from) {
            predecessor.forward();
            m_currentIndex++;
        }
    }

    @Override
    public void close() throws IOException {
        predecessor.close();
    }
}
