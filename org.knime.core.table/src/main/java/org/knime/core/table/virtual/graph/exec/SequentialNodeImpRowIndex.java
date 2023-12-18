package org.knime.core.table.virtual.graph.exec;

import static org.knime.core.table.schema.DataSpecs.LONG;

import java.io.IOException;

import org.knime.core.table.access.BufferedAccesses;
import org.knime.core.table.access.LongAccess.LongWriteAccess;
import org.knime.core.table.access.ReadAccess;

class SequentialNodeImpRowIndex implements SequentialNodeImp {

    private final SequentialNodeImp predecessor;

    private final LongWriteAccess access;

    /**
     * Index of the row that will be provided after the next call to {@code
     * forward()}.
     */
    private long m_nextRowIndex;

    SequentialNodeImpRowIndex(final SequentialNodeImp predecessor, final long offset) {
        this.predecessor = predecessor;
        access = (LongWriteAccess)BufferedAccesses.createBufferedAccess(LONG);
        m_nextRowIndex = offset;
    }

    @Override
    public ReadAccess getOutput(final int i) {
        return (ReadAccess)access;
    }

    @Override
    public void create() {
        predecessor.create();
    }

    @Override
    public boolean forward() {
        if (predecessor.forward()) {
            access.setLongValue(m_nextRowIndex++);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean canForward() {
        return predecessor.canForward();
    }

    @Override
    public void close() throws IOException {
        predecessor.close();
    }
}
