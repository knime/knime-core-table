package org.knime.core.table.virtual.graph.exec;

import static org.knime.core.table.schema.DataSpecs.LONG;

import java.io.IOException;

import org.knime.core.table.access.BufferedAccesses;
import org.knime.core.table.access.LongAccess.LongWriteAccess;
import org.knime.core.table.access.ReadAccess;

class NodeImpRowIndex implements NodeImp {

    private final NodeImp predecessor;

    private final LongWriteAccess access;

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

    public NodeImpRowIndex(final NodeImp predecessor) {
        this.predecessor = predecessor;
        access = (LongWriteAccess)BufferedAccesses.createBufferedAccess(LONG);
        m_nextRowIndex = 0;
    }

    @Override
    public ReadAccess getOutput(int i) {
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
