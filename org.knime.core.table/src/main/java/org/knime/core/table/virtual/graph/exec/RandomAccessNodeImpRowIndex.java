package org.knime.core.table.virtual.graph.exec;

import static org.knime.core.table.schema.DataSpecs.LONG;

import java.io.IOException;

import org.knime.core.table.access.BufferedAccesses;
import org.knime.core.table.access.LongAccess.LongWriteAccess;
import org.knime.core.table.access.ReadAccess;

class RandomAccessNodeImpRowIndex implements RandomAccessNodeImp {

    private final RandomAccessNodeImp predecessor;

    private final LongWriteAccess access;

    public RandomAccessNodeImpRowIndex(final RandomAccessNodeImp predecessor) {
        this.predecessor = predecessor;
        access = (LongWriteAccess)BufferedAccesses.createBufferedAccess(LONG);
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
    public void moveTo(final long row) {
        // NB no bounds checking here, because that is done at the sink NodeImp
        predecessor.moveTo(row);
        access.setLongValue(row);
    }

    @Override
    public void close() throws IOException {
        predecessor.close();
    }
}
