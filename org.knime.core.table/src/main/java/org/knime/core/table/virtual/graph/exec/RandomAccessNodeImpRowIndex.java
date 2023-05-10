package org.knime.core.table.virtual.graph.exec;

import static org.knime.core.table.schema.DataSpecs.LONG;

import java.io.IOException;

import org.knime.core.table.access.BufferedAccesses;
import org.knime.core.table.access.LongAccess.LongWriteAccess;
import org.knime.core.table.access.ReadAccess;

class RandomAccessNodeImpRowIndex implements RandomAccessNodeImp {

    private final RandomAccessNodeImp predecessor;

    /**
     * offset to add to row index.
     */
    private final long offset;

    private final LongWriteAccess access;

    RandomAccessNodeImpRowIndex(final RandomAccessNodeImp predecessor, final long offset) {
        this.predecessor = predecessor;
        this.offset = offset;
        access = (LongWriteAccess)BufferedAccesses.createBufferedAccess(LONG);
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
    public void moveTo(final long row) {
        // NB no bounds checking here, this is done in CapRandomAccessCursor
        predecessor.moveTo(row);
        access.setLongValue(row + offset);
    }

    @Override
    public void close() throws IOException {
        predecessor.close();
    }
}
