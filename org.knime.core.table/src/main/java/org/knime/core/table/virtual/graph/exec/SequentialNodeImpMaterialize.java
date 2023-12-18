package org.knime.core.table.virtual.graph.exec;

import java.io.IOException;

import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.cursor.Cursor;
import org.knime.core.table.row.DefaultReadAccessRow;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.RowWriteAccessible;
import org.knime.core.table.row.WriteAccessRow;

class SequentialNodeImpMaterialize implements SequentialNodeImp {

    private final RowWriteAccessible accessible;

    private final AccessImp[] inputs;

    private final SequentialNodeImp predecessor;

    private boolean m_canForward = true;

    SequentialNodeImpMaterialize(final RowWriteAccessible accessible, final AccessImp[] inputs, final SequentialNodeImp predecessor) {
        this.accessible = accessible;
        this.inputs = inputs;
        this.predecessor = predecessor;
    }

    @Override
    public ReadAccess getOutput(final int i) {
        throw new IndexOutOfBoundsException();
    }

    @Override
    public void create() {
        predecessor.create();
    }

    @Override
    public boolean forward() {
        if (m_canForward) {
            m_canForward = false;
        } else {
            throw new IllegalStateException("Forward can only be called once for materialize.");
        }
        final Cursor<WriteAccessRow> writeCursor = accessible.getWriteCursor();
        final WriteAccessRow writeRow = writeCursor.access();
        final ReadAccessRow readRow = new DefaultReadAccessRow(inputs.length, i -> inputs[i].getReadAccess());
        while (predecessor.forward()) {
            writeCursor.forward();
            writeRow.setFrom(readRow);
        }
        return true;
    }

    @Override
    public boolean canForward() {
        return m_canForward;
    }

    @Override
    public void close() throws IOException {
        predecessor.close();
    }
}
