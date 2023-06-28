package org.knime.core.table.virtual.graph.exec;

import java.io.IOException;
import java.util.Arrays;

import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.cursor.Cursor;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.RowWriteAccessible;
import org.knime.core.table.row.WriteAccessRow;

class NodeImpMaterialize implements NodeImp {

    private final RowWriteAccessible accessible;

    private final AccessImp[] inputs;

    private final NodeImp predecessor;

    private boolean m_canForward = true;

    NodeImpMaterialize(final RowWriteAccessible accessible, final AccessImp[] inputs, final NodeImp predecessor) {
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
        final ReadRow readRow = new ReadRow(inputs);
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

    private static class ReadRow implements ReadAccessRow {

        private final ReadAccess[] accesses;

        ReadRow(final AccessImp[] inputs) {
            accesses = new ReadAccess[inputs.length];
            Arrays.setAll(accesses, i -> inputs[i].getReadAccess());
        }

        @Override
        public int size() {
            return accesses.length;
        }

        @Override
        public <A extends ReadAccess> A getAccess(final int index) {
            return (A)accesses[index];
        }
    }
}
