package org.knime.core.table.virtual.graph.exec;

import java.io.IOException;

import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.cursor.Cursor;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.RowAccessible;

class NodeImpSource implements NodeImp {
    private final RowAccessible accessible;

    private final int[] cols;

    private final ReadAccess[] outputs;

    private Cursor<ReadAccessRow> cursor;

    public NodeImpSource(RowAccessible accessible, int[] cols) {
        this.accessible = accessible;
        this.cols = cols;
        outputs = new ReadAccess[cols.length];
    }

    @Override
    public ReadAccess getOutput(int i) {
        return outputs[i];
    }

    @Override
    public void create() {
        cursor = accessible.createCursor();
        for (int i = 0; i < outputs.length; i++) {
            outputs[i] = cursor.access().getAccess(cols[i]);
        }
    }

    @Override
    public boolean forward() {
        return cursor.forward();
    }

    @Override
    public void close() throws IOException {
        cursor.close();
    }
}
