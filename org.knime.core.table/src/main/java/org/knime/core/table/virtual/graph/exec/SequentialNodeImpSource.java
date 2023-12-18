package org.knime.core.table.virtual.graph.exec;

import java.io.IOException;

import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.cursor.Cursor;
import org.knime.core.table.cursor.LookaheadCursor;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.row.Selection;

class SequentialNodeImpSource implements SequentialNodeImp {
    private final RowAccessible accessible;

    private final int[] cols;

    private final Selection selection;

    private final ReadAccess[] outputs;

    private Cursor<ReadAccessRow> cursor;

    private LookaheadCursor<ReadAccessRow> lookahead;

    SequentialNodeImpSource(final RowAccessible accessible, final int[] cols, final long fromRow, final long toRow) {
        this.accessible = accessible;
        this.cols = cols;
        this.selection = Selection.all().retainColumns(cols).retainRows(fromRow, toRow);
        outputs = new ReadAccess[cols.length];
    }

    @Override
    public ReadAccess getOutput(final int i) {
        return outputs[i];
    }

    @Override
    public void create() {
        cursor = accessible.createCursor(selection);
        for (int i = 0; i < outputs.length; i++) {
            outputs[i] = cursor.access().getAccess(cols[i]);
        }
        if (cursor instanceof LookaheadCursor) {
            lookahead = (LookaheadCursor<ReadAccessRow>)cursor;
        }
    }

    @Override
    public boolean forward() {
        return cursor.forward();
    }

    @Override
    public boolean canForward() {
        return lookahead.canForward();
    }

    @Override
    public void close() throws IOException {
        cursor.close();
    }
}
