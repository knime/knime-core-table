package org.knime.core.table.virtual.graph.exec;

import java.io.IOException;

import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.cursor.RandomAccessCursor;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.row.Selection;

class RandomAccessNodeImpSource implements RandomAccessNodeImp {
    private final RowAccessible accessible;

    private final int[] cols;

    private final Selection selection;

    private final ReadAccess[] outputs;

    private RandomAccessCursor<ReadAccessRow> cursor;

    RandomAccessNodeImpSource(//
        final RowAccessible accessible, //
        final int[] cols, //
        final long fromRow, //
        final long toRow) {

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
        cursor = (RandomAccessCursor<ReadAccessRow>)accessible.createCursor(selection);
        for (int i = 0; i < outputs.length; i++) {
            outputs[i] = cursor.access().getAccess(cols[i]);
        }
    }

    @Override
    public void moveTo(final long row) {
        cursor.moveTo(row);
    }

    @Override
    public void close() throws IOException {
        cursor.close();
    }
}
