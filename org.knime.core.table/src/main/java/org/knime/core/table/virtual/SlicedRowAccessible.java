package org.knime.core.table.virtual;

import java.io.IOException;

import org.knime.core.table.cursor.LookaheadCursor;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.row.Selection;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.virtual.spec.SliceTransformSpec;

/**
 * Implementation of the operation specified by {@link SliceTransformSpec}.
 *
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */

/*
 * TODO in case of batch read stores we can be much smarter about "slicing" data, as we have random access on rows.
 */
final class SlicedRowAccessible implements LookaheadRowAccessible {

    private final LookaheadRowAccessible m_delegateTable;

    private final long m_from;

    private final long m_to;

    private final long m_size;

    public SlicedRowAccessible(final RowAccessible tableToSlice, final long from, final long to) {
        m_delegateTable = RowAccessibles.toLookahead(tableToSlice);
        m_from = from;
        m_to = to;

        final long s = m_delegateTable.size();
        m_size = s < 0 ? s : Math.max(0, Math.min(s, to) - from);
    }

    @Override
    public ColumnarSchema getSchema() {
        return m_delegateTable.getSchema();
    }

    @Override
    public LookaheadCursor<ReadAccessRow> createCursor() {
        return m_delegateTable.createCursor(Selection.all().retainRows(m_from, m_to));
    }

    @Override
    public LookaheadCursor<ReadAccessRow> createCursor(final Selection selection) {
        return m_delegateTable.createCursor(Selection.all().retainRows(m_from, m_to).retain(selection));
    }

    @Override
    public long size() {
        return m_size;
    }

    @Override
    public void close() throws IOException {
        m_delegateTable.close();
    }
}
