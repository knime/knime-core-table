package org.knime.core.table.virtual;

import java.io.IOException;

import org.knime.core.table.cursor.LookaheadCursor;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.Selection.RowRangeSelection;;

/**
 * Cursor that slices a row range from another cursor.
 *
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
final class SlicedCursor implements LookaheadCursor<ReadAccessRow> {

    private final LookaheadCursor<ReadAccessRow> m_delegateCursor;

    private final long m_from;

    private final long m_to;

    private long m_currentIndex;

    public SlicedCursor(final LookaheadCursor<ReadAccessRow> delegateCursor, final long from, final long to) {
        m_delegateCursor = delegateCursor;
        m_from = from;
        m_to = to;
    }

    public SlicedCursor(final LookaheadCursor<ReadAccessRow> delegateCursor, final RowRangeSelection rowRange) {
        this(delegateCursor, rowRange.fromIndex(), rowRange.toIndex());
    }

    @Override
    public ReadAccessRow access() {
        return m_delegateCursor.access();
    }

    @Override
    public ReadAccessRow pinAccess() {
        return m_delegateCursor.pinAccess();
    }

    @Override
    public boolean forward() {
        if (canForward()) {
            final boolean forwarded = m_delegateCursor.forward();
            m_currentIndex++;
            return forwarded;
        } else {
            return false;
        }
    }

    @Override
    public boolean canForward() {
        moveIndex();
        if (m_currentIndex < m_to) {
            return m_delegateCursor.canForward();
        } else {
            return false;
        }
    }

    @Override
    public void close() throws IOException {
        m_delegateCursor.close();
    }

    private void moveIndex() {
        while (m_currentIndex < m_from) {
            m_delegateCursor.forward();
            m_currentIndex++;
        }
    }
}
