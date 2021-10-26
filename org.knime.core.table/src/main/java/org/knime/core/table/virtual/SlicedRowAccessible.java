package org.knime.core.table.virtual;

import java.io.IOException;

import org.knime.core.table.cursor.LookaheadCursor;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.RowAccessible;
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

    public SlicedRowAccessible(final RowAccessible tableToSlice, final long from, final long to) {
        m_delegateTable = RowAccessibles.toLookahead(tableToSlice);
        m_from = from;
        m_to = to;
    }

    @Override
    public ColumnarSchema getSchema() {
        return m_delegateTable.getSchema();
    }

    @SuppressWarnings("resource") // Delegate cursor will be closed upon closing of the returned cursor.
    @Override
    public LookaheadCursor<ReadAccessRow> createCursor() {
        return new SlicedCursor(m_delegateTable.createCursor(), m_from, m_to);
    }

    @Override
    public void close() throws IOException {
        m_delegateTable.close();
    }

    private static final class SlicedCursor implements LookaheadCursor<ReadAccessRow> {

        private final LookaheadCursor<ReadAccessRow> m_delegateCursor;

        private final long m_from;

        private final long m_to;

        private long m_currentIndex;

        public SlicedCursor(final LookaheadCursor<ReadAccessRow> delegateCursor, final long from, final long to) {
            m_delegateCursor = delegateCursor;
            m_from = from;
            m_to = to;
        }

        @Override
        public ReadAccessRow access() {
            return m_delegateCursor.access();
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
}
