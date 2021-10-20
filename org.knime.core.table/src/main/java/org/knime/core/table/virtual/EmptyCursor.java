package org.knime.core.table.virtual;

import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.cursor.LookaheadCursor;
import org.knime.core.table.row.ReadAccessRow;

/**
 * Implementation of an empty cursor.
 *
 * @author Christian Birkhold, KNIME GmbH, Konstanz, Germany
 */
public final class EmptyCursor implements LookaheadCursor<ReadAccessRow> {

    public static final EmptyCursor INSTANCE = new EmptyCursor();

    @Override
    public ReadAccessRow access() {
        return EmptyReadAccessRow.INSTANCE;
    }

    @Override
    public boolean forward() {
        return false;
    }

    @Override
    public boolean canForward() {
        return false;
    }

    @Override
    public void close() {
        // Nothing to do.
    }

    private static final class EmptyReadAccessRow implements ReadAccessRow {

        @SuppressWarnings("hiding")
        private static final EmptyReadAccessRow INSTANCE = new EmptyReadAccessRow();

        @Override
        public int size() {
            return 0;
        }

        @Override
        public <A extends ReadAccess> A getAccess(final int index) {
            throw new IndexOutOfBoundsException(index);
        }
    }

}