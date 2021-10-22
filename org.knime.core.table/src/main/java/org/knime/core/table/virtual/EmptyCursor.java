package org.knime.core.table.virtual;

import org.knime.core.table.access.MissingAccesses;
import org.knime.core.table.cursor.LookaheadCursor;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.schema.ColumnarSchema;

/**
 * Implementation of an empty cursor.
 *
 * @author Christian Birkhold, KNIME GmbH, Konstanz, Germany
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class EmptyCursor implements LookaheadCursor<ReadAccessRow> {

    private final ReadAccessRow m_missingRow;

    /**
     * Constructor.
     *
     * @param schema of the table
     */
    public EmptyCursor(final ColumnarSchema schema) {
        m_missingRow = MissingAccesses.getMissingReadAccessRow(schema);
    }

    @Override
    public ReadAccessRow access() {
        return m_missingRow;
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

}