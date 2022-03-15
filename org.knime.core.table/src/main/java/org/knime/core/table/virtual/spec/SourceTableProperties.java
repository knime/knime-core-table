package org.knime.core.table.virtual.spec;

import org.knime.core.table.cursor.LookaheadCursor;
import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.virtual.LookaheadRowAccessible;

/**
 * Meta-data describing a {@link RowAccessible source} of a {@code VirtualTable},
 * such as its {@code ColumnarSchema}, whether it supports LookaheadCursors,
 * whether the number of rows is known, etc.
 *
 * @author Tobias Pietzsch
 */
public class SourceTableProperties {

    private final ColumnarSchema m_schema;

    private final boolean m_lookahead;

    public SourceTableProperties(final RowAccessible source) {
        this(source.getSchema(), source instanceof LookaheadRowAccessible);
    }

    public SourceTableProperties(final ColumnarSchema schema, final boolean lookahead) {
        this.m_schema = schema;
        this.m_lookahead = lookahead;
    }

    /**
     * Get the columnar schema of the source.
     *
     * @return the columnar schema of the source
     */
    public ColumnarSchema getSchema() {
        return m_schema;
    }

    /**
     * Whether the source table supports efficient row range slicing. This indicates
     * whether the source does row ranges in a more clever way than the default of
     * making a cursor wrapper that skips elements before and after the row range.
     *
     * @return {@code true} if this source supports efficient slicing
     */
    public boolean supportsRowRange() {
        return true; // TODO
    }

    /**
     * Whether the source table supports {@link LookaheadCursor}s, i.e., whether the
     * source table is a {@link LookaheadRowAccessible}.
     *
     * @return {@code true} if this source supports {@code LookaheadCursor}s
     */
    public boolean supportsLookahead() {
        return m_lookahead;
    }

    // TODO
    //   long numRows
    //   boolean knowsNumRows
}
