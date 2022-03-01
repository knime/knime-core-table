package org.knime.core.table.virtual.spec;

import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.schema.ColumnarSchema;

/**
 * Meta-data describing a {@link RowAccessible source} of a {@code VirtualTable},
 * such as its {@code ColumnarSchema}, whether it supports LookAheadCursors,
 * whether the number of rows is known, etc.
 *
 * @author Tobias Pietzsch
 */
public class SourceTableProperties {

    private final ColumnarSchema m_schema;

    public SourceTableProperties(final ColumnarSchema schema) {
        this.m_schema = schema;
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

    // TODO
    //   long numRows
    //   boolean knowsNumRows
    //   boolean supportsLookAhead
}
