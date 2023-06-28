package org.knime.core.table.virtual.spec;

import org.knime.core.table.cursor.Cursor;
import org.knime.core.table.cursor.LookaheadCursor;
import org.knime.core.table.cursor.RandomAccessCursor;
import org.knime.core.table.row.LookaheadRowAccessible;
import org.knime.core.table.row.RandomRowAccessible;
import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.schema.ColumnarSchema;

/**
 * Meta-data describing a {@link RowAccessible source} of a {@code VirtualTable},
 * such as its {@code ColumnarSchema}, whether it supports LookaheadCursors,
 * whether the number of rows is known, etc.
 *
 * @author Tobias Pietzsch
 */
public class SourceTableProperties {

    /**
     * Cursor capabilities.
     */
    public enum CursorType {
        /**
         * At least normal {@link Cursor}.
         */
        BASIC(false, false),

        /**
         * At least {@link LookaheadCursor}.
         */
        LOOKAHEAD(true, false),

        /**
         * At least {@link RandomAccessCursor}.
         */
        RANDOMACCESS(true, true);

        /**
         * Derive cursor capabilities from the type of {@code source}
         *
         * @param source {@code RowAccessible} for which to determine cursor capabilities
         * @return the capabilities of {@code source}'s cursors.
         */
        public static CursorType of(final RowAccessible source) {
            if (source instanceof RandomRowAccessible) {
                return RANDOMACCESS;
            } else if (source instanceof LookaheadRowAccessible) {
                return LOOKAHEAD;
            } else {
                return BASIC;
            }
        }

        /**
         * Whether this {@code CursorType} supports {@link LookaheadCursor}.
         *
         * @return {@code true} if this cursor type supports {@code LookaheadCursor}
         */
        public boolean supportsLookahead() {
            return supportsLookahead;
        }

        /**
         * Whether this {@code CursorType} supports {@link RandomAccessCursor}.
         *
         * @return {@code true} if this source supports {@code RandomAccessCursor}s
         */
        public boolean supportsRandomAccess() {
            return supportsRandomAccess;
        }

        private final boolean supportsLookahead;

        private final boolean supportsRandomAccess;

        CursorType(final boolean lookahead, final boolean randomAccess) {
            this.supportsLookahead = lookahead;
            this.supportsRandomAccess = randomAccess;
        }
    }

    private final ColumnarSchema m_schema;

    private final CursorType m_cursorType;

    private final long m_numRows;

    public SourceTableProperties(final RowAccessible source) {
        this(source.getSchema(), CursorType.of(source), source.size());
    }

    public SourceTableProperties(final ColumnarSchema schema, final CursorType cursorType) {
        this(schema, cursorType, -1);
    }

    public SourceTableProperties(final ColumnarSchema schema, final CursorType cursorType, final long numRows) {
        this.m_schema = schema;
        this.m_cursorType = cursorType;
        this.m_numRows = numRows;
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
        return m_cursorType.supportsLookahead();
    }

    /**
     * Whether the source table supports {@link RandomAccessCursor}s, i.e., whether the
     * source table is a {@link RandomRowAccessible}.
     *
     * @return {@code true} if this source supports {@code RandomAccessCursor}s
     */
    public boolean supportsRandomAccess() {
        return m_cursorType.supportsRandomAccess();
    }

    /**
     * Get the number of rows in the source table, if it is known.
     * <p>
     * Some tables do not know how many rows they contain. Examples would be streaming tables, or virtual tables which
     * contain row filters, where the number of rows depends on the data in the rows. In this case, {@code size()<0}
     * indicates that the number of rows is unknown.
     * <p>
     * The default implementation returns {@code -1} to indicate that the number of rows is unknown.
     *
     * @return the number of rows, or a negative number if the number of rows is unknown.
     */
    public long numRows()
    {
        return m_numRows;
    }

    /**
     * Whether the source table knows how many rows it contains.
     * <p>
     * Some tables do not know how many rows they contain. Examples would be streaming tables, or virtual tables which
     * contain row filters, where the number of rows depends on the data in the rows.
     * <p>
     *
     * @return {@code true}, if the source table knows how many rows it contains.
     */
    public boolean knowsNumRows()
    {
        return m_numRows >= 0;
    }
}
