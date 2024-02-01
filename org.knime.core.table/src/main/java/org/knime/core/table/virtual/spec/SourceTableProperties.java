/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ---------------------------------------------------------------------
 */
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
     * Get the {@link CursorType} supported by the source table.
     *
     * @return the {@link CursorType} supported by the source table
     */
    public CursorType cursorType() {
        return m_cursorType;
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
