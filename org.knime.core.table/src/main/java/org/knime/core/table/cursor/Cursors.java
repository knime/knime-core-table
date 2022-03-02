/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME AG, Zurich, Switzerland
 *
 * You may not modify, publish, transmit, transfer or sell, reproduce,
 * create derivative works from, distribute, perform, display, or in
 * any way exploit any of the content, in whole or in part, except as
 * otherwise expressly permitted in writing by the copyright owner or
 * as specified in the license file distributed with this product.
 *
 * If you have any questions please contact the copyright holder:
 * website: www.knime.com
 * email: contact@knime.com
 * ---------------------------------------------------------------------
 *
 * History
 *   Created on Jul 23, 2021 by Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
package org.knime.core.table.cursor;

import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.Selection.ColumnSelection;
import org.knime.core.table.schema.ColumnarSchema;

/**
 * Provides utilities for working with {@link Cursor Cursors} and {@link LookaheadCursor LookaheadCursors}.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class Cursors {

    /**
     * Converts the provided Cursor into a LookaheadCursor by reading one row ahead or just returns the provided cursor
     * if it already is a LookaheadCursor.
     *
     * @param schema defines the column types
     * @param cursor the Cursor to convert
     * @return a LookaheadCursor based on the provided Cursor (or itself it is already a LookaheadCursor)
     */
    public static LookaheadCursor<ReadAccessRow> toLookahead(final ColumnarSchema schema,
        final Cursor<ReadAccessRow> cursor) {
        if (cursor instanceof LookaheadCursor) {
            return (LookaheadCursor<ReadAccessRow>)cursor;
        } else {
            return new BufferingLookaheadCursor(schema, cursor);
        }
    }

    /**
     * Converts the provided Cursor into a LookaheadCursor by reading one row ahead or just returns the provided cursor
     * if it already is a LookaheadCursor.
     *
     * @param schema defines the column types
     * @param cursor the Cursor to convert
     * @param columnSelection column selection (of both, {@code cursor} and the returned LookaheadCursor).
     * @return a LookaheadCursor based on the provided Cursor (or itself it is already a LookaheadCursor)
     */
    public static LookaheadCursor<ReadAccessRow> toLookahead(final ColumnarSchema schema,
        final Cursor<ReadAccessRow> cursor, final ColumnSelection columnSelection) {
        if (cursor instanceof LookaheadCursor) {
            return (LookaheadCursor<ReadAccessRow>)cursor;
        } else {
            return new BufferingLookaheadCursor(schema, cursor, columnSelection);
        }
    }

    private Cursors() {
        // static utility class
    }
}
