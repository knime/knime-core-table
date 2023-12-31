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
 *   Created on Apr 20, 2021 by Adrian Nembach
 */
package org.knime.core.table.row;

import java.io.Closeable;

import org.knime.core.table.cursor.Cursor;
import org.knime.core.table.schema.ColumnarSchema;

/**
 * A RowAccessible has a {@link ColumnarSchema} and can create {@link Cursor Cursors} that allow iterating over it in a
 * row-wise fashion.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 * @noreference This class is not intended to be referenced by clients.
 * @noimplement This class is not intended to be implemented by clients.
 */
public interface RowAccessible extends Closeable {

    /**
     * @return the {@link ColumnarSchema} of the rows in this {@link RowAccessible}
     */
    ColumnarSchema getSchema();

    /**
     * Creates a fresh {@link Cursor} over the rows in this {@link RowAccessible}.
     *
     * @return a new {@link Cursor} over the rows in this {@link RowAccessible}
     */
    Cursor<ReadAccessRow> createCursor();

    /**
     * Creates a fresh {@link Cursor} over the rows and columns of this {@link RowAccessible} specified in the given
     * {@code selection}.
     * <p>
     * If {@code selection} only selects a subset of columns, the returned {@code Cursor} may specify {@code null}
     * accesses for the unselected columns.
     * <p>
     * If {@code selection} only selects a subset of rows, then the returned {@code Cursor} starts at
     * {@code selection.rows().fromIndex()}, that is, the first {@code forward()} moves it to row <em>fromIndex</em>.
     * The {@code Cursor} ends at {@code selection.rows().toIndex()}, that is, if positioned at row <em>toIndex-1</em>,
     * subsequent {@code forward()} will return {@code false}.
     *
     * @param selection the selected columns and row range
     * @return a new {@link Cursor} over the selected rows and columns in this {@link RowAccessible}
     */
    Cursor<ReadAccessRow> createCursor(Selection selection);

    /**
     * Get the number of rows in this {@code RowAccessible}, if it is known.
     * <p>
     * Some tables do not know how many rows they contain. Examples would be streaming tables, or virtual tables which
     * contain row filters, where the number of rows depends on the data in the rows. In this case, {@code size()<0}
     * indicates that the number of rows is unknown.
     * <p>
     * The default implementation returns {@code -1} to indicate that the number of rows is unknown.
     *
     * @return the number of rows, or a negative number if the number of rows is unknown.
     */
    default long size() {
        return -1;
    }
}
