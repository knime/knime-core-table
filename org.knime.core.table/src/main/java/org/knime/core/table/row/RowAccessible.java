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

}
