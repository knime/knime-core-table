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
