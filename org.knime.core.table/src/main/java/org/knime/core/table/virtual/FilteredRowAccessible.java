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
 *   Apr 12, 2021 (marcel): created
 */
package org.knime.core.table.virtual;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Stream;

import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.cursor.Cursor;
import org.knime.core.table.cursor.Cursors;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.row.Selection;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.virtual.spec.RowFilterTransformSpec;
import org.knime.core.table.virtual.spec.RowFilterTransformSpec.RowFilter;

/**
 * Implementation of the operation specified by {@link RowFilterTransformSpec}.
 *
 * @author Tobias Pietzsch
 */
class FilteredRowAccessible implements RowAccessible {

    private final RowAccessible m_delegateTable;

    private final int[] m_inputs;

    private final RowFilterTransformSpec.RowFilter m_filter;

    public FilteredRowAccessible(final RowAccessible tableToFilter, final int[] inputColumns, final RowFilter filter) {
        m_delegateTable = tableToFilter;
        m_inputs = inputColumns;
        m_filter = filter;
    }

    @Override
    public ColumnarSchema getSchema() {
        return m_delegateTable.getSchema();
    }

    @SuppressWarnings("resource") // Delegate cursor will be closed upon closing of the returned cursor.
    @Override
    public Cursor<ReadAccessRow> createCursor() {
        return new FilteredCursor(m_delegateTable.createCursor(), m_inputs,  m_filter);
    }

    @SuppressWarnings("resource") // Delegate cursor will be closed upon closing of the returned cursor.
    @Override
    public Cursor<ReadAccessRow> createCursor(final Selection selection) {
        // Handle column selection first. We must make sure that (at least) m_inputs are
        // selected, because we need those to evaluate the filter.
        final Cursor<ReadAccessRow> delegateCursor;
        if (selection.columns().allSelected(getSchema())) {
            delegateCursor = m_delegateTable.createCursor();
        } else {
            final int[] cols = Stream.concat( //
                Arrays.stream(selection.columns().getSelected()).boxed(), //
                Arrays.stream(m_inputs).boxed() //
            ).distinct().mapToInt(Integer::intValue).toArray();
            delegateCursor = m_delegateTable.createCursor(Selection.all().retainColumns(cols));
        }
        final FilteredCursor filteredCursor = new FilteredCursor(delegateCursor, m_inputs, m_filter);

        // For row selection, we can only handle this after filtering, so we have to start
        // from the beginning of the table and skip selection.fromRowIndex() valid rows.
        if (selection.rows().allSelected()) {
            return filteredCursor;
        } else {
            final long from = selection.rows().fromIndex();
            final long to = selection.rows().toIndex();
            return new SlicedCursor(Cursors.toLookahead(getSchema(), filteredCursor, selection.columns()), from, to);
        }
    }

    @Override
    public void close() throws IOException {
        m_delegateTable.close();
    }

    private static final class FilteredCursor implements Cursor<ReadAccessRow> {

        private final Cursor<ReadAccessRow> m_delegateCursor;
        private final RowFilterTransformSpec.RowFilter m_filter;

        private final ReadAccess[] m_inputs;

        public FilteredCursor(final Cursor<ReadAccessRow> delegateCursor, final int[] inputColumns, final RowFilterTransformSpec.RowFilter filter) {
            m_delegateCursor = delegateCursor;
            m_filter = filter;

            m_inputs = new ReadAccess[inputColumns.length];
            Arrays.setAll(m_inputs, i -> m_delegateCursor.access().getAccess(inputColumns[i]));
        }

        @Override
        public ReadAccessRow access() {
            return m_delegateCursor.access();
        }

        @Override
        public boolean forward() {
            while (m_delegateCursor.forward()) {
                if (m_filter.test(m_inputs)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public void close() throws IOException {
            m_delegateCursor.close();
        }
    }
}
