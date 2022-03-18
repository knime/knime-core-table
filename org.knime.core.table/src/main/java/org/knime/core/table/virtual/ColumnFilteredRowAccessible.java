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
 *   Apr 4, 2021 (marcel): created
 */
package org.knime.core.table.virtual;

import java.io.IOException;
import java.util.Arrays;

import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.cursor.LookaheadCursor;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.row.Selection;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.virtual.spec.ColumnFilterTransformSpec;

/**
 * Implementation of the operation specified by {@link ColumnFilterTransformSpec}.
 *
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
final class ColumnFilteredRowAccessible implements LookaheadRowAccessible {

    private final LookaheadRowAccessible m_delegateTable;

    private final int[] m_columnIndices;

    private final ColumnarSchema m_schema;

    public ColumnFilteredRowAccessible(final RowAccessible delegate, final int[] selection) {
        m_columnIndices = selection;
        m_schema = ColumnarSchemas.filter(delegate.getSchema(), selection);
        m_delegateTable = RowAccessibles.toLookahead(delegate);
    }

    @Override
    public ColumnarSchema getSchema() {
        return m_schema;
    }

    @SuppressWarnings("resource") // Delegate cursor will be closed upon closing of the returned cursor.
    @Override
    public LookaheadCursor<ReadAccessRow> createCursor() {
        if ( m_schema.numColumns() == 0 ) {
            return new EmptyCursor(m_schema);
        } else {
            final Selection selection = Selection.all().retainColumns(m_columnIndices);
            return new ColumnFilteredCursor(m_delegateTable.createCursor(selection), m_columnIndices, m_schema);
        }
    }

    @SuppressWarnings("resource") // Delegate cursor will be closed upon closing of the returned cursor.
    @Override
    public LookaheadCursor<ReadAccessRow> createCursor(Selection selection) {
        if ( m_schema.numColumns() == 0 ) {
            return new EmptyCursor(m_schema);
        } else {
            if (selection.columns().allSelected(getSchema())) {
                selection = Selection.all().retainRows(selection.rows()).retainColumns(m_columnIndices);
            } else {
                final int[] selected = selection.columns().getSelected(0, m_schema.numColumns());
                final int[] cols = new int[selected.length];
                Arrays.setAll(cols, i -> m_columnIndices[selected[i]]);
                selection = Selection.all().retainRows(selection.rows()).retainColumns(cols);
            }
            return new ColumnFilteredCursor(m_delegateTable.createCursor(selection), m_columnIndices, m_schema);
        }
    }

    @Override
    public void close() throws IOException {
        m_delegateTable.close();
    }

    private static final class ColumnFilteredCursor implements LookaheadCursor<ReadAccessRow> {

        private final LookaheadCursor<ReadAccessRow> m_delegateCursor;

        private final ColumnFilteredReadAccessRow m_access;

        public ColumnFilteredCursor(final LookaheadCursor<ReadAccessRow> delegateCursor, final int[] columnIndices,
            final ColumnarSchema schema) {
            m_delegateCursor = delegateCursor;
            m_access = new ColumnFilteredReadAccessRow(delegateCursor.access(), columnIndices, schema);
        }

        @Override
        public ReadAccessRow access() {
            return m_access;
        }

        @Override
        public boolean forward() {
            return m_delegateCursor.forward();
        }

        @Override
        public boolean canForward() {
            return m_delegateCursor.canForward();
        }

        @Override
        public void close() throws IOException {
            m_delegateCursor.close();
        }

        private static final class ColumnFilteredReadAccessRow implements ReadAccessRow {

            private final ReadAccessRow m_delegateAccess;

            private final int[] m_columnIndices;

            private final ColumnarSchema m_schema;

            public ColumnFilteredReadAccessRow(final ReadAccessRow delegateAccess, final int[] columnIndices,
                final ColumnarSchema schema) {
                m_delegateAccess = delegateAccess;
                m_columnIndices = columnIndices;
                m_schema = schema;
            }

            @Override
            public int size() {
                return m_schema.numColumns();
            }

            @Override
            public <A extends ReadAccess> A getAccess(final int index) {
                return m_delegateAccess.getAccess(m_columnIndices[index]);
            }
        }
    }
}
