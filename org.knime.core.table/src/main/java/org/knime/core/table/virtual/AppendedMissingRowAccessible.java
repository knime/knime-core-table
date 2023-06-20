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
import java.util.List;

import org.knime.core.table.access.MissingAccesses;
import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.cursor.LookaheadCursor;
import org.knime.core.table.row.LookaheadRowAccessible;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.row.Selection;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.virtual.spec.AppendMissingValuesTransformSpec;

/**
 * Implementation of the operation specified by {@link AppendMissingValuesTransformSpec}.
 *
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
final class AppendedMissingRowAccessible implements LookaheadRowAccessible {

    private final LookaheadRowAccessible m_delegateTable;

    private final ColumnarSchema m_schema;

    public AppendedMissingRowAccessible(final RowAccessible tableToAppendTo, final ColumnarSchema missingSchema) {
        m_delegateTable = RowAccessibles.toLookahead(tableToAppendTo);
        m_schema = ColumnarSchemas.append(List.of(tableToAppendTo.getSchema(), missingSchema));
    }

    @Override
    public ColumnarSchema getSchema() {
        return m_schema;
    }

    @SuppressWarnings("resource") // Delegate cursor will be closed upon closing of the returned cursor.
    @Override
    public LookaheadCursor<ReadAccessRow> createCursor() {
        return new AppendedMissingValuesCursor(m_delegateTable.createCursor(), m_schema);
    }

    @Override
    public LookaheadCursor<ReadAccessRow> createCursor(Selection selection) {
        if (!selection.columns().allSelected(m_schema)) {
            final int[] selected = selection.columns().getSelected(0, m_delegateTable.getSchema().numColumns());
            selection = Selection.all().retainRows(selection.rows()).retainColumns(selected);
        }
        return new AppendedMissingValuesCursor(m_delegateTable.createCursor(selection), m_schema);
    }

    @Override
    public long size() {
        return m_delegateTable.size();
    }

    @Override
    public void close() throws IOException {
        m_delegateTable.close();
    }

    private static final class AppendedMissingValuesCursor implements LookaheadCursor<ReadAccessRow> {

        private final LookaheadCursor<ReadAccessRow> m_delegateCursor;

        private final AppendedMissingValuesReadAccessRow m_access;

        public AppendedMissingValuesCursor(final LookaheadCursor<ReadAccessRow> delegateCursor, final ColumnarSchema schema) {
            m_delegateCursor = delegateCursor;
            m_access = new AppendedMissingValuesReadAccessRow(delegateCursor.access(), schema);

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

        private static final class AppendedMissingValuesReadAccessRow implements ReadAccessRow {

            private final ReadAccessRow m_delegateAccess;

            private final ColumnarSchema m_schema;

            private final int m_numNonMissingColumns;

            private final ReadAccess[] m_missingColumnAccesses;

            public AppendedMissingValuesReadAccessRow(final ReadAccessRow delegateAccess, final ColumnarSchema schema) {
                m_delegateAccess = delegateAccess;
                m_schema = schema;
                m_numNonMissingColumns = m_delegateAccess.size();
                m_missingColumnAccesses = new ReadAccess[m_schema.numColumns() - m_numNonMissingColumns];
                for (int i = 0; i < m_missingColumnAccesses.length; i++) {//NOSONAR
                    m_missingColumnAccesses[i] =
                            MissingAccesses.getMissingAccess(m_schema.getSpec(m_numNonMissingColumns + i));
                }
            }

            @Override
            public int size() {
                return m_schema.numColumns();
            }

            @Override
            public <A extends ReadAccess> A getAccess(final int index) {
                if (index < m_numNonMissingColumns) {
                    return m_delegateAccess.getAccess(index);
                } else {
                    @SuppressWarnings("unchecked")
                    final A casted = (A)m_missingColumnAccesses[index - m_delegateAccess.size()];//NOSONAR
                    return casted;
                }
            }
        }

    }
}
