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
 *   Apr 3, 2021 (marcel): created
 */
package org.knime.core.table.virtual;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.stream.Collectors;

import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.cursor.Cursor;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.virtual.DelegatingReadAccesses.DelegatingReadAccess;
import org.knime.core.table.virtual.spec.ConcatenateTransformSpec;

/**
 * Implementation of the operation specified by {@link ConcatenateTransformSpec}.
 *
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
class ConcatenatedRowAccessible implements RowAccessible {

    private final List<RowAccessible> m_delegates;

    private final ColumnarSchema m_schema;

    ConcatenatedRowAccessible(final List<RowAccessible> tablesToConcatenate) {
        m_delegates = tablesToConcatenate;
        m_schema = ColumnarSchemas
            .concatenate(tablesToConcatenate.stream().map((i) -> i.getSchema()).collect(Collectors.toList()));
    }

    /**
     * @return the concatenated row accessibles
     */
    public List<? extends RowAccessible> getConcatenatedRowAccessibles() {
        return m_delegates;
    }

    @Override
    public ColumnarSchema getSchema() {
        return m_schema;
    }

    @Override
    public Cursor<ReadAccessRow> createCursor() {
        return new ConcatenatedRowCursor(m_delegates, m_schema);
    }

    @Override
    public void close() throws IOException {
        for (int i = 0; i < m_delegates.size(); i++) {
            m_delegates.get(i).close();
        }
    }

    // TODO: the implementation of the cursor here is conceptually pretty similar to the multi-chunk cursor in
    // knime-core-columnar. Consolidate some of the "multiple-underlying-partitions" handling logic?
    private static final class ConcatenatedRowCursor implements Cursor<ReadAccessRow> {

        private final List<RowAccessible> m_delegateTables;

        private final ConcatenatedReadAccessRow m_access;

        private int m_currentTableIndex = 0;

        private Cursor<ReadAccessRow> m_currentDelegateCursor;

        public ConcatenatedRowCursor(final List<RowAccessible> inputs, final ColumnarSchema schema) {
            m_delegateTables = inputs;
            @SuppressWarnings("resource") // Cursor is closed below.
            final Cursor<ReadAccessRow> delegate = m_delegateTables.get(m_currentTableIndex).createCursor();
            m_currentDelegateCursor = delegate;
            m_access = new ConcatenatedReadAccessRow(m_currentDelegateCursor.access(), schema);
        }

        @Override
        public ReadAccessRow access() {
            return m_access;
        }

        @Override
        public boolean forward() {
            final boolean forwarded = m_currentDelegateCursor.forward();
            if (forwarded) {
                return forwarded;
            } else {
                try {
                    m_currentDelegateCursor.close();
                }
                // TODO: or add IOException to the signature of forward?
                catch (final IOException ex) {
                    throw new UncheckedIOException(ex);
                }
                m_currentTableIndex++;
                if (m_currentTableIndex < m_delegateTables.size()) {
                    @SuppressWarnings("resource") // Cursor is closed above or below.
                    final Cursor<ReadAccessRow> delegateCursor =
                        m_delegateTables.get(m_currentTableIndex).createCursor();
                    m_currentDelegateCursor = delegateCursor;
                    m_access.setDelegateAccess(m_currentDelegateCursor.access());
                    return forward();
                } else {
                    return false;
                }
            }
        }

        @Override
        public void close() throws IOException {
            m_currentDelegateCursor.close();
        }

        private static final class ConcatenatedReadAccessRow implements ReadAccessRow {

            private final ColumnarSchema m_schema;

            private final DelegatingReadAccess<?>[] m_accesses;

            public ConcatenatedReadAccessRow(final ReadAccessRow initialDelegateAccess, final ColumnarSchema schema) {
                m_schema = schema;
                m_accesses = new DelegatingReadAccess[m_schema.numColumns()];
                for (int i = 0; i < m_accesses.length; i++) {
                    m_accesses[i] = DelegatingReadAccesses.createDelegatingAccess(m_schema.getSpec(i));
                }
                setDelegateAccess(initialDelegateAccess);
            }

            private void setDelegateAccess(final ReadAccessRow delegateAccess) {
                for (int i = 0; i < m_schema.numColumns(); i++) {
                    m_accesses[i].setDelegateAccess(delegateAccess.getAccess(i));
                }
            }

            @Override
            public int size() {
                return m_schema.numColumns();
            }

            @Override
            public <A extends ReadAccess> A getAccess(final int index) {
                @SuppressWarnings("unchecked")
                final A casted = (A)m_accesses[index];
                return casted;
            }
        }
    }
}
