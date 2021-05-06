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
 *   May 4, 2021 (marcel): created
 */
package org.knime.core.table.virtual;

import java.io.IOException;
import java.util.function.BiConsumer;

import org.knime.core.table.access.BufferedAccesses;
import org.knime.core.table.access.BufferedAccesses.BufferedAccess;
import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.access.WriteAccess;
import org.knime.core.table.cursor.Cursor;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.row.WriteAccessRow;
import org.knime.core.table.schema.ColumnarSchema;

/**
 * Implementation of the operation specified by {@link MapTransformSpec}.
 *
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
class MappedTable implements RowAccessible {

    private final RowAccessible m_delegateTable;

    private final BiConsumer<ReadAccessRow, WriteAccessRow> m_mapper;

    private final ColumnarSchema m_schema;

    public MappedTable(final RowAccessible tableToMap, final BiConsumer<ReadAccessRow, WriteAccessRow> mapper,
        final ColumnarSchema mappedSchema) {
        m_delegateTable = tableToMap;
        m_mapper = mapper;
        m_schema = mappedSchema;
    }

    @Override
    public ColumnarSchema getSchema() {
        return m_schema;
    }

    @SuppressWarnings("resource") // Delegate cursor will be closed upon closing of the returned cursor.
    @Override
    public Cursor<ReadAccessRow> createCursor() {
        return new MappedCursor(m_delegateTable.createCursor(), m_mapper, m_schema);
    }

    @Override
    public void close() throws IOException {
        m_delegateTable.close();
    }

    private static final class MappedCursor implements Cursor<ReadAccessRow> {

        private final Cursor<ReadAccessRow> m_delegateCursor;

        private final ReadAccessRow m_delegateAccess;

        private final BiConsumer<ReadAccessRow, WriteAccessRow> m_mapper;

        private final BufferedAccessRow m_access;

        public MappedCursor(final Cursor<ReadAccessRow> delegateCursor,
            final BiConsumer<ReadAccessRow, WriteAccessRow> mapper, final ColumnarSchema schema) {
            m_delegateCursor = delegateCursor;
            m_delegateAccess = delegateCursor.access();
            m_mapper = mapper;
            m_access = new BufferedAccessRow(schema);
        }

        @Override
        public ReadAccessRow access() {
            return m_access;
        }

        @Override
        public boolean forward() {
            final boolean forwarded = m_delegateCursor.forward();
            if (forwarded) {
                // Explicitly reset accesses such that they behave like our default accesses (i.e. point to missing
                // entries by default). Otherwise values from one row might erroneously be carried over to the next row.
                for (final BufferedAccess access : m_access.m_accesses) {
                    access.setMissing();
                }
                m_mapper.accept(m_delegateAccess, m_access);
            }
            return forwarded;
        }

        @Override
        public void close() throws IOException {
            m_delegateCursor.close();
        }

        private static final class BufferedAccessRow implements ReadAccessRow, WriteAccessRow {

            private final BufferedAccess[] m_accesses;

            public BufferedAccessRow(final ColumnarSchema schema) {
                m_accesses = new BufferedAccess[schema.numColumns()];
                for (int i = 0; i < m_accesses.length; i++) {
                    m_accesses[i] = BufferedAccesses.createBufferedAccess(schema.getSpec(i));
                }
            }

            @Override
            public int getNumColumns() {
                return m_accesses.length;
            }

            @Override
            public <A extends ReadAccess> A getAccess(final int index) {
                @SuppressWarnings("unchecked")
                final A casted = (A)m_accesses[index]; // Ensuring type safety is the client's responsibility.
                return casted;
            }

            @Override
            public <A extends WriteAccess> A getWriteAccess(final int index) {
                @SuppressWarnings("unchecked")
                final A casted = (A)m_accesses[index]; // Ensuring type safety is the client's responsibility.
                return casted;
            }

            @Override
            public void setFrom(final ReadAccessRow values) {
                // TODO: check if source and destination are equally long?
                final int numColumns = values.getNumColumns();
                for (int i = 0; i < numColumns; i++) {
                    m_accesses[i].setFrom(values.getAccess(i));
                }
            }
        }
    }
}
