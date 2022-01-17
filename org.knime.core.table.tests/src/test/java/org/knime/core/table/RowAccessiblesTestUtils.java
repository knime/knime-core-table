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
 *   Apr 28, 2021 (marcel): created
 */
package org.knime.core.table;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;

import org.knime.core.table.TestAccesses.TestAccess;
import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.access.WriteAccess;
import org.knime.core.table.cursor.Cursor;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.row.RowWriteAccessible;
import org.knime.core.table.row.WriteAccessRow;
import org.knime.core.table.schema.ColumnarSchema;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
public final class RowAccessiblesTestUtils {

    private RowAccessiblesTestUtils() {
    }

    /**
     * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
     * @since 4.4
     */
    public static class TestRowAccessible implements RowAccessible {

        private final ColumnarSchema m_schema;

        private final TestAccess[] m_accesses;

        private final long m_length;

        private TestRowAccessible(final ColumnarSchema schema, final TestAccess[] accesses, final long length) {
            m_schema = schema;
            m_accesses = accesses;
            m_length = length;
        }

        @Override
        public void close() throws IOException {
        }

        @Override
        public ColumnarSchema getSchema() {
            return m_schema;
        }

        @Override
        public Cursor<ReadAccessRow> createCursor() {
            return new TestRowAccessCursor(m_accesses, m_length);
        }

        private static final class TestRowAccessCursor implements Cursor<ReadAccessRow>, ReadAccessRow {

            private final TestAccess[] m_accesses;

            private final long m_length;

            private int m_index = -1;

            TestRowAccessCursor(final TestAccess[] accesses, final long length) {
                m_accesses = new TestAccess[accesses.length];
                Arrays.setAll(m_accesses, i -> accesses[i].copy());
                m_length = length;
            }

            @Override
            public void close() throws IOException {
            }

            @Override
            public int size() {
                return m_accesses.length;
            }

            @Override
            public boolean forward() {
                if (++m_index >= m_length) {
                    return false;
                } else {
                    for (TestAccess access : m_accesses) {
                        access.setIndex(m_index);
                    }
                }
                return true;
            }

            @Override
            public <A extends ReadAccess> A getAccess(final int index) {
                @SuppressWarnings("unchecked")
                final A cast = (A)m_accesses[index];
                return cast;
            }

            @Override
            public ReadAccessRow access() {
                return this;
            }
        }

    }

    /**
     * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
     * @since 4.4
     */
    public static class TestRowWriteAccessible implements RowWriteAccessible {

        private final ColumnarSchema m_schema;

        private final TestAccess[] m_accesses;

        private int m_index = -1;

        private TestRowWriteAccessible(final ColumnarSchema schema, final TestAccess[] accesses) {
            m_schema = schema;
            m_accesses = accesses;
        }

        @Override
        public void close() throws IOException {
        }

        @Override
        public Cursor<WriteAccessRow> getWriteCursor() {
            return new TestWriteAccessCursor(m_accesses);
        }

        @Override
        public ColumnarSchema getSchema() {
            return m_schema;
        }

        private TestAccess[] getAccesses() {
            return m_accesses;
        }

        long length() {
            return m_index + 1;
        }

        private final class TestWriteAccessCursor implements Cursor<WriteAccessRow>, WriteAccessRow {

            private final WriteAccess[] m_access;

            TestWriteAccessCursor(final WriteAccess[] accesses) {
                m_access = accesses;
            }

            @Override
            public void close() {
            }

            @Override
            public int size() {
                return m_access.length;
            }

            @Override
            public <A extends WriteAccess> A getWriteAccess(final int index) {
                @SuppressWarnings("unchecked")
                final A access = (A)m_access[index];
                return access;
            }

            @Override
            public void setFrom(final ReadAccessRow row) {
                for (int i = 0; i < m_access.length; i++) {
                    final ReadAccess access = row.getAccess(i);
                    m_access[i].setFrom(access);
                }
            }

            @Override
            public WriteAccessRow access() {
                return this;
            }

            @Override
            public boolean forward() {
                m_index++;
                for (TestAccess access : m_accesses) {
                    access.setIndex(m_index);
                }
                return true;
            }
        }
    }

    /**
     * Create a new {@link TestRowWriteAccessible} which can store numRows rows.
     *
     * @param schema the schema
     *
     * @return the {@link TestRowWriteAccessible}
     */
    public static TestRowWriteAccessible createRowWriteAccessible(final ColumnarSchema schema) {
        return new TestRowWriteAccessible(schema, TestAccesses.createTestAccesses(schema));
    }

    /**
     * Simple helper method to create a RowAccessible from an object array
     *
     * @param schema to create row accessible
     * @param valuesPerRow the values
     * @return a row accessible filled with values
     * @throws IOException
     */
    @SuppressWarnings("resource")
    public static RowAccessible createRowAccessibleFromRowWiseValues(final ColumnarSchema schema,
        final Object[][] valuesPerRow) {
        final TestRowWriteAccessible table = createRowWriteAccessible(schema);
        try (final Cursor<WriteAccessRow> cursor = table.getWriteCursor()) {
            final WriteAccessRow row = cursor.access();
            for (int r = 0; r < valuesPerRow.length; r++) {
                cursor.forward();
                for (int c = 0; c < schema.numColumns(); c++) {
                    WriteAccessValueSetter.setValue(schema.getSpec(c), row.getWriteAccess(c), valuesPerRow[r][c]);
                }
            }
        } catch (IOException e) {
            // will not happen
        }
        return toRowAccessible(table);
    }

    /**
     * Convert {@link TestRowWriteAccessible} into {@link TestRowAccessible}
     *
     * @param table to turn into {@link RowAccessible}
     * @return the {@link RowAccessible} on the provided table
     */
    public static TestRowAccessible toRowAccessible(final TestRowWriteAccessible table) {
        return new TestRowAccessible(table.getSchema(), table.getAccesses(), table.length());
    }

    // Store needs to be kept open to allow reading. It's the client's responsibility to close the store when done with
    // it.
    @SuppressWarnings("resource")
    static RowAccessible createZeroColumnTable() {
        return toRowAccessible(createRowWriteAccessible(ColumnarSchema.of()));
    }

    static void assertRowAccessibleEquals(final RowAccessible result, final ColumnarSchema expectedSchema,
        final Object[][] expectedValues) throws IOException {
        assertEquals(expectedSchema, result.getSchema());
        assertTableEqualsValues(expectedValues, result);
    }

    public static void assertTableEqualsValues(final Object[][] expectedValues, final RowAccessible actualTable) {
        final ColumnarSchema schema = actualTable.getSchema();
        try (final Cursor<ReadAccessRow> cursor = actualTable.createCursor()) {
            final ReadAccessRow actualRow = cursor.access();
            int index;
            for (index = 0; cursor.forward(); index++) {
                final Object[] expectedRow;
                try {
                    expectedRow = expectedValues[index];
                } catch (final IndexOutOfBoundsException ex) {
                    throw new AssertionError("The size of the table under test is greater than the expected size of "
                        + expectedValues.length + ".", ex);
                }
                try {
                    assertRowEqualsValues(schema, expectedRow, actualRow);
                } catch (final AssertionError e) {
                    throw new AssertionError("At row index " + index + ": " + e.getMessage(), e);
                }
            }
            assertEquals("The size of the table under test is smaller than the expected size.", expectedValues.length,
                index);
        } catch (final IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    static void assertRowEqualsValues(final ColumnarSchema schema, final Object[] expectedValues,
        final ReadAccessRow actualRow) {
        assertEquals("Number of entries in row differ: ", expectedValues.length, actualRow.size());
        for (int i = 0; i < actualRow.size(); i++) {
            final Object expected = expectedValues[i];
            final Object actual = ReadAccessValueGetter.getValue(schema.getSpec(i), actualRow.getAccess(i));
            try {
                if (expected instanceof byte[]) {
                    // Special case for VarBinaryData.
                    assertArrayEquals((byte[])expected, (byte[])actual);
                } else if (expected instanceof Object[]) {
                    // Special case for ListData and StructData.
                    assertArrayEquals((Object[])expected, (Object[])actual);
                } else {
                    assertEquals(expected, actual);
                }
            } catch (final AssertionError e) {
                throw new AssertionError("At column index " + i + ": " + e.getMessage(), e);
            }
        }
    }
}
