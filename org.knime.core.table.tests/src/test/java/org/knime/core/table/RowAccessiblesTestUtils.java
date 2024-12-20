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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.knime.core.table.TestAccesses.TestAccess;
import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.access.WriteAccess;
import org.knime.core.table.cursor.Cursor;
import org.knime.core.table.cursor.LookaheadCursor;
import org.knime.core.table.cursor.RandomAccessCursor;
import org.knime.core.table.cursor.WriteCursor;
import org.knime.core.table.row.LookaheadRowAccessible;
import org.knime.core.table.row.RandomRowAccessible;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.row.RowWriteAccessible;
import org.knime.core.table.row.Selection;
import org.knime.core.table.row.WriteAccessRow;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.virtual.RowAccessibles;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
public final class RowAccessiblesTestUtils {

    private RowAccessiblesTestUtils() {
    }

    /**
     * Wraps a {@link TestRowAccessible} without exposing the {@link RandomRowAccessible} interface
     */
    static class TestRowAccessibleNoRandomAccess implements LookaheadRowAccessible {

        private final TestRowAccessible m_delegate;

        private TestRowAccessibleNoRandomAccess(TestRowAccessible delegate) {
            m_delegate = delegate;
        }

        @Override
        public ColumnarSchema getSchema() {
            return m_delegate.getSchema();
        }

        @Override
        public LookaheadCursor<ReadAccessRow> createCursor() {
            return m_delegate.createCursor();
        }

        @Override
        public LookaheadCursor<ReadAccessRow> createCursor(Selection selection) {
            return m_delegate.createCursor(selection);
        }

        @Override
        public long size() {
            return m_delegate.size();
        }

        @Override
        public void close() throws IOException {
            m_delegate.close();
        }
    }

    /**
     * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
     * @since 4.4
     */
    public static class TestRowAccessible implements RandomRowAccessible {

        private final ColumnarSchema m_schema;

        private final TestAccess[] m_accesses;

        private final long m_length;

        private TestRowAccessible(final ColumnarSchema schema, final TestAccess[] accesses, final long length) {
            m_schema = schema;
            m_accesses = accesses;
            m_length = length;
        }

        /**
         * Wrap as {@code TestRowAccessible} without exposing the {@link RandomRowAccessible} interface
         */
        public TestRowAccessibleNoRandomAccess toRowAccessible() {
            return new TestRowAccessibleNoRandomAccess(this);
        }

        @Override
        public void close() throws IOException {
        }

        @Override
        public ColumnarSchema getSchema() {
            return m_schema;
        }

        @Override
        public RandomAccessCursor<ReadAccessRow> createCursor() {
            return new TestRandomRowAccessCursor(m_accesses, m_length);
        }

        @Override
        public RandomAccessCursor<ReadAccessRow> createCursor(final Selection selection) {
            return new TestRandomRowAccessCursor(m_accesses, selection, m_length);
        }

        @Override
        public long size() {
            return m_length;
        }

        private static final class TestRandomRowAccessCursor implements RandomAccessCursor<ReadAccessRow>, ReadAccessRow {

            private final TestAccess[] m_accesses;

            // inclusive
            private final int m_fromIndex;

            // exclusive
            private final int m_toIndex;

            private int m_index;

            TestRandomRowAccessCursor(final TestAccess[] accesses, final long length) {
                this(accesses, Selection.all(), length);
            }

            TestRandomRowAccessCursor(final TestAccess[] accesses, final Selection selection, final long length) {
                m_accesses = new TestAccess[accesses.length];
                Arrays.setAll(m_accesses, i -> selection.columns().isSelected(i) ? accesses[i].copy() : null);
                if (selection.rows().allSelected()) {
                    m_fromIndex = 0;
                    m_toIndex = (int)length;
                } else {
                    m_fromIndex = (int)selection.rows().fromIndex();
                    m_toIndex = (int)Math.min(selection.rows().toIndex(), length);
                }
                m_index = m_fromIndex - 1;
            }

            @Override
            public void close() throws IOException {
            }

            @Override
            public int size() {
                return m_accesses.length;
            }

            @Override
            public void moveTo(long row) {
                setRowIndex(row + m_fromIndex);
            }

            private void setRowIndex(long row) {
                if (row < m_fromIndex || row >= m_toIndex) {
                    throw new IndexOutOfBoundsException(String.format("%d out of bounds [%d, %d)", row, m_fromIndex, m_toIndex));
                }
                m_index = (int)row;
                for (TestAccess access : m_accesses) {
                    if (access != null) {
                        access.setIndex(m_index);
                    }
                }
            }

            @Override
            public boolean canForward() {
                return m_index + 1 < m_toIndex;
            }

            @Override
            public boolean forward() {
                if (canForward()) {
                    setRowIndex(m_index + 1);
                    return true;
                }
                return false;
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

        private int m_index = 0;

        private TestRowWriteAccessible(final ColumnarSchema schema, final TestAccess[] accesses) {
            m_schema = schema;
            m_accesses = accesses;
        }

        @Override
        public void close() throws IOException {
        }

        @Override
        public WriteCursor<WriteAccessRow> getWriteCursor() {
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
            return m_index;
        }

        private final class TestWriteAccessCursor implements WriteCursor<WriteAccessRow>, WriteAccessRow {

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
            public void commit() throws IOException {
                m_index++;
                for (TestAccess access : m_accesses) {
                    access.setIndex(m_index);
                }
            }

            @Override
            public void flush() throws IOException {
                // noop
            }

            @Override
            public void finish() throws IOException {
                // noop
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
        return toRowAccessible(writeTable(schema, valuesPerRow));
    }

    private static TestRowWriteAccessible writeTable(ColumnarSchema schema, Object[][] valuesPerRow) {
        final TestRowWriteAccessible table = createRowWriteAccessible(schema);
        try (final WriteCursor<WriteAccessRow> cursor = table.getWriteCursor()) {
            final WriteAccessRow row = cursor.access();
            for (int r = 0; r < valuesPerRow.length; r++) {
                for (int c = 0; c < schema.numColumns(); c++) {
                    WriteAccessValueSetter.setValue(schema.getSpec(c), row.getWriteAccess(c), valuesPerRow[r][c]);
                }
                cursor.commit();
            }
        } catch (IOException e) {
            // will not happen
        }
        return table;
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
        assertTableEqualsValues(expectedValues, result, false);
        assertTableEqualsValues(expectedValues, result, true);
    }

    public static void assertTableEqualsValues(final Object[][] expectedValues, final RowAccessible actualTable, final boolean selectAll) {
        final ColumnarSchema schema = actualTable.getSchema();
        try (final Cursor<ReadAccessRow> cursor = selectAll ? actualTable.createCursor(Selection.all()) : actualTable.createCursor()) {
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
                } else if (expected instanceof Double) {
                    // Special case for ListData and StructData.
                    assertEquals((double)expected, (double)actual, 0.000001);
                } else {
                    assertEquals(expected, actual);
                }
            } catch (final AssertionError e) {
                throw new AssertionError("At column index " + i + ": " + e.getMessage(), e);
            }
        }
    }

    public static RowAccessible[] toLookahead(final RowAccessible[] accessibles) {
        return Arrays.stream(accessibles) //
            .map(RowAccessibles::toLookahead) //
            .toArray(RowAccessible[]::new);
    }

    public static void assertCanForwardPredictsForward(final RowAccessible rowAccessible) {
        try (final LookaheadCursor<ReadAccessRow> cursor = RowAccessibles.toLookahead(rowAccessible).createCursor()) {
            boolean predictedForward = cursor.canForward();
            while (cursor.forward()) {
                assertTrue("canForward() was false, but then forward() returned true", predictedForward);
                predictedForward = cursor.canForward();
            }
            assertFalse("canForward() was true, but then forward() returned false", predictedForward);
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    public static void assertTableEqualsValuesInRandomRowOrder(final Object[][] expectedValues, final RowAccessible actualTable, final boolean selectAll) {
        assertTrue("The table under test is not a RandomRowAccessible", actualTable instanceof RandomRowAccessible);
        final RandomRowAccessible rra = (RandomRowAccessible)actualTable;
        final int size = (int)rra.size();
        assertEquals("The size of the table under test is smaller than the expected size.", expectedValues.length, size);
        final List<Integer> indices = IntStream.range(0, size).boxed().collect(Collectors.toList());
        Collections.shuffle(indices, new Random(1L));
        final ColumnarSchema schema = rra.getSchema();
        try (final RandomAccessCursor<ReadAccessRow> cursor = selectAll ? rra.createCursor(Selection.all()) : rra.createCursor()) {
            final ReadAccessRow actualRow = cursor.access();
            for (int index : indices) {
                cursor.moveTo(index);
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
        } catch (final IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }
}
