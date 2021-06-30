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
 *   Apr 30, 2021 (marcel): created
 */
package org.knime.core.table;

import static org.knime.core.table.RowAccessiblesTestUtils.createRowAccessibleFromRowWiseValues;

import java.io.IOException;

import org.junit.Test;
import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.schema.BooleanDataSpec;
import org.knime.core.table.schema.ByteDataSpec;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.schema.DefaultColumnarSchema;
import org.knime.core.table.schema.DoubleDataSpec;
import org.knime.core.table.schema.FloatDataSpec;
import org.knime.core.table.schema.IntDataSpec;
import org.knime.core.table.schema.LongDataSpec;
import org.knime.core.table.schema.StringDataSpec;
import org.knime.core.table.schema.VarBinaryDataSpec;
import org.knime.core.table.schema.VoidDataSpec;
import org.knime.core.table.virtual.ColumnarSchemas;
import org.knime.core.table.virtual.RowAccessibles;
import org.knime.core.table.virtual.spec.ColumnFilterTransformSpec;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings({"javadoc", "static-method"})
public final class ColumnFilterTransformTest {

    @Test
    public void testFilterFirstColumn() throws IOException {
        final ColumnarSchema originalSchema =
            new DefaultColumnarSchema(DoubleDataSpec.INSTANCE, IntDataSpec.INSTANCE, StringDataSpec.INSTANCE);
        final Object[][] originalValues = new Object[][]{ //
            new Object[]{0.1, 1, "First"}, //
            new Object[]{0.2, 2, "Second"}, //
            new Object[]{0.3, 3, "Third"}, //
            new Object[]{0.4, 4, "Fourth"}, //
            new Object[]{0.5, 5, "Fifth"} //
        };
        final int[] columnIndicesToKeep = {1, 2};

        final ColumnarSchema filteredSchema = new DefaultColumnarSchema(IntDataSpec.INSTANCE, StringDataSpec.INSTANCE);
        final Object[][] filteredValues = new Object[][]{ //
            new Object[]{1, "First"}, //
            new Object[]{2, "Second"}, //
            new Object[]{3, "Third"}, //
            new Object[]{4, "Fourth"}, //
            new Object[]{5, "Fifth"} //
        };

        testFilterTable(filteredSchema, filteredValues, originalSchema, originalValues, columnIndicesToKeep);
    }

    @Test
    public void testFilterLastColumn() throws IOException {
        final ColumnarSchema originalSchema =
            new DefaultColumnarSchema(DoubleDataSpec.INSTANCE, IntDataSpec.INSTANCE, StringDataSpec.INSTANCE);
        final Object[][] originalValues = new Object[][]{ //
            new Object[]{0.1, 1, "First"}, //
            new Object[]{0.2, 2, "Second"}, //
            new Object[]{0.3, 3, "Third"}, //
            new Object[]{0.4, 4, "Fourth"}, //
            new Object[]{0.5, 5, "Fifth"} //
        };
        final int[] columnIndicesToKeep = {0, 1};

        final ColumnarSchema filteredSchema = new DefaultColumnarSchema(DoubleDataSpec.INSTANCE, IntDataSpec.INSTANCE);
        final Object[][] filteredValues = new Object[][]{ //
            new Object[]{0.1, 1}, //
            new Object[]{0.2, 2}, //
            new Object[]{0.3, 3}, //
            new Object[]{0.4, 4}, //
            new Object[]{0.5, 5} //
        };

        testFilterTable(filteredSchema, filteredValues, originalSchema, originalValues, columnIndicesToKeep);
    }

    @Test
    public void testFilterAboutHalfOfTheColumns() throws IOException {
        final ColumnarSchema originalSchema = new DefaultColumnarSchema(DoubleDataSpec.INSTANCE, IntDataSpec.INSTANCE,
            StringDataSpec.INSTANCE, BooleanDataSpec.INSTANCE, FloatDataSpec.INSTANCE, LongDataSpec.INSTANCE,
            ByteDataSpec.INSTANCE, VarBinaryDataSpec.INSTANCE, VoidDataSpec.INSTANCE);
        final Object[][] originalValues = new Object[][]{ //
            new Object[]{0.1, 1, "First", true, 0.01f, 10l, (byte)11, new byte[]{1, 2, 3, 4}, null}, //
            new Object[]{0.2, 2, "Second", false, 0.02f, 20l, (byte)22, new byte[]{5, 6, 7, 8}, null}, //
            new Object[]{0.3, 3, "Third", true, 0.03f, 30l, (byte)33, new byte[]{9, 0, 1, 2}, null}, //
            new Object[]{0.4, 4, "Fourth", false, 0.04f, 40l, (byte)44, new byte[]{3, 4, 5, 6}, null}, //
            new Object[]{0.5, 5, "Fifth", true, 0.05f, 50l, (byte)55, new byte[]{7, 8, 9, 0}, null} //
        };
        final int[] columnIndicesToKeep = {0, 3, 4, 7};

        final ColumnarSchema filteredSchema = new DefaultColumnarSchema(DoubleDataSpec.INSTANCE,
            BooleanDataSpec.INSTANCE, FloatDataSpec.INSTANCE, VarBinaryDataSpec.INSTANCE);
        final Object[][] filteredValues = new Object[][]{ //
            new Object[]{0.1, true, 0.01f, new byte[]{1, 2, 3, 4}}, //
            new Object[]{0.2, false, 0.02f, new byte[]{5, 6, 7, 8}}, //
            new Object[]{0.3, true, 0.03f, new byte[]{9, 0, 1, 2}}, //
            new Object[]{0.4, false, 0.04f, new byte[]{3, 4, 5, 6}}, //
            new Object[]{0.5, true, 0.05f, new byte[]{7, 8, 9, 0}} //
        };

        testFilterTable(filteredSchema, filteredValues, originalSchema, originalValues, columnIndicesToKeep);
    }

    @Test
    public void testFilterAllButOneColumn() throws IOException {
        final ColumnarSchema originalSchema = new DefaultColumnarSchema(DoubleDataSpec.INSTANCE, IntDataSpec.INSTANCE,
            StringDataSpec.INSTANCE, BooleanDataSpec.INSTANCE, FloatDataSpec.INSTANCE, LongDataSpec.INSTANCE,
            ByteDataSpec.INSTANCE, VarBinaryDataSpec.INSTANCE, VoidDataSpec.INSTANCE);
        final Object[][] originalValues = new Object[][]{ //
            new Object[]{0.1, 1, "First", true, 0.01f, 10l, (byte)11, new byte[]{1, 2, 3, 4}, null}, //
            new Object[]{0.2, 2, "Second", false, 0.02f, 20l, (byte)22, new byte[]{5, 6, 7, 8}, null}, //
            new Object[]{0.3, 3, "Third", true, 0.03f, 30l, (byte)33, new byte[]{9, 0, 1, 2}, null}, //
            new Object[]{0.4, 4, "Fourth", false, 0.04f, 40l, (byte)44, new byte[]{3, 4, 5, 6}, null}, //
            new Object[]{0.5, 5, "Fifth", true, 0.05f, 50l, (byte)55, new byte[]{7, 8, 9, 0}, null} //
        };
        final int[] columnIndicesToKeep = {1};

        final ColumnarSchema filteredSchema = new DefaultColumnarSchema(IntDataSpec.INSTANCE);
        final Object[][] filteredValues = new Object[][]{ //
            new Object[]{1}, //
            new Object[]{2}, //
            new Object[]{3}, //
            new Object[]{4}, //
            new Object[]{5} //
        };

        testFilterTable(filteredSchema, filteredValues, originalSchema, originalValues, columnIndicesToKeep);
    }

    @Test
    public void testFilterAllColumns() throws IOException {
        final ColumnarSchema originalSchema =
            new DefaultColumnarSchema(DoubleDataSpec.INSTANCE, IntDataSpec.INSTANCE, StringDataSpec.INSTANCE);
        final Object[][] originalValues = new Object[][]{ //
            new Object[]{0.1, 1, "First"}, //
            new Object[]{0.2, 2, "Second"}, //
            new Object[]{0.3, 3, "Third"}, //
            new Object[]{0.4, 4, "Fourth"}, //
            new Object[]{0.5, 5, "Fifth"} //
        };
        final int[] columnIndicesToKeep = {};

        final ColumnarSchema filteredSchema = new DefaultColumnarSchema();
        final Object[][] filteredValues = new Object[0][0];

        testFilterTable(filteredSchema, filteredValues, originalSchema, originalValues, columnIndicesToKeep);
    }

    @SuppressWarnings("unused")
    @Test(expected = IndexOutOfBoundsException.class)
    public void testRejectNegativeColumnIndices() {
        new ColumnFilterTransformSpec(new int[]{-1, 0, 2, 3});
    }

    @SuppressWarnings("unused")
    @Test(expected = IllegalArgumentException.class)
    public void testRejectDuplicateColumnIndices() {
        new ColumnFilterTransformSpec(new int[]{0, 1, 3, 3});
    }

    @SuppressWarnings("unused")
    @Test(expected = IllegalArgumentException.class)
    public void testRejectUnorderedColumnIndices() {
        new ColumnFilterTransformSpec(new int[]{0, 2, 1, 3});
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testRejectFilterContainsColumnIndicesNotInTable() {
        ColumnarSchemas.filter(
            new DefaultColumnarSchema(DoubleDataSpec.INSTANCE, IntDataSpec.INSTANCE, StringDataSpec.INSTANCE),
            new int[]{0, 3});
    }

    private static void testFilterTable(final ColumnarSchema expectedSchema, final Object[][] expectedValues,
        final ColumnarSchema originalSchema, final Object[][] originalValues, final int[] columnIndicesToKeep)
        throws IOException {
        try (final RowAccessible originalTable = createRowAccessibleFromRowWiseValues(originalSchema, originalValues)) {
            @SuppressWarnings("resource")
            RowAccessible result = RowAccessibles.filter(originalTable, columnIndicesToKeep);
            RowAccessiblesTestUtils.assertRowAccessibleEquals(result, expectedSchema, expectedValues);
        }
    }
}
