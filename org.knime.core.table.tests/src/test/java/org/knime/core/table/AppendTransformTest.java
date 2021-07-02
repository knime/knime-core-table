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
package org.knime.core.table;

import static org.knime.core.table.RowAccessiblesTestUtils.createRowAccessibleFromRowWiseValues;
import static org.knime.core.table.RowAccessiblesTestUtils.createZeroColumnTable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
import org.knime.core.table.virtual.RowAccessibles;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings({"javadoc", "static-method"})
public final class AppendTransformTest {

    @Test
    public void testAppendTwoTables() throws IOException {
        final ColumnarSchema firstSchema =
            TestColumnarSchemaUtils.createWithEmptyTraits(DoubleDataSpec.INSTANCE, IntDataSpec.INSTANCE, StringDataSpec.INSTANCE);
        final Object[][] firstValues = new Object[][]{ //
            new Object[]{0.1, 1, "First"}, //
            new Object[]{0.2, 2, "Second"}, //
            new Object[]{0.3, 3, "Third"}, //
            new Object[]{0.4, 4, "Fourth"}, //
            new Object[]{0.5, 5, "Fifth"} //
        };
        final ColumnarSchema secondSchema =
            TestColumnarSchemaUtils.createWithEmptyTraits(BooleanDataSpec.INSTANCE, FloatDataSpec.INSTANCE, LongDataSpec.INSTANCE);
        final Object[][] secondValues = new Object[][]{ //
            new Object[]{true, 0.01f, 10l}, //
            new Object[]{false, 0.02f, 20l}, //
            new Object[]{true, 0.03f, 30l}, //
            new Object[]{false, 0.04f, 40l}, //
            new Object[]{true, 0.05f, 50l} //
        };

        final ColumnarSchema appendedSchema = TestColumnarSchemaUtils.createWithEmptyTraits(DoubleDataSpec.INSTANCE, IntDataSpec.INSTANCE,
            StringDataSpec.INSTANCE, BooleanDataSpec.INSTANCE, FloatDataSpec.INSTANCE, LongDataSpec.INSTANCE);
        final Object[][] appendedValues = new Object[][]{ //
            new Object[]{0.1, 1, "First", true, 0.01f, 10l}, //
            new Object[]{0.2, 2, "Second", false, 0.02f, 20l}, //
            new Object[]{0.3, 3, "Third", true, 0.03f, 30l}, //
            new Object[]{0.4, 4, "Fourth", false, 0.04f, 40l}, //
            new Object[]{0.5, 5, "Fifth", true, 0.05f, 50l} //
        };

        testAppendTablesFromValues(appendedSchema, appendedValues, Arrays.asList(firstSchema, secondSchema),
            Arrays.asList(firstValues, secondValues));
    }

    @Test
    public void testAppendThreeTables() throws IOException {
        final ColumnarSchema firstSchema =
            TestColumnarSchemaUtils.createWithEmptyTraits(DoubleDataSpec.INSTANCE, IntDataSpec.INSTANCE, StringDataSpec.INSTANCE);
        final Object[][] firstValues = new Object[][]{ //
            new Object[]{0.1, 1, "First"}, //
            new Object[]{0.2, 2, "Second"}, //
            new Object[]{0.3, 3, "Third"}, //
            new Object[]{0.4, 4, "Fourth"}, //
            new Object[]{0.5, 5, "Fifth"} //
        };
        final ColumnarSchema secondSchema =
            TestColumnarSchemaUtils.createWithEmptyTraits(BooleanDataSpec.INSTANCE, FloatDataSpec.INSTANCE, LongDataSpec.INSTANCE);
        final Object[][] secondValues = new Object[][]{ //
            new Object[]{true, 0.01f, 10l}, //
            new Object[]{false, 0.02f, 20l}, //
            new Object[]{true, 0.03f, 30l}, //
            new Object[]{false, 0.04f, 40l}, //
            new Object[]{true, 0.05f, 50l} //
        };
        final ColumnarSchema thirdSchema =
            TestColumnarSchemaUtils.createWithEmptyTraits(ByteDataSpec.INSTANCE, VarBinaryDataSpec.INSTANCE, VoidDataSpec.INSTANCE);
        final Object[][] thirdValues = new Object[][]{ //
            new Object[]{(byte)11, new byte[]{1, 2, 3, 4}, null}, //
            new Object[]{(byte)22, new byte[]{5, 6, 7, 8}, null}, //
            new Object[]{(byte)33, new byte[]{9, 0, 1, 2}, null}, //
            new Object[]{(byte)44, new byte[]{3, 4, 5, 6}, null}, //
            new Object[]{(byte)55, new byte[]{7, 8, 9, 0}, null} //
        };

        final ColumnarSchema appendedSchema = TestColumnarSchemaUtils.createWithEmptyTraits(DoubleDataSpec.INSTANCE, IntDataSpec.INSTANCE,
            StringDataSpec.INSTANCE, BooleanDataSpec.INSTANCE, FloatDataSpec.INSTANCE, LongDataSpec.INSTANCE,
            ByteDataSpec.INSTANCE, VarBinaryDataSpec.INSTANCE, VoidDataSpec.INSTANCE);
        final Object[][] appendedValues = new Object[][]{ //
            new Object[]{0.1, 1, "First", true, 0.01f, 10l, (byte)11, new byte[]{1, 2, 3, 4}, null}, //
            new Object[]{0.2, 2, "Second", false, 0.02f, 20l, (byte)22, new byte[]{5, 6, 7, 8}, null}, //
            new Object[]{0.3, 3, "Third", true, 0.03f, 30l, (byte)33, new byte[]{9, 0, 1, 2}, null}, //
            new Object[]{0.4, 4, "Fourth", false, 0.04f, 40l, (byte)44, new byte[]{3, 4, 5, 6}, null}, //
            new Object[]{0.5, 5, "Fifth", true, 0.05f, 50l, (byte)55, new byte[]{7, 8, 9, 0}, null} //
        };

        testAppendTablesFromValues(appendedSchema, appendedValues,
            Arrays.asList(firstSchema, secondSchema, thirdSchema),
            Arrays.asList(firstValues, secondValues, thirdValues));
    }

    @Test
    public void testAppendReferenceIdenticalTables() throws IOException {
        final ColumnarSchema schema =
            TestColumnarSchemaUtils.createWithEmptyTraits(DoubleDataSpec.INSTANCE, IntDataSpec.INSTANCE, StringDataSpec.INSTANCE);
        final Object[][] values = new Object[][]{ //
            new Object[]{0.1, 1, "First"}, //
            new Object[]{0.2, 2, "Second"}, //
            new Object[]{0.3, 3, "Third"}, //
            new Object[]{0.4, 4, "Fourth"}, //
            new Object[]{0.5, 5, "Fifth"} //
        };

        final ColumnarSchema appendedSchema = TestColumnarSchemaUtils.createWithEmptyTraits(DoubleDataSpec.INSTANCE, IntDataSpec.INSTANCE,
            StringDataSpec.INSTANCE, DoubleDataSpec.INSTANCE, IntDataSpec.INSTANCE, StringDataSpec.INSTANCE);
        final Object[][] appendedValues = new Object[][]{ //
            new Object[]{0.1, 1, "First", 0.1, 1, "First"}, //
            new Object[]{0.2, 2, "Second", 0.2, 2, "Second"}, //
            new Object[]{0.3, 3, "Third", 0.3, 3, "Third"}, //
            new Object[]{0.4, 4, "Fourth", 0.4, 4, "Fourth"}, //
            new Object[]{0.5, 5, "Fifth", 0.5, 5, "Fifth"} //
        };

        try (final RowAccessible table = createRowAccessibleFromRowWiseValues(schema, values)) {
            testAppendTables(appendedSchema, appendedValues, Arrays.asList(table, table));
        }
    }

    @Test
    public void testAppendTablesOfDifferentSizes() throws IOException {
        final ColumnarSchema firstSchema =
            TestColumnarSchemaUtils.createWithEmptyTraits(DoubleDataSpec.INSTANCE, IntDataSpec.INSTANCE, StringDataSpec.INSTANCE);
        final Object[][] firstValues = new Object[][]{ //
            new Object[]{0.1, 1, "First"}, //
            new Object[]{0.2, 2, "Second"}, //
            new Object[]{0.3, 3, "Third"}, //
            new Object[]{0.4, 4, "Fourth"}, //
            new Object[]{0.5, 5, "Fifth"} //
        };
        final ColumnarSchema secondSchema =
            TestColumnarSchemaUtils.createWithEmptyTraits(BooleanDataSpec.INSTANCE, FloatDataSpec.INSTANCE, LongDataSpec.INSTANCE);
        final Object[][] secondValues = new Object[][]{ //
            new Object[]{true, 0.01f, 10l}, //
            new Object[]{false, 0.02f, 20l}, //
            new Object[]{true, 0.03f, 30l}, //
            new Object[]{false, 0.04f, 40l}, //
            new Object[]{true, 0.05f, 50l}, //
            new Object[]{false, 0.06f, 60l}, //
            new Object[]{true, 0.07f, 70l}, //
            new Object[]{false, 0.08f, 80l}, //
            new Object[]{true, 0.09f, 90l}, //
            new Object[]{false, 0.10f, 100l}, //
            new Object[]{true, 0.11f, 110l} //
        };

        final ColumnarSchema appendedSchema = TestColumnarSchemaUtils.createWithEmptyTraits(DoubleDataSpec.INSTANCE, IntDataSpec.INSTANCE,
            StringDataSpec.INSTANCE, BooleanDataSpec.INSTANCE, FloatDataSpec.INSTANCE, LongDataSpec.INSTANCE);
        final Object[][] appendedValues = new Object[][]{ //
            new Object[]{0.1, 1, "First", true, 0.01f, 10l}, //
            new Object[]{0.2, 2, "Second", false, 0.02f, 20l}, //
            new Object[]{0.3, 3, "Third", true, 0.03f, 30l}, //
            new Object[]{0.4, 4, "Fourth", false, 0.04f, 40l}, //
            new Object[]{0.5, 5, "Fifth", true, 0.05f, 50l}, //
            new Object[]{null, null, null, false, 0.06f, 60l}, //
            new Object[]{null, null, null, true, 0.07f, 70l}, //
            new Object[]{null, null, null, false, 0.08f, 80l}, //
            new Object[]{null, null, null, true, 0.09f, 90l}, //
            new Object[]{null, null, null, false, 0.10f, 100l}, //
            new Object[]{null, null, null, true, 0.11f, 110l} //
        };

        testAppendTablesFromValues(appendedSchema, appendedValues, Arrays.asList(firstSchema, secondSchema),
            Arrays.asList(firstValues, secondValues));
    }

    @Test
    public void testPrependZeroColumnTable() throws IOException {
        try (final RowAccessible firstTable = createZeroColumnTable()) {
            final ColumnarSchema secondSchema =
                TestColumnarSchemaUtils.createWithEmptyTraits(DoubleDataSpec.INSTANCE, IntDataSpec.INSTANCE, StringDataSpec.INSTANCE);
            final Object[][] secondValues = new Object[][]{ //
                new Object[]{0.1, 1, "First"}, //
                new Object[]{0.2, 2, "Second"}, //
                new Object[]{0.3, 3, "Third"}, //
                new Object[]{0.4, 4, "Fourth"}, //
                new Object[]{0.5, 5, "Fifth"} //
            };
            try (final RowAccessible secondTable = createRowAccessibleFromRowWiseValues(secondSchema, secondValues)) {
                testAppendTables(secondSchema, secondValues, Arrays.asList(firstTable, secondTable));
            }
        }
    }

    @Test
    public void testAppendZeroColumnTable() throws IOException {
        final ColumnarSchema firstSchema =
            TestColumnarSchemaUtils.createWithEmptyTraits(DoubleDataSpec.INSTANCE, IntDataSpec.INSTANCE, StringDataSpec.INSTANCE);
        final Object[][] firstValues = new Object[][]{ //
            new Object[]{0.1, 1, "First"}, //
            new Object[]{0.2, 2, "Second"}, //
            new Object[]{0.3, 3, "Third"}, //
            new Object[]{0.4, 4, "Fourth"}, //
            new Object[]{0.5, 5, "Fifth"} //
        };
        try (final RowAccessible firstTable = createRowAccessibleFromRowWiseValues(firstSchema, firstValues);
                final RowAccessible secondTable = createZeroColumnTable()) {
            testAppendTables(firstSchema, firstValues, Arrays.asList(firstTable, secondTable));
        }
    }

    @Test
    public void testInsertZeroColumnTable() throws IOException {
        final ColumnarSchema firstSchema =
            TestColumnarSchemaUtils.createWithEmptyTraits(DoubleDataSpec.INSTANCE, IntDataSpec.INSTANCE, StringDataSpec.INSTANCE);
        final Object[][] firstValues = new Object[][]{ //
            new Object[]{0.1, 1, "First"}, //
            new Object[]{0.2, 2, "Second"}, //
            new Object[]{0.3, 3, "Third"}, //
            new Object[]{0.4, 4, "Fourth"}, //
            new Object[]{0.5, 5, "Fifth"} //
        };
        try (final RowAccessible firstTable = createRowAccessibleFromRowWiseValues(firstSchema, firstValues);
                final RowAccessible secondTable = createZeroColumnTable()) {
            final ColumnarSchema thirdSchema =
                TestColumnarSchemaUtils.createWithEmptyTraits(BooleanDataSpec.INSTANCE, FloatDataSpec.INSTANCE, LongDataSpec.INSTANCE);
            final Object[][] thirdValues = new Object[][]{ //
                new Object[]{true, 0.01f, 10l}, //
                new Object[]{false, 0.02f, 20l}, //
                new Object[]{true, 0.03f, 30l}, //
                new Object[]{false, 0.04f, 40l}, //
                new Object[]{true, 0.05f, 50l} //
            };
            try (final RowAccessible thirdTable = createRowAccessibleFromRowWiseValues(thirdSchema, thirdValues)) {
                final ColumnarSchema appendedSchema =
                    TestColumnarSchemaUtils.createWithEmptyTraits(DoubleDataSpec.INSTANCE, IntDataSpec.INSTANCE, StringDataSpec.INSTANCE,
                        BooleanDataSpec.INSTANCE, FloatDataSpec.INSTANCE, LongDataSpec.INSTANCE);
                final Object[][] appendedValues = new Object[][]{ //
                    new Object[]{0.1, 1, "First", true, 0.01f, 10l}, //
                    new Object[]{0.2, 2, "Second", false, 0.02f, 20l}, //
                    new Object[]{0.3, 3, "Third", true, 0.03f, 30l}, //
                    new Object[]{0.4, 4, "Fourth", false, 0.04f, 40l}, //
                    new Object[]{0.5, 5, "Fifth", true, 0.05f, 50l} //
                };

                testAppendTables(appendedSchema, appendedValues, Arrays.asList(firstTable, secondTable, thirdTable));
            }
        }
    }

    @Test
    public void testAppendOnlyZeroColumnTables() throws IOException {
        try (final RowAccessible firstTable = createZeroColumnTable();
                final RowAccessible secondTable = createZeroColumnTable()) {
            testAppendTables(TestColumnarSchemaUtils.createWithEmptyTraits(), new Object[0][0], Arrays.asList(firstTable, secondTable));
        }
    }

    private static void testAppendTablesFromValues(final ColumnarSchema expectedSchema, final Object[][] expectedValues,
        final List<ColumnarSchema> schemasToAppend, final List<Object[][]> valuesToAppend) throws IOException {
        final List<RowAccessible> tablesToAppend = new ArrayList<>();
        try {
            for (int i = 0; i < schemasToAppend.size(); i++) {
                @SuppressWarnings("resource") // Closed in finally-block below.
                final RowAccessible table =
                    createRowAccessibleFromRowWiseValues(schemasToAppend.get(i), valuesToAppend.get(i));
                tablesToAppend.add(table);
            }
            testAppendTables(expectedSchema, expectedValues, tablesToAppend);
        } finally {
            for (final RowAccessible table : tablesToAppend) {
                table.close();
            }
        }
    }

    private static void testAppendTables(final ColumnarSchema expectedSchema, final Object[][] expectedValues,
        final List<RowAccessible> tablesToAppend) throws IOException {

        @SuppressWarnings("resource")
        final RowAccessible result = RowAccessibles.append(tablesToAppend.get(0),
            tablesToAppend.subList(1, tablesToAppend.size()).toArray(RowAccessible[]::new));

        RowAccessiblesTestUtils.assertRowAccessibleEquals(result, expectedSchema, expectedValues);
    }
}
