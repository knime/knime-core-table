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
 *   May 5, 2021 (marcel): created
 */
package org.knime.core.table;

import static org.knime.core.table.RowAccessiblesTestUtils.createRowAccessibleFromRowWiseValues;

import java.io.IOException;
import java.util.function.BiConsumer;

import org.junit.Test;
import org.knime.core.table.access.DoubleAccess.DoubleReadAccess;
import org.knime.core.table.access.DoubleAccess.DoubleWriteAccess;
import org.knime.core.table.access.IntAccess.IntReadAccess;
import org.knime.core.table.access.IntAccess.IntWriteAccess;
import org.knime.core.table.access.LongAccess.LongReadAccess;
import org.knime.core.table.access.StringAccess.StringReadAccess;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.row.WriteAccessRow;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.schema.DefaultColumnarSchema;
import org.knime.core.table.schema.DoubleDataSpec;
import org.knime.core.table.schema.IntDataSpec;
import org.knime.core.table.schema.LongDataSpec;
import org.knime.core.table.schema.StringDataSpec;
import org.knime.core.table.virtual.RowAccessibles;
import org.knime.core.table.virtual.spec.MapperSpec;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings({"javadoc", "static-method"})
public final class MapTransformTest {

    @Test
    public void testIdentityMap() throws IOException {
        final ColumnarSchema schema =
            new DefaultColumnarSchema(DoubleDataSpec.INSTANCE, IntDataSpec.INSTANCE, StringDataSpec.INSTANCE);
        final Object[][] values = new Object[][]{ //
            new Object[]{0.1, 1, "First"}, //
            new Object[]{null, 2, "Second"}, //
            new Object[]{0.3, null, "Third"}, //
            new Object[]{0.4, 4, null}, //
            new Object[]{0.5, 5, "Fifth"}, //
            new Object[]{null, null, null}, //
            new Object[]{null, 7, null}, //
            new Object[]{0.8, null, null}, //
            new Object[]{null, null, "Ninth"}, //
            new Object[]{1.0, 10, "Tenth"} //
        };

        final MapperSpec spec = new MapperSpec() {

            @Override
            public ColumnarSchema getOutputSchema() {
                return schema;
            }

            @Override
            public int[] getColumnSelection() {
                return null;
            }

            @Override
            public BiConsumer<ReadAccessRow, WriteAccessRow> create() {
                return (r, w) -> w.setFrom(r);
            }
        };

        testMapTable(schema, values, schema, values, spec);
    }

    @Test
    public void testAppendColumn() throws IOException {
        final ColumnarSchema originalSchema =
            new DefaultColumnarSchema(DoubleDataSpec.INSTANCE, IntDataSpec.INSTANCE, LongDataSpec.INSTANCE);
        final Object[][] originalValues = new Object[][]{ //
            new Object[]{0.1, 1, 10l}, //
            new Object[]{null, 2, 20l}, //
            new Object[]{0.3, null, 30l}, //
            new Object[]{0.4, 4, null}, //
            new Object[]{0.5, 5, 50l} //
        };

        final ColumnarSchema mappedSchema = new DefaultColumnarSchema(DoubleDataSpec.INSTANCE, IntDataSpec.INSTANCE,
            LongDataSpec.INSTANCE, DoubleDataSpec.INSTANCE);
        final Object[][] mappedValues = new Object[][]{ //
            new Object[]{0.1, 1, 10l, 11.1}, //
            new Object[]{null, 2, 20l, null}, //
            new Object[]{0.3, null, 30l, null}, //
            new Object[]{0.4, 4, null, null}, //
            new Object[]{0.5, 5, 50l, 55.5} //
        };

        final MapperSpec spec = new MapperSpec() {

            @Override
            public ColumnarSchema getOutputSchema() {
                return mappedSchema;
            }

            @Override
            public int[] getColumnSelection() {
                return null;
            }

            @Override
            public BiConsumer<ReadAccessRow, WriteAccessRow> create() {
                return (r, w) -> {
                    w.getWriteAccess(0).setFrom(r.getAccess(0));
                    w.getWriteAccess(1).setFrom(r.getAccess(1));
                    w.getWriteAccess(2).setFrom(r.getAccess(2));
                    if (r.getAccess(0).isMissing() || r.getAccess(1).isMissing() || r.getAccess(2).isMissing()) {
                        w.getWriteAccess(3).setMissing();
                    } else {
                        final double sum = r.<DoubleReadAccess> getAccess(0).getDoubleValue() //
                            + r.<IntReadAccess> getAccess(1).getIntValue() //
                            + r.<LongReadAccess> getAccess(2).getLongValue();
                        w.<DoubleWriteAccess> getWriteAccess(3).setDoubleValue(sum);
                    }
                };
            }
        };

        testMapTable(mappedSchema, mappedValues, originalSchema, originalValues, spec);
    }

    @Test
    public void testReplaceColumn() throws IOException {
        final ColumnarSchema originalSchema =
            new DefaultColumnarSchema(DoubleDataSpec.INSTANCE, StringDataSpec.INSTANCE, StringDataSpec.INSTANCE);
        final Object[][] originalValues = new Object[][]{ //
            new Object[]{null, "1", "First"}, //
            new Object[]{0.2, null, "Second"}, //
            new Object[]{0.3, "3", null}, //
            new Object[]{null, null, null}, //
            new Object[]{0.5, "5", "Fifth"} //
        };

        final ColumnarSchema mappedSchema =
            new DefaultColumnarSchema(DoubleDataSpec.INSTANCE, IntDataSpec.INSTANCE, StringDataSpec.INSTANCE);
        final Object[][] mappedValues = new Object[][]{ //
            new Object[]{null, 1, "First"}, //
            new Object[]{0.2, -1, "Second"}, //
            new Object[]{0.3, 3, null}, //
            new Object[]{null, -1, null}, //
            new Object[]{0.5, 5, "Fifth"} //
        };

        final MapperSpec spec = new MapperSpec() {

            @Override
            public ColumnarSchema getOutputSchema() {
                return mappedSchema;
            }

            @Override
            public int[] getColumnSelection() {
                return null;
            }

            @Override
            public BiConsumer<ReadAccessRow, WriteAccessRow> create() {
                return (r, w) -> {
                    w.getWriteAccess(0).setFrom(r.getAccess(0));
                    final int parsed = r.getAccess(1).isMissing() //
                        ? -1 //
                        : Integer.parseInt(r.<StringReadAccess> getAccess(1).getStringValue());
                    w.<IntWriteAccess> getWriteAccess(1).setIntValue(parsed);
                    w.getWriteAccess(2).setFrom(r.getAccess(2));
                };
            }
        };

        testMapTable(mappedSchema, mappedValues, originalSchema, originalValues, spec);
    }

    /**
     * Ensures that accesses are reset to "missing" between rows. If this was not the case, values from one row could
     * erroneously be carried over to the next row.
     */
    @Test
    public void testUnpopulatedOutputEntriesAreMissing() throws IOException {
        final ColumnarSchema schema = new DefaultColumnarSchema(IntDataSpec.INSTANCE, StringDataSpec.INSTANCE);
        final Object[][] originalValues = new Object[][]{ //
            new Object[]{1, "First"}, //
            new Object[]{2, "Second"}, //
            new Object[]{3, "Third"}, //
            new Object[]{4, "Fourth"}, //
            new Object[]{5, "Fifth"} //
        };

        final Object[][] mappedValues = new Object[][]{ //
            new Object[]{1, null}, //
            new Object[]{null, "Second"}, //
            new Object[]{3, null}, //
            new Object[]{4, "Fourth"}, //
            new Object[]{null, null} //
        };

        final MapperSpec spec = new MapperSpec() {

            @Override
            public ColumnarSchema getOutputSchema() {
                return schema;
            }

            @Override
            public int[] getColumnSelection() {
                return null;
            }

            @Override
            public BiConsumer<ReadAccessRow, WriteAccessRow> create() {
                // TODO Auto-generated method stub
                return (r, w) -> {
                    final int intValue = r.<IntReadAccess> getAccess(0).getIntValue();
                    if (intValue == 1 || intValue == 3) {
                        w.getWriteAccess(0).setFrom(r.getAccess(0));
                    } else if (intValue == 2) {
                        w.getWriteAccess(1).setFrom(r.getAccess(1));
                    } else if (intValue == 4) {
                        w.getWriteAccess(0).setFrom(r.getAccess(0));
                        w.getWriteAccess(1).setFrom(r.getAccess(1));
                    }
                };
            }
        };

        testMapTable(schema, mappedValues, schema, originalValues, spec);
    }

    @Test
    public void testNoOpMap() throws IOException {
        final ColumnarSchema schema =
            new DefaultColumnarSchema(DoubleDataSpec.INSTANCE, IntDataSpec.INSTANCE, StringDataSpec.INSTANCE);
        final Object[][] originalValues = new Object[][]{ //
            new Object[]{0.1, 1, "First"}, //
            new Object[]{null, 2, "Second"}, //
            new Object[]{0.3, null, "Third"}, //
            new Object[]{0.4, 4, null}, //
            new Object[]{null, null, null} //
        };

        final Object[][] mappedValues = new Object[][]{ //
            new Object[]{null, null, null}, //
            new Object[]{null, null, null}, //
            new Object[]{null, null, null}, //
            new Object[]{null, null, null}, //
            new Object[]{null, null, null} //
        };

        final MapperSpec spec = new MapperSpec() {

            @Override
            public ColumnarSchema getOutputSchema() {
                return schema;
            }

            @Override
            public int[] getColumnSelection() {
                return null;
            }

            @Override
            public BiConsumer<ReadAccessRow, WriteAccessRow> create() {
                return (r, w) -> {
                };
            }
        };

        testMapTable(schema, mappedValues, schema, originalValues, spec);
    }

    // TODO this test is testing completeness of test backend.
    //    @Test
    //    public void testAllDataSpecsSupported() throws IOException {
    //        // TODO: test or drop support for temporal types
    //        final ColumnarSchema schema = new DefaultColumnarSchema( //
    //            BooleanDataSpec.INSTANCE, //
    //            ByteDataSpec.INSTANCE, //
    //            DoubleDataSpec.INSTANCE, //
    //            IntDataSpec.INSTANCE, //
    //            new ListDataSpec(IntDataSpec.INSTANCE), //
    //            LongDataSpec.INSTANCE, //
    //            StringDataSpec.INSTANCE, //
    //            new StructDataSpec(DoubleDataSpec.INSTANCE, //
    //                IntDataSpec.INSTANCE, StringDataSpec.INSTANCE), //
    //            VarBinaryDataSpec.INSTANCE, //
    //            VoidDataSpec.INSTANCE //
    //        );
    //        final Object[][] values = new Object[DataSpecToTestDataMapper.NUM_TEST_VALUES][schema.numColumns()];
    //        for (int c = 0; c < schema.numColumns(); c++) {
    //            final Object[] columnValues = schema.getSpec(c).accept(DataSpecToTestDataMapper.INSTANCE);
    //            for (int r = 0; r < values.length; r++) {
    //                values[r][c] = columnValues[r];
    //            }
    //        }
    //        final MapperSpec spec = new MapperSpec() {
    //
    //            @Override
    //            public ColumnarSchema getOutputSchema() {
    //                return schema;
    //            }
    //
    //            @Override
    //            public int[] getColumnSelection() {
    //                return null;
    //            }
    //
    //            @Override
    //            public BiConsumer<ReadAccessRow, WriteAccessRow> create() {
    //                return (r, w) -> w.setFrom(r);
    //            }
    //        };
    //
    //        testMapTable(schema, values, schema, values, spec);
    //    }

    private static void testMapTable(final ColumnarSchema expectedSchema, final Object[][] expectedValues,
        final ColumnarSchema originalSchema, final Object[][] originalValues, final MapperSpec mapper)
        throws IOException {
        try (final RowAccessible originalTable = createRowAccessibleFromRowWiseValues(originalSchema, originalValues)) {
            @SuppressWarnings("resource")
            RowAccessible result = RowAccessibles.map(originalTable, mapper);
            RowAccessiblesTestUtils.assertRowAccessibleEquals(result, expectedSchema, expectedValues);
        }
    }

//    private static final class DataSpecToTestDataMapper implements DataSpec.Mapper<Object[]> {
//
//        private static final DataSpecToTestDataMapper INSTANCE = new DataSpecToTestDataMapper();
//
//        private static final int NUM_TEST_VALUES = 5;
//
//        @Override
//        public Object[] visit(final BooleanDataSpec spec) {
//            return new Object[]{null, false, true, false, true};
//        }
//
//        @Override
//        public Object[] visit(final ByteDataSpec spec) {
//            return new Object[]{(byte)11, null, (byte)33, (byte)44, (byte)55};
//        }
//
//        @Override
//        public Object[] visit(final DoubleDataSpec spec) {
//            return new Object[]{0.1, 0.2, null, 0.4, 0.5};
//        }
//
//        @Override
//        public Object[] visit(final DurationDataSpec spec) {
//            throw new IllegalStateException("not yet implemented"); // TODO: implement
//        }
//
//        @Override
//        public Object[] visit(final FloatDataSpec spec) {
//            return new Object[]{0.01f, 0.02f, 0.03f, null, 0.05f};
//        }
//
//        @Override
//        public Object[] visit(final IntDataSpec spec) {
//            return new Object[]{1, 2, 3, 4, null};
//        }
//
//        @Override
//        public Object[] visit(final LocalDateDataSpec spec) {
//            throw new IllegalStateException("not yet implemented"); // TODO: implement
//        }
//
//        @Override
//        public Object[] visit(final LocalDateTimeDataSpec spec) {
//            throw new IllegalStateException("not yet implemented"); // TODO: implement
//        }
//
//        @Override
//        public Object[] visit(final LocalTimeDataSpec spec) {
//            throw new IllegalStateException("not yet implemented"); // TODO: implement
//        }
//
//        @Override
//        public Object[] visit(final LongDataSpec spec) {
//            return new Object[]{null, 20l, 30l, 40l, 50l};
//        }
//
//        @Override
//        public Object[] visit(final PeriodDataSpec spec) {
//            throw new IllegalStateException("not yet implemented"); // TODO: implement
//        }
//
//        @Override
//        public Object[] visit(final VarBinaryDataSpec spec) {
//            return new byte[][]{ //
//                new byte[]{1, 2, 3, 4}, //
//                null, //
//                new byte[]{9, 0, 1, 2}, //
//                new byte[]{3, 4, 5, 6}, //
//                new byte[]{7, 8, 9, 0} //
//            };
//        }
//
//        @Override
//        public Object[] visit(final VoidDataSpec spec) {
//            return new Object[]{null, null, null, null, null};
//        }
//
//        @Override
//        public Object[] visit(final StructDataSpec spec) {
//            return new Object[][]{ //
//                new Object[]{0.1, 1, "First"}, //
//                new Object[]{0.2, 2, "Second"}, //
//                null, //
//                new Object[]{0.4, 4, "Fourth"}, //
//                new Object[]{0.5, 5, "Fifth"} //
//            };
//        }
//
//        @Override
//        public Object[] visit(final ListDataSpec listDataSpec) {
//            return new Object[][]{ //
//                new Object[]{1, 2, 3, 4}, //
//                new Object[]{5, 6, 7, 8}, //
//                new Object[]{9, 0, 1, 2}, //
//                null, //
//                new Object[]{7, 8, 9, 0} //
//            };
//        }
//
//        @Override
//        public Object[] visit(final ZonedDateTimeDataSpec spec) {
//            throw new IllegalStateException("not yet implemented"); // TODO: implement
//        }
//
//        @Override
//        public Object[] visit(final StringDataSpec spec) {
//            return new String[]{"First", "Second", "Third", "Fourth", null};
//        }
//    }
}
