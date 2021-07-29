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
import static org.knime.core.table.schema.DataSpecs.DOUBLE;
import static org.knime.core.table.schema.DataSpecs.INT;
import static org.knime.core.table.schema.DataSpecs.STRING;

import java.io.IOException;

import org.junit.Test;
import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.virtual.DefaultRowRangeSelection;
import org.knime.core.table.virtual.RowAccessibles;
import org.knime.core.table.virtual.spec.SliceTransformSpec;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings({"javadoc", "static-method"})
public final class SliceTransformTest {

    @Test
    public void testSliceEntireTable() throws IOException {
        final ColumnarSchema schema = ColumnarSchema.of(DOUBLE, INT, STRING);
        final Object[][] values = new Object[][]{ //
            new Object[]{0.1, 1, "First"}, //
            new Object[]{0.2, 2, "Second"}, //
            new Object[]{0.3, 3, "Third"}, //
            new Object[]{0.4, 4, "Fourth"}, //
            new Object[]{0.5, 5, "Fifth"} //
        };

        testSliceTable(schema, values, values, 0, 5);
    }

    @Test
    public void testSliceEntireTableWithToIndexGreaterThanTableSize() throws IOException {
        final ColumnarSchema schema = ColumnarSchema.of(DOUBLE, INT, STRING);
        final Object[][] values = new Object[][]{ //
            new Object[]{0.1, 1, "First"}, //
            new Object[]{0.2, 2, "Second"}, //
            new Object[]{0.3, 3, "Third"}, //
            new Object[]{0.4, 4, "Fourth"}, //
            new Object[]{0.5, 5, "Fifth"} //
        };

        testSliceTable(schema, values, values, 0, Long.MAX_VALUE);
    }

    @Test
    public void testSliceAllButFirstRow() throws IOException {
        final ColumnarSchema schema = ColumnarSchema.of(DOUBLE, INT, STRING);
        final Object[][] originalValues = new Object[][]{ //
            new Object[]{0.1, 1, "First"}, //
            new Object[]{0.2, 2, "Second"}, //
            new Object[]{0.3, 3, "Third"}, //
            new Object[]{0.4, 4, "Fourth"}, //
            new Object[]{0.5, 5, "Fifth"} //
        };

        final Object[][] slicedValues = new Object[][]{ //
            new Object[]{0.2, 2, "Second"}, //
            new Object[]{0.3, 3, "Third"}, //
            new Object[]{0.4, 4, "Fourth"}, //
            new Object[]{0.5, 5, "Fifth"} //
        };

        testSliceTable(schema, slicedValues, originalValues, 1, 5);
    }

    @Test
    public void testSliceAllButLastRow() throws IOException {
        final ColumnarSchema schema = ColumnarSchema.of(DOUBLE, INT, STRING);
        final Object[][] originalValues = new Object[][]{ //
            new Object[]{0.1, 1, "First"}, //
            new Object[]{0.2, 2, "Second"}, //
            new Object[]{0.3, 3, "Third"}, //
            new Object[]{0.4, 4, "Fourth"}, //
            new Object[]{0.5, 5, "Fifth"} //
        };

        final Object[][] slicedValues = new Object[][]{ //
            new Object[]{0.1, 1, "First"}, //
            new Object[]{0.2, 2, "Second"}, //
            new Object[]{0.3, 3, "Third"}, //
            new Object[]{0.4, 4, "Fourth"}, //
        };

        testSliceTable(schema, slicedValues, originalValues, 0, 4);
    }

    @Test
    public void testSliceSingleRow() throws IOException {
        final ColumnarSchema schema = ColumnarSchema.of(DOUBLE, INT, STRING);
        final Object[][] originalValues = new Object[][]{ //
            new Object[]{0.1, 1, "First"}, //
            new Object[]{0.2, 2, "Second"}, //
            new Object[]{0.3, 3, "Third"}, //
            new Object[]{0.4, 4, "Fourth"}, //
            new Object[]{0.5, 5, "Fifth"} //
        };

        final Object[][] slicedValues = new Object[][]{ //
            new Object[]{0.3, 3, "Third"} //
        };

        testSliceTable(schema, slicedValues, originalValues, 2, 3);
    }

    @Test
    public void testSliceHalfOfTheTable() throws IOException {
        final ColumnarSchema schema = ColumnarSchema.of(DOUBLE, INT, STRING);
        final Object[][] originalValues = new Object[][]{ //
            new Object[]{0.1, 1, "First"}, //
            new Object[]{0.2, 2, "Second"}, //
            new Object[]{0.3, 3, "Third"}, //
            new Object[]{0.4, 4, "Fourth"}, //
            new Object[]{0.5, 5, "Fifth"}, //
            new Object[]{0.6, 6, "Sixth"}, //
            new Object[]{0.7, 7, "Seventh"}, //
            new Object[]{0.8, 8, "Eighth"}, //
            new Object[]{0.9, 9, "Ninth"}, //
            new Object[]{0.0, 0, "Tenth"} //
        };

        final Object[][] slicedValues = new Object[][]{ //
            new Object[]{0.6, 6, "Sixth"}, //
            new Object[]{0.7, 7, "Seventh"}, //
            new Object[]{0.8, 8, "Eighth"}, //
            new Object[]{0.9, 9, "Ninth"}, //
            new Object[]{0.0, 0, "Tenth"} //
        };

        testSliceTable(schema, slicedValues, originalValues, 5, 10);
    }

    @Test
    public void testSliceNothing() throws IOException {
        final ColumnarSchema schema = ColumnarSchema.of(DOUBLE, INT, STRING);
        final Object[][] originalValues = new Object[][]{ //
            new Object[]{0.1, 1, "First"}, //
            new Object[]{0.2, 2, "Second"}, //
            new Object[]{0.3, 3, "Third"}, //
            new Object[]{0.4, 4, "Fourth"}, //
            new Object[]{0.5, 5, "Fifth"} //
        };

        final Object[][] slicedValues = new Object[0][0];

        testSliceTable(schema, slicedValues, originalValues, 2, 2);
    }

    @Test
    public void testSliceNothingWithFromIndexGreaterThanToIndex() throws IOException {
        final ColumnarSchema schema = ColumnarSchema.of(DOUBLE, INT, STRING);
        final Object[][] originalValues = new Object[][]{ //
            new Object[]{0.1, 1, "First"}, //
            new Object[]{0.2, 2, "Second"}, //
            new Object[]{0.3, 3, "Third"}, //
            new Object[]{0.4, 4, "Fourth"}, //
            new Object[]{0.5, 5, "Fifth"} //
        };

        final Object[][] slicedValues = new Object[0][0];

        testSliceTable(schema, slicedValues, originalValues, 1, 0);
    }

    @SuppressWarnings("unused")
    @Test(expected = IndexOutOfBoundsException.class)
    public void testRejectNegativeFromIndex() {
        new SliceTransformSpec(-1, 1);
    }

    @SuppressWarnings("unused")
    @Test(expected = IndexOutOfBoundsException.class)
    public void testRejectNegativeToIndex() {
        new SliceTransformSpec(0, -1);
    }

    private static void testSliceTable(final ColumnarSchema expectedAndOriginalSchema, final Object[][] expectedValues,
        final Object[][] originalValues, final long sliceFrom, final long sliceTo) throws IOException {
        try (final RowAccessible originalTable =
            createRowAccessibleFromRowWiseValues(expectedAndOriginalSchema, originalValues)) {
            @SuppressWarnings("resource")
            final RowAccessible result =
                RowAccessibles.slice(originalTable, new DefaultRowRangeSelection(sliceFrom, sliceTo));
            RowAccessiblesTestUtils.assertRowAccessibleEquals(result, expectedAndOriginalSchema, expectedValues);
        }
    }
}
