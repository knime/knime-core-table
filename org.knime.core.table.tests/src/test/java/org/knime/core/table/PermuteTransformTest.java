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

import static org.knime.core.table.schema.DataSpecs.BOOLEAN;
import static org.knime.core.table.schema.DataSpecs.BYTE;
import static org.knime.core.table.schema.DataSpecs.DOUBLE;
import static org.knime.core.table.schema.DataSpecs.FLOAT;
import static org.knime.core.table.schema.DataSpecs.INT;
import static org.knime.core.table.schema.DataSpecs.LONG;
import static org.knime.core.table.schema.DataSpecs.STRING;
import static org.knime.core.table.schema.DataSpecs.VARBINARY;
import static org.knime.core.table.schema.DataSpecs.VOID;

import java.io.IOException;

import org.junit.Test;
import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.virtual.ColumnarSchemas;
import org.knime.core.table.virtual.RowAccessibles;
import org.knime.core.table.virtual.spec.PermuteTransformSpec;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings({"javadoc", "static-method"})
public final class PermuteTransformTest {

    @Test
    public void testIdentityPermutation() throws IOException {
        final ColumnarSchema schema = ColumnarSchema.of(DOUBLE, INT, STRING);
        final Object[][] values = new Object[][]{ //
            new Object[]{0.1, 1, "First"}, //
            new Object[]{0.2, 2, "Second"}, //
            new Object[]{0.3, 3, "Third"}, //
            new Object[]{0.4, 4, "Fourth"}, //
            new Object[]{0.5, 5, "Fifth"} //
        };
        final int[] permutation = {0, 1, 2};

        testPermuteTable(schema, values, schema, values, permutation);
    }

    @Test
    public void testSwapFirstAndLastColumns() throws IOException {
        final ColumnarSchema originalSchema = ColumnarSchema.of(DOUBLE, INT, STRING);
        final Object[][] originalValues = new Object[][]{ //
            new Object[]{0.1, 1, "First"}, //
            new Object[]{0.2, 2, "Second"}, //
            new Object[]{0.3, 3, "Third"}, //
            new Object[]{0.4, 4, "Fourth"}, //
            new Object[]{0.5, 5, "Fifth"} //
        };
        final int[] permutation = {2, 1, 0};

        final ColumnarSchema permutedSchema = ColumnarSchema.of(STRING, INT, DOUBLE);
        final Object[][] permutedValues = new Object[][]{ //
            new Object[]{"First", 1, 0.1}, //
            new Object[]{"Second", 2, 0.2}, //
            new Object[]{"Third", 3, 0.3}, //
            new Object[]{"Fourth", 4, 0.4}, //
            new Object[]{"Fifth", 5, 0.5} //
        };

        testPermuteTable(permutedSchema, permutedValues, originalSchema, originalValues, permutation);
    }

    @Test
    public void testPermuteAroundHalfOfTheColumns() throws IOException {
        final ColumnarSchema originalSchema =
                ColumnarSchema.of(DOUBLE, INT, STRING, BOOLEAN, FLOAT, LONG, BYTE, VARBINARY, VOID);
        final Object[][] originalValues = new Object[][]{ //
            new Object[]{0.1, 1, "First", true, 0.01f, 10l, (byte)11, new byte[]{1, 2, 3, 4}, null}, //
            new Object[]{0.2, 2, "Second", false, 0.02f, 20l, (byte)22, new byte[]{5, 6, 7, 8}, null}, //
            new Object[]{0.3, 3, "Third", true, 0.03f, 30l, (byte)33, new byte[]{9, 0, 1, 2}, null}, //
            new Object[]{0.4, 4, "Fourth", false, 0.04f, 40l, (byte)44, new byte[]{3, 4, 5, 6}, null}, //
            new Object[]{0.5, 5, "Fifth", true, 0.05f, 50l, (byte)55, new byte[]{7, 8, 9, 0}, null} //
        };
        final int[] permutation = {0, 2, 1, 3, 6, 5, 7, 4, 8};

        final ColumnarSchema permutedSchema =
                ColumnarSchema.of(DOUBLE, STRING, INT, BOOLEAN, BYTE, LONG, VARBINARY, FLOAT, VOID);
        final Object[][] permutedValues = new Object[][]{ //
            new Object[]{0.1, "First", 1, true, (byte)11, 10l, new byte[]{1, 2, 3, 4}, 0.01f, null}, //
            new Object[]{0.2, "Second", 2, false, (byte)22, 20l, new byte[]{5, 6, 7, 8}, 0.02f, null}, //
            new Object[]{0.3, "Third", 3, true, (byte)33, 30l, new byte[]{9, 0, 1, 2}, 0.03f, null}, //
            new Object[]{0.4, "Fourth", 4, false, (byte)44, 40l, new byte[]{3, 4, 5, 6}, 0.04f, null}, //
            new Object[]{0.5, "Fifth", 5, true, (byte)55, 50l, new byte[]{7, 8, 9, 0}, 0.05f, null} //
        };

        testPermuteTable(permutedSchema, permutedValues, originalSchema, originalValues, permutation);
    }

    @Test
    public void testShiftAllColumnsToTheLeft() throws IOException {
        final ColumnarSchema originalSchema =
                ColumnarSchema.of(DOUBLE, INT, STRING, BOOLEAN, FLOAT, LONG, BYTE, VARBINARY, VOID);
        final Object[][] originalValues = new Object[][]{ //
            new Object[]{0.1, 1, "First", true, 0.01f, 10l, (byte)11, new byte[]{1, 2, 3, 4}, null}, //
            new Object[]{0.2, 2, "Second", false, 0.02f, 20l, (byte)22, new byte[]{5, 6, 7, 8}, null}, //
            new Object[]{0.3, 3, "Third", true, 0.03f, 30l, (byte)33, new byte[]{9, 0, 1, 2}, null}, //
            new Object[]{0.4, 4, "Fourth", false, 0.04f, 40l, (byte)44, new byte[]{3, 4, 5, 6}, null}, //
            new Object[]{0.5, 5, "Fifth", true, 0.05f, 50l, (byte)55, new byte[]{7, 8, 9, 0}, null} //
        };
        final int[] permutation = {1, 2, 3, 4, 5, 6, 7, 8, 0};

        final ColumnarSchema permutedSchema =
                ColumnarSchema.of(INT, STRING, BOOLEAN, FLOAT, LONG, BYTE, VARBINARY, VOID, DOUBLE);
        final Object[][] permutedValues = new Object[][]{ //
            new Object[]{1, "First", true, 0.01f, 10l, (byte)11, new byte[]{1, 2, 3, 4}, null, 0.1}, //
            new Object[]{2, "Second", false, 0.02f, 20l, (byte)22, new byte[]{5, 6, 7, 8}, null, 0.2}, //
            new Object[]{3, "Third", true, 0.03f, 30l, (byte)33, new byte[]{9, 0, 1, 2}, null, 0.3}, //
            new Object[]{4, "Fourth", false, 0.04f, 40l, (byte)44, new byte[]{3, 4, 5, 6}, null, 0.4}, //
            new Object[]{5, "Fifth", true, 0.05f, 50l, (byte)55, new byte[]{7, 8, 9, 0}, null, 0.5} //
        };

        testPermuteTable(permutedSchema, permutedValues, originalSchema, originalValues, permutation);
    }

    @SuppressWarnings("unused")
    @Test(expected = IndexOutOfBoundsException.class)
    public void testRejectNegativePermutationIndices() {
        new PermuteTransformSpec(new int[]{4, 3, 2, -1, 0});
    }

    @SuppressWarnings("unused")
    @Test(expected = IllegalArgumentException.class)
    public void testRejectDuplicatePermutationIndices() {
        new PermuteTransformSpec(new int[]{4, 3, 3, 2, 1, 0});
    }

    @SuppressWarnings("unused")
    @Test(expected = IllegalArgumentException.class)
    public void testRejectPermutationIndicesWithHoles() {
        new PermuteTransformSpec(new int[]{4, 3, 2, 0});
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRejectNumPermutationIndicesGreaterThanNumColumns() {
        ColumnarSchemas.permute(ColumnarSchema.of(DOUBLE, INT), new int[]{2, 1, 0});
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRejectNumPermutationIndicesLessThanNumColumns() {
        ColumnarSchemas.permute(ColumnarSchema.of(DOUBLE, INT, STRING), new int[]{1, 0});
    }

    private static void testPermuteTable(final ColumnarSchema expectedSchema, final Object[][] expectedValues,
        final ColumnarSchema originalSchema, final Object[][] originalValues, final int[] permutation)
        throws IOException {
        @SuppressWarnings("resource")
        final RowAccessible originalTable =
            RowAccessiblesTestUtils.createRowAccessibleFromRowWiseValues(originalSchema, originalValues);
        @SuppressWarnings("resource")
        final RowAccessible permuted = RowAccessibles.permute(originalTable, permutation);
        RowAccessiblesTestUtils.assertRowAccessibleEquals(permuted, expectedSchema, expectedValues);
    }
}
