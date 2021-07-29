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
import static org.knime.core.table.schema.DataSpecs.BOOLEAN;
import static org.knime.core.table.schema.DataSpecs.DOUBLE;
import static org.knime.core.table.schema.DataSpecs.FLOAT;
import static org.knime.core.table.schema.DataSpecs.INT;
import static org.knime.core.table.schema.DataSpecs.LONG;
import static org.knime.core.table.schema.DataSpecs.STRING;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.virtual.ColumnarSchemas;
import org.knime.core.table.virtual.RowAccessibles;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings({"javadoc", "static-method"})
public final class ConcatenateTransformTest {

    // TODO: implement and test concatenation of tables of unequal data specs; figure out how to find a common base type
    // -- is there a mechanism that works for both physical and logical types? Let clients pass in a resolver?

    @Test
    public void testConcatenateTwoTables() throws IOException {
        final ColumnarSchema schema = ColumnarSchema.of(DOUBLE, INT, STRING);
        final Object[][] firstValues = new Object[][]{ //
            new Object[]{0.1, 1, "First"}, //
            new Object[]{0.2, 2, "Second"}, //
            new Object[]{0.3, 3, "Third"}, //
            new Object[]{0.4, 4, "Fourth"} //
        };
        final Object[][] secondValues = new Object[][]{ //
            new Object[]{0.5, 5, "Fifth"}, //
            new Object[]{0.6, 6, "Sixth"}, //
            new Object[]{0.7, 7, "Seventh"}, //
            new Object[]{0.8, 8, "Eighth"}, //
            new Object[]{0.9, 9, "Ninth"}, //
            new Object[]{0.0, 0, "Tenth"} //
        };

        final Object[][] concatenatedValues = new Object[][]{ //
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

        testConcatenateTables(schema, concatenatedValues, Arrays.asList(firstValues, secondValues));
    }

    @Test
    public void testConcatenateThreeTables() throws IOException {
        final ColumnarSchema schema = ColumnarSchema.of(BOOLEAN, FLOAT, LONG);
        final Object[][] firstValues = new Object[][]{ //
            new Object[]{true, 0.01f, 10l}, //
            new Object[]{false, 0.02f, 20l}, //
            new Object[]{true, 0.03f, 30l}, //
            new Object[]{false, 0.04f, 40l}, //
            new Object[]{true, 0.05f, 50l}, //
            new Object[]{false, 0.06f, 60l}, //
            new Object[]{true, 0.07f, 70l} //
        };
        final Object[][] secondValues = new Object[][]{ //
            new Object[]{false, 0.08f, 80l}, //
            new Object[]{true, 0.09f, 90l}, //
            new Object[]{false, 0.10f, 100l} //
        };
        final Object[][] thirdValues = new Object[][]{ //
            new Object[]{true, 0.11f, 110l} //
        };

        final Object[][] concatenatedValues = new Object[][]{ //
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

        testConcatenateTables(schema, concatenatedValues, Arrays.asList(firstValues, secondValues, thirdValues));
    }

    @Test
    public void testAppendReferenceIdenticalTables() throws IOException {
        final ColumnarSchema schema = ColumnarSchema.of(DOUBLE, INT, STRING);
        final Object[][] values = new Object[][]{ //
            new Object[]{0.1, 1, "First"}, //
            new Object[]{0.2, 2, "Second"}, //
            new Object[]{0.3, 3, "Third"}, //
            new Object[]{0.4, 4, "Fourth"}, //
            new Object[]{0.5, 5, "Fifth"} //
        };

        final Object[][] concatenatedValues = new Object[][]{ //
            new Object[]{0.1, 1, "First"}, //
            new Object[]{0.2, 2, "Second"}, //
            new Object[]{0.3, 3, "Third"}, //
            new Object[]{0.4, 4, "Fourth"}, //
            new Object[]{0.5, 5, "Fifth"}, //
            new Object[]{0.1, 1, "First"}, //
            new Object[]{0.2, 2, "Second"}, //
            new Object[]{0.3, 3, "Third"}, //
            new Object[]{0.4, 4, "Fourth"}, //
            new Object[]{0.5, 5, "Fifth"} //
        };

        testConcatenateTables(schema, concatenatedValues, Arrays.asList(values, values));
    }

    @Test
    public void testPreConcatenateZeroRowTable() throws IOException {
        final ColumnarSchema schema = ColumnarSchema.of(DOUBLE, INT, STRING);
        final Object[][] firstValues = new Object[0][0];
        final Object[][] secondValues = new Object[][]{ //
            new Object[]{0.1, 1, "First"}, //
            new Object[]{0.2, 2, "Second"}, //
            new Object[]{0.3, 3, "Third"}, //
            new Object[]{0.4, 4, "Fourth"}, //
            new Object[]{0.5, 5, "Fifth"} //
        };

        testConcatenateTables(schema, secondValues, Arrays.asList(firstValues, secondValues));
    }

    @Test
    public void testPostConcatenateZeroRowTable() throws IOException {
        final ColumnarSchema schema = ColumnarSchema.of(DOUBLE, INT, STRING);
        final Object[][] firstValues = new Object[][]{ //
            new Object[]{0.1, 1, "First"}, //
            new Object[]{0.2, 2, "Second"}, //
            new Object[]{0.3, 3, "Third"}, //
            new Object[]{0.4, 4, "Fourth"}, //
            new Object[]{0.5, 5, "Fifth"} //
        };
        final Object[][] secondValues = new Object[0][0];

        testConcatenateTables(schema, firstValues, Arrays.asList(firstValues, secondValues));
    }

    @Test
    public void testInsertConcatenateZeroRowTable() throws IOException {
        final ColumnarSchema schema = ColumnarSchema.of(DOUBLE, INT, STRING);
        final Object[][] firstValues = new Object[][]{ //
            new Object[]{0.1, 1, "First"}, //
            new Object[]{0.2, 2, "Second"}, //
            new Object[]{0.3, 3, "Third"}, //
            new Object[]{0.4, 4, "Fourth"}, //
            new Object[]{0.5, 5, "Fifth"} //
        };
        final Object[][] secondValues = new Object[0][0];
        final Object[][] thirdValues = new Object[][]{ //
            new Object[]{0.6, 6, "Sixth"}, //
            new Object[]{0.7, 7, "Seventh"} //
        };

        final Object[][] concatenatedValues = new Object[][]{ //
            new Object[]{0.1, 1, "First"}, //
            new Object[]{0.2, 2, "Second"}, //
            new Object[]{0.3, 3, "Third"}, //
            new Object[]{0.4, 4, "Fourth"}, //
            new Object[]{0.5, 5, "Fifth"}, //
            new Object[]{0.6, 6, "Sixth"}, //
            new Object[]{0.7, 7, "Seventh"} //
        };

        testConcatenateTables(schema, concatenatedValues, Arrays.asList(firstValues, secondValues, thirdValues));
    }

    @Test
    public void testConcatenateOnlyZeroRowTables() throws IOException {
        final ColumnarSchema schema = ColumnarSchema.of(DOUBLE, INT, STRING);
        final Object[][] values = new Object[0][0];

        testConcatenateTables(schema, values, Arrays.asList(values, values));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRejectIncompatibleSchemas() {
        ColumnarSchemas.concatenate(Arrays.asList( //
                ColumnarSchema.of(DOUBLE, INT), //
                ColumnarSchema.of(DOUBLE, STRING) //
        ));
    }

    private static void testConcatenateTables(final ColumnarSchema expectedAndOriginalSchema,
        final Object[][] expectedValues, final List<Object[][]> valuesToConcatenate) throws IOException {
        final List<ColumnarSchema> schemasToConcatenate = new ArrayList<>();
        final List<RowAccessible> tablesToConcatenate = new ArrayList<>();
        for (int i = 0; i < valuesToConcatenate.size(); i++) {
            schemasToConcatenate.add(expectedAndOriginalSchema);
            @SuppressWarnings("resource") // Closed in finally-block below.
            final RowAccessible table =
                createRowAccessibleFromRowWiseValues(expectedAndOriginalSchema, valuesToConcatenate.get(i));
            tablesToConcatenate.add(table);
        }

        @SuppressWarnings("resource")
        final RowAccessible result = RowAccessibles.concatenate(tablesToConcatenate.get(0),
            tablesToConcatenate.subList(1, tablesToConcatenate.size()).toArray(RowAccessible[]::new));

        RowAccessiblesTestUtils.assertRowAccessibleEquals(result, expectedAndOriginalSchema, expectedValues);
    }
}
