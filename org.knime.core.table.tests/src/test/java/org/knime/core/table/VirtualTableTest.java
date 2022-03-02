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
 *   May 12, 2021 (marcel): created
 */
package org.knime.core.table;

import static org.junit.Assert.assertEquals;
import static org.knime.core.table.RowAccessiblesTestUtils.assertTableEqualsValues;
import static org.knime.core.table.schema.DataSpecs.BOOLEAN;
import static org.knime.core.table.schema.DataSpecs.DOUBLE;
import static org.knime.core.table.schema.DataSpecs.INT;
import static org.knime.core.table.schema.DataSpecs.STRING;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.Test;
import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.virtual.VirtualTable;
import org.knime.core.table.virtual.exec.LazyVirtualTableExecutor;
import org.knime.core.table.virtual.exec.VirtualTableExecutor;

/**
 * Integration test for different combinations of table transformations. Refer to the other tests in this package for
 * more isolated unit tests of the individual transformations.
 *
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings({"javadoc", "static-method"})
public final class VirtualTableTest {

    @Test
    public void testLinearTransformGraph() throws IOException {
        final ColumnarSchema originalSchema = ColumnarSchema.of(DOUBLE, INT, STRING);
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

        final ColumnarSchema expectedTransformedSchema = ColumnarSchema.of(DOUBLE, STRING);
        final Object[][] expectedTransformedValues = new Object[][]{ //
            new Object[]{0.3, "Third"}, //
            new Object[]{0.4, "Fourth"}, //
            new Object[]{0.5, "Fifth"}, //
            new Object[]{0.6, "Sixth"}, //
            new Object[]{0.7, "Seventh"}, //
        };

        try (final RowAccessible originalTable =
            RowAccessiblesTestUtils.createRowAccessibleFromRowWiseValues(originalSchema, originalValues)) {
            final UUID sourceIdentifier = UUID.randomUUID();
            final VirtualTable transformedTable = new VirtualTable(sourceIdentifier, originalTable.getSchema()) //
                .slice(2, 7) //
                .filterColumns(0, 2);
            testTransformedTable(expectedTransformedSchema, expectedTransformedValues,
                Map.of(sourceIdentifier, originalTable), transformedTable);
        }
    }

    @Test
    public void testJoiningTransformGraph() throws IOException {
        final ColumnarSchema originalSchema1 = ColumnarSchema.of(DOUBLE, INT);
        final Object[][] originalValues1 = new Object[][]{ //
            new Object[]{0.1, 1}, //
            new Object[]{0.2, 2}, //
            new Object[]{0.3, 3}, //
            new Object[]{0.4, 4}, //
            new Object[]{0.5, 5}, //
            new Object[]{0.6, 6}, //
            new Object[]{0.7, 7}, //
            new Object[]{0.8, 8}, //
            new Object[]{0.9, 9}, //
            new Object[]{0.0, 0} //
        };
        final ColumnarSchema originalSchema2 = ColumnarSchema.of(STRING, BOOLEAN);
        final Object[][] originalValues2 = new Object[][]{ //
            new Object[]{"First", true}, //
            new Object[]{"Second", false}, //
            new Object[]{"Third", true}, //
            new Object[]{"Fourth", false}, //
            new Object[]{"Fifth", true}, //
            new Object[]{"Sixth", false}, //
            new Object[]{"Seventh", true}, //
            new Object[]{"Eighth", false}, //
            new Object[]{"Ninth", true}, //
            new Object[]{"Tenth", false} //
        };

        final ColumnarSchema expectedTransformedSchema = ColumnarSchema.of(DOUBLE, STRING, BOOLEAN);
        final Object[][] expectedTransformedValues = new Object[][]{ //
            new Object[]{0.1, "Fourth", false}, //
            new Object[]{0.2, "Fifth", true}, //
            new Object[]{0.3, "Sixth", false}, //
            new Object[]{0.4, null, null}, //
            new Object[]{0.5, null, null}, //
            new Object[]{0.6, null, null}, //
            new Object[]{0.7, null, null}, //
            new Object[]{0.8, null, null}, //
            new Object[]{0.9, null, null}, //
            new Object[]{0.0, null, null} //
        };

        try (final RowAccessible originalTable1 =
            RowAccessiblesTestUtils.createRowAccessibleFromRowWiseValues(originalSchema1, originalValues1);
                final RowAccessible originalTable2 =
                    RowAccessiblesTestUtils.createRowAccessibleFromRowWiseValues(originalSchema2, originalValues2)) {
            final UUID sourceIdentifier1 = UUID.randomUUID();
            final VirtualTable transformedTable1 =
                new VirtualTable(sourceIdentifier1, originalTable1.getSchema()).filterColumns(0);
            final UUID sourceIdentifier2 = UUID.randomUUID();
            final VirtualTable transformedTable2 =
                new VirtualTable(sourceIdentifier2, originalTable2.getSchema()).slice(3, 6);
            final VirtualTable joinedTransformedTable = transformedTable1.append(Arrays.asList(transformedTable2));
            testTransformedTable(expectedTransformedSchema, expectedTransformedValues,
                Map.of(sourceIdentifier1, originalTable1, sourceIdentifier2, originalTable2), joinedTransformedTable);
        }
    }

    @Test
    public void testForkingAndJoiningTransformGraph() throws IOException {
        final ColumnarSchema originalSchema = ColumnarSchema.of(DOUBLE, INT, STRING);
        final Object[][] originalValues = new Object[][]{ //
            new Object[]{0.1, 1, "First"}, //
            new Object[]{0.2, 2, "Second"}, //
            new Object[]{0.3, 3, "Third"}, //
            new Object[]{0.4, 4, "Fourth"}, //
            new Object[]{0.5, 5, "Fifth"}, //
            new Object[]{0.6, 6, "Sixth"}, //
            new Object[]{0.7, 7, "Seventh"} //
        };

        final ColumnarSchema expectedTransformedSchema = ColumnarSchema.of(INT, STRING, INT, DOUBLE);
        final Object[][] expectedTransformedValues = new Object[][]{ //
            new Object[]{2, "Second", 2, 0.2}, //
            new Object[]{3, "Third", 3, 0.3}, //
            new Object[]{4, "Fourth", 4, 0.4}, //
            new Object[]{5, "Fifth", 5, 0.5}, //
            new Object[]{6, "Sixth", 6, 0.6} //
        };

        try (final RowAccessible originalTable =
            RowAccessiblesTestUtils.createRowAccessibleFromRowWiseValues(originalSchema, originalValues)) {
            final UUID sourceIdentifier = UUID.randomUUID();
            final VirtualTable transformedTable =
                new VirtualTable(sourceIdentifier, originalTable.getSchema()).slice(1, 6);
            final VirtualTable forkedTable1 = transformedTable.filterColumns(1);
            final VirtualTable forkedTable2 = transformedTable.permute(2, 1, 0);
            final VirtualTable joinedTransformedTable = forkedTable1.append(Arrays.asList(forkedTable2));
            testTransformedTable(expectedTransformedSchema, expectedTransformedValues,
                Map.of(sourceIdentifier, originalTable), joinedTransformedTable);
        }
    }

    private static void testTransformedTable(final ColumnarSchema expectedTransformedSchema,
        final Object[][] expectedTransformedValues, final Map<UUID, RowAccessible> sourceTables,
        final VirtualTable actualTransformedTable) throws IOException {

        // TODO only single sink execution possible in lazy mode
        final VirtualTableExecutor executor =
            new LazyVirtualTableExecutor(actualTransformedTable.getProducingTransform());
        final List<RowAccessible> result = executor.execute(sourceTables);

        try (final RowAccessible actualMaterializedTransformedTable = result.get(0)) {
            assertEquals(expectedTransformedSchema, actualTransformedTable.getSchema());
            assertTableEqualsValues(expectedTransformedValues, actualMaterializedTransformedTable, false);
            assertTableEqualsValues(expectedTransformedValues, actualMaterializedTransformedTable, true);
        }
    }
}
