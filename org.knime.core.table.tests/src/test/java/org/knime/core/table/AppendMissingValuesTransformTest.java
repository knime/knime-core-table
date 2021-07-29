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
 *   Apr 27, 2021 (marcel): created
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

import org.junit.Test;
import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.virtual.RowAccessibles;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings({"javadoc", "static-method"})
public final class AppendMissingValuesTransformTest {

    @Test
    public void testAppendMissingValues() throws IOException {
        final ColumnarSchema originalSchema = ColumnarSchema.of(DOUBLE, INT, STRING);
        final Object[][] originalValues = new Object[][]{ //
            new Object[]{0.1, 1, "First"}, //
            new Object[]{0.2, 2, "Second"}, //
            new Object[]{0.3, 3, "Third"}, //
            new Object[]{0.4, 4, "Fourth"}, //
            new Object[]{0.5, 5, "Fifth"} //
        };
        final ColumnarSchema missingValuesColumns = ColumnarSchema.of(BOOLEAN, FLOAT, LONG);
        final ColumnarSchema missingsAppendedSchema = ColumnarSchema.of(DOUBLE, INT, STRING, BOOLEAN, FLOAT, LONG);
        final Object[][] missingsAppendedValues = new Object[][]{ //
            new Object[]{0.1, 1, "First", null, null, null}, //
            new Object[]{0.2, 2, "Second", null, null, null}, //
            new Object[]{0.3, 3, "Third", null, null, null}, //
            new Object[]{0.4, 4, "Fourth", null, null, null}, //
            new Object[]{0.5, 5, "Fifth", null, null, null} //
        };

        testAppendMissingValues(missingsAppendedSchema, missingsAppendedValues, originalSchema, originalValues,
            missingValuesColumns);
    }

    private static void testAppendMissingValues(final ColumnarSchema expectedSchema, final Object[][] expectedValues,
        final ColumnarSchema originalSchema, final Object[][] originalValues, final ColumnarSchema missingValuesColumns)
        throws IOException {
        try (final RowAccessible originalTable = createRowAccessibleFromRowWiseValues(originalSchema, originalValues)) {
            @SuppressWarnings("resource")
            final RowAccessible result = RowAccessibles.appendMissing(originalTable, missingValuesColumns);
            RowAccessiblesTestUtils.assertRowAccessibleEquals(result, expectedSchema, expectedValues);
        }
    }
}
