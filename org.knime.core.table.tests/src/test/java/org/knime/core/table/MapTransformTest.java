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
import org.knime.core.table.access.DoubleAccess.DoubleReadAccess;
import org.knime.core.table.access.DoubleAccess.DoubleWriteAccess;
import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.virtual.RowAccessibles;
import org.knime.core.table.virtual.spec.MapTransformSpec.Map;

/**
 * @author Tobias Pietzsch
 */
@SuppressWarnings({"javadoc", "static-method"})
public final class MapTransformTest {

    @Test
    public void testAddOneToDoubleColumn() throws IOException {
        final ColumnarSchema schema = ColumnarSchema.of(DOUBLE, INT, STRING);
        final Object[][] values = new Object[][]{ //
            new Object[]{0.1, 1, "First"}, //
            new Object[]{0.2, 2, "Second"}, //
            new Object[]{0.3, 3, "Third"} //
        };

        final int[] columnIndices = {0};
        final ColumnarSchema outputSchema = ColumnarSchema.of(DOUBLE);
        final Map map =  (inputs, outputs) -> {
            final double value = ((DoubleReadAccess)inputs[0]).getDoubleValue();
            final double result = value + 1.0;
            ((DoubleWriteAccess)outputs[0]).setDoubleValue(result);
        };

        final ColumnarSchema expectedSchema = outputSchema;
        final Object[][] expectedValues = new Object[][] { //
                new Object[]{1.1}, //
                new Object[]{1.2}, //
                new Object[]{1.3} //
        };

        try (final RowAccessible originalTable = createRowAccessibleFromRowWiseValues(schema, values)) {
            @SuppressWarnings("resource")
            final RowAccessible result = RowAccessibles.map(originalTable, columnIndices, outputSchema, map);
            RowAccessiblesTestUtils.assertRowAccessibleEquals(result, outputSchema, expectedValues);
        }
    }

    // TODO: reject invalid columnIndices

}
