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
import org.knime.core.table.access.IntAccess;
import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.virtual.RowAccessibles;
import org.knime.core.table.virtual.spec.RowFilterTransformSpec.RowFilterFactory;

/**
 * @author Tobias Pietzsch
 */
@SuppressWarnings({"javadoc", "static-method"})
public final class RowFilterTransformTest {

    @Test
    public void testSkipIfNegative() throws IOException {
        final ColumnarSchema schema = ColumnarSchema.of(DOUBLE, INT, STRING);
        final Object[][] values = new Object[][]{ //
            new Object[]{0.1, 1, "First"}, //
            new Object[]{0.2, -2, "Second"}, //
            new Object[]{0.3, 3, "Third"}, //
            new Object[]{0.4, -1, "Fourth"} //
        };

        final int[] columnIndices = {1};
        final RowFilterFactory filterFactory = inputs -> {
            final IntAccess.IntReadAccess i0 = (IntAccess.IntReadAccess)inputs[0];
            return () -> i0.getIntValue() >= 0;
        };

        final ColumnarSchema expectedSchema = schema;
        final Object[][] expectedValues = new Object[][] { //
                new Object[]{0.1, 1, "First"}, //
//                new Object[]{0.2, -2, "Second"}, //
                new Object[]{0.3, 3, "Third"}, //
//                new Object[]{0.4, -1, "Fourth"} //
        };

        try (final RowAccessible originalTable = createRowAccessibleFromRowWiseValues(schema, values)) {
            @SuppressWarnings("resource")
            final RowAccessible result = RowAccessibles.filterRows(originalTable, columnIndices, filterFactory);
            RowAccessiblesTestUtils.assertRowAccessibleEquals(result, expectedSchema, expectedValues);
        }
    }

    // TODO: reject invalid columnIndices

}
