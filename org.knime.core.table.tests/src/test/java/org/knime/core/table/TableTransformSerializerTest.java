/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME AG, Zurich, Switzerland
 *
 * You may not modify, publish, transmit, transfer or sell, reproduce,
 * create derivative works from, distribute, perform, display, or in
 * any way exploit any of the content, in whole or in part, except as
 * otherwise expressly permitted in writing by the copyright owner or
 * as specified in the license file distributed with this product.
 *
 * If you have any questions please contact the copyright holder:
 * website: www.knime.com
 * email: contact@knime.com
 * ---------------------------------------------------------------------
 *
 * History
 *   Created on Jul 2, 2021 by marcel
 */
package org.knime.core.table;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.knime.core.table.schema.DataSpecs.BOOLEAN;
import static org.knime.core.table.schema.DataSpecs.DOUBLE;
import static org.knime.core.table.schema.DataSpecs.INT;
import static org.knime.core.table.schema.DataSpecs.STRING;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.virtual.TableTransform;
import org.knime.core.table.virtual.VirtualTable;
import org.knime.core.table.virtual.serialization.TableTransformSerializer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("javadoc")
@RunWith(Parameterized.class)
public final class TableTransformSerializerTest {

    // TODO: example graphs are taken from VirtualTableTest. Consolidate creation (e.g. via a centralized "graph zoo").
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> createTableTransformSpecsToTest() {
        final List<Object[]> params = new ArrayList<>();

        // Linear graph

        add(params, "Linear graph",
            new VirtualTable(UUID.randomUUID(),
                    ColumnarSchema.of(DOUBLE, INT, STRING)) //
                    .slice(2, 7) //
                    .filterColumns(0, 2) //
                    .getProducingTransform() //
        );

        // Joining graph

        final VirtualTable transformedTable1 = new VirtualTable(UUID.randomUUID(),
                ColumnarSchema.of(DOUBLE, INT)) //
                .filterColumns(0);
        final VirtualTable transformedTable2 = new VirtualTable(UUID.randomUUID(),
                ColumnarSchema.of(STRING, BOOLEAN)) //
                .slice(3, 6);
        final TableTransform joiningGraph = transformedTable1.append(Arrays.asList(transformedTable2)) //
            .getProducingTransform();
        add(params, "Joining graph", joiningGraph);

        // Forking and joining graph

        final VirtualTable transformedTable = new VirtualTable(UUID.randomUUID(),
                ColumnarSchema.of(DOUBLE, INT, STRING)) //
                .slice(1, 6);
        final VirtualTable forkedTable1 = transformedTable.filterColumns(1);
        final VirtualTable forkedTable2 = transformedTable.permute(2, 1, 0);
        final TableTransform forkingJoiningGraph = forkedTable1.append(Arrays.asList(forkedTable2)) //
            .getProducingTransform();
        add(params, "Forking and joining graph", forkingJoiningGraph);

        return params;
    }

    private static void add(final List<Object[]> params, final String name, final TableTransform transform) {
        params.add(new Object[]{name, transform});
    }

    @Parameterized.Parameter(0)
    public String m_graphName;

    @Parameterized.Parameter(1)
    public TableTransform m_transform;

    @Test
    public void testTableTransformSerializationRoundtrip() {
        final JsonNode config = TableTransformSerializer.save(m_transform, JsonNodeFactory.instance);
        final TableTransform deserialized = TableTransformSerializer.load(config);
        assertNotSame(m_transform, deserialized);
        assertEquals(m_transform, deserialized);
    }
}
