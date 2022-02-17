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
 *   Apr 24, 2021 (marcel): created
 */
package org.knime.core.table;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.knime.core.table.schema.AnnotatedColumnarSchema;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.schema.DataSpecs;
import org.knime.core.table.schema.DefaultAnnotatedColumnarSchema;
import org.knime.core.table.schema.traits.DataTrait.DictEncodingTrait.KeyType;
import org.knime.core.table.virtual.serialization.AnnotatedColumnarSchemaSerializer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

/**
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings({"javadoc", "static-method"})
public final class AnnotatedColumnarSchemaSerializerTest {

    private static AnnotatedColumnarSchema createTestData() {
        final var schema = ColumnarSchema.of(DataSpecs.STRING(DataSpecs.DICT_ENCODING), //
            DataSpecs.INT(DataSpecs.LOGICAL_TYPE("test")), //
            DataSpecs.DOUBLE, //
            DataSpecs.VARBINARY, //
            DataSpecs.LIST().of(DataSpecs.LONG), //
            DataSpecs.STRUCT(DataSpecs.LOGICAL_TYPE("struct")).of(//
                DataSpecs.INT, //
                DataSpecs.STRING(DataSpecs.DICT_ENCODING(KeyType.BYTE_KEY)))//
        );

        final var columnNames = new String[]{"Strings", "Ints", "Doubles", "Blobs", "LongList", "StructsOfStuff"};
        return DefaultAnnotatedColumnarSchema.annotate(schema, columnNames);
    }

    @Test
    public void testAnnotatedColumnarSchemaSerializationRoundtrip() {
        var testData = createTestData();
        final JsonNode config = AnnotatedColumnarSchemaSerializer.save(testData, JsonNodeFactory.instance);
        final var deserialized = AnnotatedColumnarSchemaSerializer.load(config);
        assertEquals(testData.getColumnarSchema(), deserialized.getColumnarSchema());
        assertEquals(testData, deserialized);
    }
}
