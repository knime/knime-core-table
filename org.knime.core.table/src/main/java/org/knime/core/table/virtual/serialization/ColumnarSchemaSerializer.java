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
 *   Created on May 17, 2021 by marcel
 */
package org.knime.core.table.virtual.serialization;

import java.util.ArrayList;
import java.util.List;

import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.schema.DefaultColumnarSchema;
import org.knime.core.table.schema.traits.DataTraits;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

/**
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
public final class ColumnarSchemaSerializer {

    private ColumnarSchemaSerializer() {
    }

    /**
     * Note that this method is <b>not</b> thread-safe.
     */
    @SuppressWarnings("javadoc")
    public static JsonNode save(final ColumnarSchema schema, final JsonNodeFactory factory) {
        var schemaNode = factory.objectNode();
        var specs = schemaNode.putArray("specs");
        var traits = schemaNode.putArray("traits");

        var dataSpecSerializer = new DataSpecSerializer();
        var dataTraitsSerializer = new DataTraitsSerializer(factory);

        for (int idx = 0; idx < schema.numColumns(); idx++) {
            specs.add(dataSpecSerializer.save(schema.getSpec(idx), factory));
            traits.add(dataTraitsSerializer.save(schema.getTraits(idx)));
        }

        return schemaNode;
    }

    @SuppressWarnings("javadoc")
    public static ColumnarSchema load(final JsonNode input) {
        var specs = (ArrayNode)input.get("specs");
        var traits = (ArrayNode)input.get("traits");

        if (specs == null || traits == null) {
            throw new IllegalStateException("ColumnarSchema requires JSON children 'specs' and 'traits'");
        }

        if (specs.size() != traits.size()) {
            throw new IllegalStateException(
                "ColumnarSchema requires JSON children 'specs' and 'traits' to be of same length");
        }

        List<DataSpec> dataSpecs = new ArrayList<>();
        List<DataTraits> dataTraits = new ArrayList<>();

        for (int idx = 0; idx < specs.size(); idx++) {
            dataSpecs.add(DataSpecSerializer.load(specs.get(idx)));
            dataTraits.add(DataTraitsSerializer.load(traits.get(idx)));
        }

        return new DefaultColumnarSchema(dataSpecs, dataTraits);
    }
}
