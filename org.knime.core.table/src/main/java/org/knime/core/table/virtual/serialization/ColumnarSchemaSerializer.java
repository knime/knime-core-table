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
            throw new IllegalStateException("ColumnarSchema requires JSON children 'specs' and 'traits' to be of same length");
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
