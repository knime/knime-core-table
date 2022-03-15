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

import org.knime.core.table.schema.AnnotatedColumnarSchema;
import org.knime.core.table.schema.DefaultAnnotatedColumnarSchema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

/**
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
public final class AnnotatedColumnarSchemaSerializer {

    /**
     * Note that this method is <b>not</b> thread-safe.
     */
    @SuppressWarnings("javadoc")
    public static JsonNode save(final AnnotatedColumnarSchema schema, final JsonNodeFactory factory) {
        // TODO: serialize as columns instead of individual arrays.

        var annotatedSchemaNode = factory.objectNode();

        var schemaNode = ColumnarSchemaSerializer.save(schema, factory);
        annotatedSchemaNode.set("schema", schemaNode);

        var columnNames = annotatedSchemaNode.putArray("columnNames");
        var columnMetaData = annotatedSchemaNode.putArray("columnMetaData");
        for (int idx = 0; idx < schema.numColumns(); idx++) {
            columnNames.add(schema.getColumnName(idx));
            var meta = schema.getColumnMetaData(idx);
            JsonNode jsonMeta = (meta == null) ? factory.nullNode() : meta.toJson(factory);
            columnMetaData.add(jsonMeta);
        }

        if (schema.getMetaData() != null) {
            annotatedSchemaNode.set("metaData", schema.getMetaData().toJson(factory));
        }

        return annotatedSchemaNode;
    }

    @SuppressWarnings("javadoc")
    public static AnnotatedColumnarSchema load(final JsonNode input) {

        var schema = ColumnarSchemaSerializer.load(input.get("schema"));
        var columnNames = (ArrayNode)input.get("columnNames");
        var columnMetaData = (ArrayNode)input.get("columnMetaData");

        if (columnNames == null || columnMetaData == null) {
            throw new IllegalStateException(
                "AnnotatedColumnarSchema requires JSON children 'columnNames' and 'columnMetaData'");
        }

        if (columnNames.size() != columnMetaData.size() || columnNames.size() != schema.numColumns()) {
            throw new IllegalStateException(
                "ColumnarSchema requires JSON children 'columnNames' and 'columnMetaData' to be of same length as schema");
        }

        String[] names = new String[schema.numColumns()];

        for (int idx = 0; idx < schema.numColumns(); idx++) {
            names[idx] = columnNames.get(idx).asText();
        }

        return DefaultAnnotatedColumnarSchema.annotate(schema, names);
    }
}
