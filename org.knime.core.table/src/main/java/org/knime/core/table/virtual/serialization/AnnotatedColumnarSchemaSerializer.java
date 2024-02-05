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
