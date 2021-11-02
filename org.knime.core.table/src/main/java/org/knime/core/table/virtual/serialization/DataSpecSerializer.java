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

import org.knime.core.table.schema.BooleanDataSpec;
import org.knime.core.table.schema.ByteDataSpec;
import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.schema.DoubleDataSpec;
import org.knime.core.table.schema.FloatDataSpec;
import org.knime.core.table.schema.IntDataSpec;
import org.knime.core.table.schema.ListDataSpec;
import org.knime.core.table.schema.LongDataSpec;
import org.knime.core.table.schema.StringDataSpec;
import org.knime.core.table.schema.StructDataSpec;
import org.knime.core.table.schema.VarBinaryDataSpec;
import org.knime.core.table.schema.VoidDataSpec;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
public final class DataSpecSerializer {

    // FIXME: serialize traits!

    private Mapper m_mapper = new Mapper();

    /**
     * Note that this method is <b>not</b> thread-safe.
     */
    @SuppressWarnings("javadoc")
    public JsonNode save(final DataSpec spec, final JsonNodeFactory factory) {
        m_mapper.m_factory = factory;
        try {
            return spec.accept(m_mapper);
        } finally {
            m_mapper.m_factory = null;
        }
    }

    @SuppressWarnings("javadoc")
    public static DataSpec load(final JsonNode input) {
        if (input.isTextual()) {
            final String typeIdentifier = input.textValue();
            switch (typeIdentifier) {
                case "boolean":
                    return BooleanDataSpec.INSTANCE;
                case "byte":
                    return ByteDataSpec.INSTANCE;
                case "double":
                    return DoubleDataSpec.INSTANCE;
                case "float":
                    return FloatDataSpec.INSTANCE;
                case "int":
                    return IntDataSpec.INSTANCE;
                case "long":
                    return LongDataSpec.INSTANCE;
                case "string":
                    return StringDataSpec.INSTANCE;
                case "variable_width_binary":
                    return VarBinaryDataSpec.INSTANCE;
                case "void":
                    return VoidDataSpec.INSTANCE;
                default:
                    throw new IllegalStateException("Unknown data spec type identifier: " + typeIdentifier);
            }
        } else {
            final ObjectNode config = (ObjectNode)input;
            final String typeIdentifier = config.get("type").textValue();
            if ("list".equals(typeIdentifier)) {
                final DataSpec innerSpec = load(config.get("inner_type"));
                return new ListDataSpec(innerSpec);
            } else if ("struct".equals(typeIdentifier)) {
                final ArrayNode innerTypeIdentifiers = (ArrayNode)config.get("inner_types");
                final DataSpec[] innerSpecs = new DataSpec[innerTypeIdentifiers.size()];
                for (int i = 0; i < innerSpecs.length; i++) {
                    innerSpecs[i] = DataSpecSerializer.load(innerTypeIdentifiers.get(i));
                }
                return new StructDataSpec(innerSpecs);
            } else {
                throw new IllegalStateException("Unknown data spec type identifier: " + typeIdentifier);
            }
        }
    }

    private static final class Mapper implements DataSpec.Mapper<JsonNode> {

        private JsonNodeFactory m_factory;

        @Override
        public JsonNode visit(final BooleanDataSpec spec) {
            return m_factory.textNode("boolean");
        }

        @Override
        public JsonNode visit(final ByteDataSpec spec) {
            return m_factory.textNode("byte");
        }

        @Override
        public JsonNode visit(final DoubleDataSpec spec) {
            return m_factory.textNode("double");
        }

        @Override
        public JsonNode visit(final FloatDataSpec spec) {
            return m_factory.textNode("float");
        }

        @Override
        public JsonNode visit(final IntDataSpec spec) {
            return m_factory.textNode("int");
        }

        @Override
        public JsonNode visit(final ListDataSpec listDataSpec) {
            final ObjectNode config = m_factory.objectNode();
            config.put("type", "list");
            final JsonNode innerTypeIdentifier = listDataSpec.getInner().accept(this);
            config.set("inner_type", innerTypeIdentifier);
            return config;
        }

        @Override
        public JsonNode visit(final LongDataSpec spec) {
            return m_factory.textNode("long");
        }

        @Override
        public JsonNode visit(final StructDataSpec spec) {
            final ObjectNode config = m_factory.objectNode();
            config.put("type", "struct");
            final ArrayNode innerTypeIdentifiers = config.putArray("inner_types");
            for (int i = 0; i < spec.size(); i++) {
                final JsonNode innerTypeIdentifier = spec.getDataSpec(i).accept(this);
                innerTypeIdentifiers.add(innerTypeIdentifier);
            }
            return config;
        }

        @Override
        public JsonNode visit(final VarBinaryDataSpec spec) {
            return m_factory.textNode("variable_width_binary");
        }

        @Override
        public JsonNode visit(final VoidDataSpec spec) {
            return m_factory.textNode("void");
        }

        @Override
        public JsonNode visit(final StringDataSpec spec) {
            return m_factory.textNode("string");
        }
    }
}
