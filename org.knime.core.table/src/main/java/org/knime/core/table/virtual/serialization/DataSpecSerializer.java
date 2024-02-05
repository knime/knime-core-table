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
