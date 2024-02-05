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
 *   Created on Sep 29, 2021 by Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
package org.knime.core.table.virtual.serialization;

import java.util.ArrayList;
import java.util.List;

import org.knime.core.table.schema.traits.DataTrait;
import org.knime.core.table.schema.traits.DataTrait.DictEncodingTrait;
import org.knime.core.table.schema.traits.DataTraits;
import org.knime.core.table.schema.traits.DefaultDataTraits;
import org.knime.core.table.schema.traits.DefaultListDataTraits;
import org.knime.core.table.schema.traits.DefaultStructDataTraits;
import org.knime.core.table.schema.traits.ListDataTraits;
import org.knime.core.table.schema.traits.LogicalTypeTrait;
import org.knime.core.table.schema.traits.StructDataTraits;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Serializes {@link DataTraits} into JSON.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class DataTraitsSerializer {

    private final JsonNodeFactory m_factory;

    /**
     * Constructor.
     *
     * @param jsonNodeFactory for creating JSON nodes
     */
    public DataTraitsSerializer(final JsonNodeFactory jsonNodeFactory) {
        m_factory = jsonNodeFactory;
    }

    /**
     * Saves the provided traits into JSON.
     *
     * @param traits to save
     * @return the JSON
     */
    public JsonNode save(final DataTraits traits) {
        if (traits instanceof ListDataTraits) {
            return saveListTraits((ListDataTraits)traits);
        } else if (traits instanceof StructDataTraits) {
            return saveStructTraits((StructDataTraits)traits);
        } else {
            return saveSimpleTraits(traits);
        }
    }

    /**
     * Load data traits from JSON
     *
     * @param input JSON encoded {@link DataTraits}
     * @return the {@link DataTraits}
     */
    public static DataTraits load(final JsonNode input) {
        final String type = input.get("type").asText();

        if (type == null) {
            throw new IllegalStateException("Cannot load DataTraits from JSON, missing 'type' member");
        }

        if (type.equals("simple")) {
            return loadTopLevelTraits(input);
        } else if (type.equals("list")) {
            return loadListTraits(input);
        } else if (type.equals("struct")) {
            return loadStructTraits(input);
        }

        return null;
    }

    private JsonNode saveListTraits(final ListDataTraits listTraits) {
        final var config = m_factory.objectNode();
        config.put("type", "list");
        addTopLevelTraits(listTraits, config);
        config.set("inner", save(listTraits.getInner()));
        return config;
    }

    private static ListDataTraits loadListTraits(final JsonNode json) {
        DataTraits inner = load(json.get("inner"));
        DataTraits outer = loadTopLevelTraits(json);
        return new DefaultListDataTraits(outer.getTraits(), inner);
    }

    private JsonNode saveStructTraits(final StructDataTraits structTraits) {
        final var config = m_factory.objectNode();
        config.put("type", "struct");
        addTopLevelTraits(structTraits, config);
        ArrayNode innerNodes = config.putArray("inner");
        for (int i = 0; i < structTraits.size(); i++) {
            innerNodes.add(save(structTraits.getDataTraits(i)));
        }
        return config;
    }

    private static StructDataTraits loadStructTraits(final JsonNode json) {
        ArrayNode innerNodes = (ArrayNode)json.get("inner");
        List<DataTraits> innerTraits = new ArrayList<>();
        for (int i = 0; i < innerNodes.size(); i++) {
            innerTraits.add(load(innerNodes.get(i)));
        }

        DataTraits outer = loadTopLevelTraits(json);
        return new DefaultStructDataTraits(outer.getTraits(), innerTraits.toArray(DataTraits[]::new));
    }

    private JsonNode saveSimpleTraits(final DataTraits simpleTraits) {
        final var config = m_factory.objectNode();
        config.put("type", "simple");
        addTopLevelTraits(simpleTraits, config);
        return config;
    }

    private static void addTopLevelTraits(final DataTraits simpleTraits, final ObjectNode config) {
        final var traitsNode = config.putObject("traits");
        for (var trait : simpleTraits.getTraits()) {
            traitsNode.put(getId(trait), trait.toString());
        }
    }

    private static DataTraits loadTopLevelTraits(final JsonNode json) {
        final var traitsNode = json.get("traits");

        List<DataTrait> traits = new ArrayList<>();

        // must support all existing traits here
        var logicalType = traitsNode.get("logical_type");
        if (logicalType != null) {
            traits.add(new LogicalTypeTrait(logicalType.asText()));
        }

        var dictEncoding = traitsNode.get("dict_encoding");
        if (dictEncoding != null) {
            final var keyType = DataTrait.DictEncodingTrait.KeyType.valueOf(dictEncoding.asText());
            traits.add(new DictEncodingTrait(keyType));
        }

        if (traits.isEmpty()) {
            return DefaultDataTraits.EMPTY;
        }

        return new DefaultDataTraits(traits.toArray(DataTrait[]::new));
    }

    private static String getId(final DataTrait trait) {
        if (trait instanceof LogicalTypeTrait) {
            return "logical_type";
        } else if (trait instanceof DictEncodingTrait) {
            return "dict_encoding";
        } else {
            throw new IllegalArgumentException("Unsupported trait: " + trait);
        }
    }
}
