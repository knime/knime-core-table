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

        DataTraits dataTraits = new DefaultDataTraits(traits.toArray(DataTrait[]::new));
        return dataTraits;
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
