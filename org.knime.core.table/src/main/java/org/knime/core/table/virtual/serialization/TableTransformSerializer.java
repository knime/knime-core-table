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
 *   Created on May 20, 2021 by marcel
 */
package org.knime.core.table.virtual.serialization;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.knime.core.table.virtual.TableTransform;
import org.knime.core.table.virtual.spec.AppendMissingValuesTransformSpec;
import org.knime.core.table.virtual.spec.AppendTransformSpec;
import org.knime.core.table.virtual.spec.ColumnFilterTransformSpec;
import org.knime.core.table.virtual.spec.ConcatenateTransformSpec;
import org.knime.core.table.virtual.spec.PermuteTransformSpec;
import org.knime.core.table.virtual.spec.SliceTransformSpec;
import org.knime.core.table.virtual.spec.SourceTransformSpec;
import org.knime.core.table.virtual.spec.TableTransformSpec;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
public final class TableTransformSerializer {

    private TableTransformSerializer() {}

    public static JsonNode save(final TableTransform transform, final JsonNodeFactory factory) {
        return new TableTransformInSerialization(transform, factory).save();
    }

    public static TableTransform load(final JsonNode config) {
        return new TableTransformInDeserialization(config).load();
    }

    private static final class TableTransformInSerialization {

        private final JsonNodeFactory m_configFactory;

        private final Set<TableTransform> m_sourceTransforms = new HashSet<>();

        private final Map<TableTransform, List<TableTransform>> m_childrenTransforms = new HashMap<>();

        public TableTransformInSerialization(final TableTransform transform, final JsonNodeFactory factory) {
            m_configFactory = factory;
            traceBack(transform);
        }

        // TODO: consolidate with logic in lazy executor.
        private void traceBack(final TableTransform transform) {
            final List<TableTransform> parents = transform.getPrecedingTransforms();
            if (parents.isEmpty()) {
                m_sourceTransforms.add(transform);
            } else {
                for (final TableTransform parent : parents) {
                    List<TableTransform> children = m_childrenTransforms.get(parent);
                    if (children == null) {
                        children = new ArrayList<>();
                        children.add(transform);
                        m_childrenTransforms.put(parent, children);
                        traceBack(parent);
                    } else {
                        children.add(transform);
                    }
                }
            }
        }

        public JsonNode save() {
            final Deque<TableTransform> transformsToTraverse = new ArrayDeque<>();
            final Map<TableTransform, JsonNode> serializedTransforms = new LinkedHashMap<>();
            final Map<TableTransform, Integer> transformIds = new HashMap<>();

            final ObjectNode configRoot = m_configFactory.objectNode();
            final ArrayNode transformsConfig = configRoot.putArray("transforms");
            final ArrayNode connectionsConfig = configRoot.putArray("connections");

            m_sourceTransforms.forEach(transformsToTraverse::push);

            while (!transformsToTraverse.isEmpty()) {
                final TableTransform transform = transformsToTraverse.pop();

                // Push children in reverse order. This is not necessary to guarantee the correctness of the
                // serialization logic, but it keeps the serialized format more intuitive (because transforms will
                // appear in the order in which they were defined programmatically).
                Lists.reverse(m_childrenTransforms.getOrDefault(transform, Collections.emptyList()))
                    .forEach(transformsToTraverse::push);

                if (serializedTransforms.containsKey(transform)) {
                    continue;
                }

                if (transform.getPrecedingTransforms().size() > 1 &&
                    transform.getPrecedingTransforms().stream().anyMatch(t -> !serializedTransforms.containsKey(t))) {
                    // We cannot process the transform yet because not all parents have been visited; guarantees
                    // topological ordering.
                    transformsToTraverse.addLast(transform);
                    continue;
                }

                final JsonNode serializedTransform = serializeTransformSpec(transform.getSpec(), m_configFactory);
                serializedTransforms.put(transform, serializedTransform);
                // Transform's id coincides with its position in the JSON array.
                final int transformId = transformsConfig.size();
                transformIds.put(transform, transformId);
                transformsConfig.add(serializedTransform);

                final List<TableTransform> parentTransforms = transform.getPrecedingTransforms();
                for (int i = 0; i < parentTransforms.size(); i++) {
                    final TableTransform parentTransform = parentTransforms.get(i);
                    final ObjectNode connection = connectionsConfig.addObject();
                    // Forward compatibility: make "from" an object to allow adding port information in a future
                    // iteration.
                    final ObjectNode from = connection.putObject("from");
                    from.put("transform", transformIds.get(parentTransform));
                    final ObjectNode to = connection.putObject("to");
                    to.put("transform", transformId);
                    to.put("port", i);
                }
            }

            return configRoot;
        }

        private static <T extends TableTransformSpec> JsonNode serializeTransformSpec(final T transformSpec,
            final JsonNodeFactory configFactory) {
            // TODO: hack for now. Implement proper registration/discovery mechanism later.
            final Class<?> serializerClass = Arrays.stream(transformSpec.getClass().getNestMembers())
                .filter(TableTransformSpecSerializer.class::isAssignableFrom) //
                .findFirst() //
                .orElseThrow(() -> new IllegalStateException(
                    "No serializer found for transform spec: " + transformSpec.getClass()));
            try {
                @SuppressWarnings("unchecked")
                final TableTransformSpecSerializer<T> serializer =
                    (TableTransformSpecSerializer<T>)serializerClass.getDeclaredConstructor().newInstance();
                return serializer.save(transformSpec, configFactory);
            } catch (final ReflectiveOperationException ex) {
                throw new IllegalStateException(ex);
            }
        }
    }

    private static final class TableTransformInDeserialization {

        private final ObjectNode m_configRoot;

        public TableTransformInDeserialization(final JsonNode input) {
            m_configRoot = (ObjectNode)input;
        }

        public TableTransform load() {
            final ArrayNode transformsConfig = (ArrayNode)m_configRoot.get("transforms");
            final ArrayNode connectionsConfig = (ArrayNode)m_configRoot.get("connections");

            final List<TableTransformSpec> transformSpecs = new ArrayList<>(transformsConfig.size());
            for (final JsonNode transformConfig : transformsConfig) {
                transformSpecs.add(deserializeTransformSpec(transformConfig));
            }

            final Set<Integer> leafTransforms =
                new LinkedHashSet<>(IntStream.range(0, transformSpecs.size()).boxed().collect(Collectors.toList()));
            final Map<Integer, Map<Integer, Integer>> parentTransforms = new HashMap<>();
            for (final JsonNode connection : connectionsConfig) {
                final int fromTransform = connection.get("from").get("transform").intValue();
                leafTransforms.remove(fromTransform);
                final JsonNode to = connection.get("to");
                final int toTransform = to.get("transform").intValue();
                final int toPort = to.get("port").intValue();
                parentTransforms.computeIfAbsent(toTransform, k -> new HashMap<>()).put(toPort, fromTransform);
            }

            final Map<Integer, TableTransform> transforms = new HashMap<>();
            for (int i = 0; i < transformSpecs.size(); i++) {
                resolveTransformsTree(i, transformSpecs, parentTransforms, transforms);
            }

            // TODO: support returning multi-output graphs
            return transforms.get(leafTransforms.iterator().next());
        }

        // TODO: our serialization logic above guarantees a topological ordering of the serialized graph representation.
        // This should allow us to simplify this method (i.e. getting rid of the recursion).
        private static void resolveTransformsTree(final int specIndex, final List<TableTransformSpec> transformSpecs,
            final Map<Integer, Map<Integer, Integer>> parentTransforms, final Map<Integer, TableTransform> transforms) {
            if (transforms.containsKey(specIndex)) {
                return;
            }
            final Map<Integer, Integer> parents = parentTransforms.get(specIndex);
            final List<TableTransform> resolvedParents;
            if (parents != null) {
                resolvedParents = new ArrayList<>(parents.size());
                for (int j = 0; j < parents.size(); j++) {
                    final int parentSpecIndex = parents.get(j);
                    resolveTransformsTree(parentSpecIndex, transformSpecs, parentTransforms, transforms);
                    resolvedParents.add(transforms.get(parentSpecIndex));
                }
            } else {
                resolvedParents = Collections.emptyList();
            }
            transforms.put(specIndex, new TableTransform(resolvedParents, transformSpecs.get(specIndex)));
        }

        private static TableTransformSpec deserializeTransformSpec(final JsonNode transformSpecConfig) {
            final String type = transformSpecConfig.get("type").textValue();
            final TableTransformSpecSerializer<?> serializer = getTransformSpecSerializer(type);
            return serializer.load(transformSpecConfig);
        }

        private static TableTransformSpecSerializer<?> getTransformSpecSerializer(final String transformIdentifier) {
            switch (transformIdentifier) {
                case "append":
                    return new AppendTransformSpec.AppendTransformSpecSerializer();
                case "append_missing_values":
                    return new AppendMissingValuesTransformSpec.AppendMissingValuesTransformSpecSerializer();
                case "column_filter":
                    return new ColumnFilterTransformSpec.ColumnFilterTransformSpecSerializer();
                case "concatenate":
                    return new ConcatenateTransformSpec.ConcatenateTransformSpecSerializer();
                case "permute":
                    return new PermuteTransformSpec.PermuteTransformSpecSerializer();
                case "slice":
                    return new SliceTransformSpec.SliceTransformSpecSerializer();
                case "source":
                    return new SourceTransformSpec.SourceTransformSpecSerializer();
                default:
                    throw new UnsupportedOperationException("Unkown transformation: " + transformIdentifier);
            }
        }
    }
}
