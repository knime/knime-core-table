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
 *   Created on May 25, 2021 by marcel
 */
package org.knime.core.table.virtual.spec;

import java.util.UUID;

import org.knime.core.table.virtual.serialization.AbstractTableTransformSpecSerializer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
public final class SourceTransformSpec implements TableTransformSpec {

    private final UUID m_sourceIdentifier;

    public SourceTransformSpec(final UUID sourceIdentifier) {
        m_sourceIdentifier = sourceIdentifier;
    }

    public UUID getSourceIdentifier() {
        return m_sourceIdentifier;
    }

    @Override
    public int hashCode() {
        return m_sourceIdentifier.hashCode();
    }

    @Override
    public boolean equals(final Object obj) {
        return obj instanceof SourceTransformSpec &&
            m_sourceIdentifier.equals(((SourceTransformSpec)obj).m_sourceIdentifier);
    }

    @Override
    public String toString() {
        return "Source " + m_sourceIdentifier.toString();
    }

    public static final class SourceTransformSpecSerializer
        extends AbstractTableTransformSpecSerializer<SourceTransformSpec> {

        public SourceTransformSpecSerializer() {
            super("source", 0);
        }

        @Override
        protected JsonNode saveInternal(final SourceTransformSpec spec, final JsonNodeFactory factory) {
            return factory.objectNode().put("identifier", spec.m_sourceIdentifier.toString());
        }

        @Override
        protected SourceTransformSpec loadInternal(final JsonNode config) {
            return new SourceTransformSpec(UUID.fromString(config.get("identifier").textValue()));
        }
    }
}
