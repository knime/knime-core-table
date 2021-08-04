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
 *   Created on Jul 30, 2021 by Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
package org.knime.core.table.virtual.spec;

import org.knime.core.table.virtual.serialization.AbstractTableTransformSpecSerializer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

/**
 * This transformation is a no-op i.e. it doesn't change the incoming table.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public enum IdentityTransformSpec implements TableTransformSpec {
    /**
     * The singleton instance of IdentityTransformSpec.
     */
    INSTANCE;

    /**
     * Serializer for {@link IdentityTransformSpec}.
     *
     * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
     */
    public static final class IdentityTransformSpecSerializer
        extends AbstractTableTransformSpecSerializer<IdentityTransformSpec> {

        /**
         * Constructor.
         */
        public IdentityTransformSpecSerializer() {
            super("identity", 0);
        }

        @Override
        protected JsonNode saveInternal(final IdentityTransformSpec spec, final JsonNodeFactory output) {
            // Nothing to serialize.
            return null;
        }

        @Override
        protected IdentityTransformSpec loadInternal(final JsonNode input) {
            return IdentityTransformSpec.INSTANCE;
        }
    }
}
