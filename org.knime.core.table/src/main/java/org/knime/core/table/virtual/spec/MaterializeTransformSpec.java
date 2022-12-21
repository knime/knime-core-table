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

import java.util.Objects;
import java.util.UUID;

public final class MaterializeTransformSpec implements TableTransformSpec {

    private final UUID m_sinkIdentifier;

    public MaterializeTransformSpec(final UUID sinkIdentifier) {
        m_sinkIdentifier = sinkIdentifier;
    }

    public UUID getSinkIdentifier() {
        return m_sinkIdentifier;
    }

    @Override
    public int hashCode() {
        return Objects.hash(m_sinkIdentifier);
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (! (obj instanceof MaterializeTransformSpec)) {
            return false;
        }
        final MaterializeTransformSpec that = (MaterializeTransformSpec)obj;
        return m_sinkIdentifier.equals(that.m_sinkIdentifier);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Sink");
        sb.append(" m_sinkIdentifier=").append(m_sinkIdentifier);
        return sb.toString();
    }

}
