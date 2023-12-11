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

import org.knime.core.table.row.Selection.RowRangeSelection;
import org.knime.core.table.schema.ColumnarSchema;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
public final class SourceTransformSpec implements TableTransformSpec {

    private final UUID m_sourceIdentifier;

    private final SourceTableProperties m_properties;

    private final RowRangeSelection m_rowRange;

    public SourceTransformSpec(final UUID sourceIdentifier, final SourceTableProperties properties) {
        this(sourceIdentifier, properties, RowRangeSelection.all());
    }

    public SourceTransformSpec(final UUID sourceIdentifier, final SourceTableProperties properties, final RowRangeSelection rowRange) {
        m_sourceIdentifier = sourceIdentifier;
        m_properties = properties;
        m_rowRange = rowRange;
    }

    public UUID getSourceIdentifier() {
        return m_sourceIdentifier;
    }

    public SourceTableProperties getProperties() {
        return m_properties;
    }

    public ColumnarSchema getSchema() {
        return m_properties.getSchema();
    }

    public RowRangeSelection getRowRange() {
        return m_rowRange;
    }

    @Override
    public int hashCode() {
        return Objects.hash(m_sourceIdentifier, m_rowRange);
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (! (obj instanceof SourceTransformSpec that)) {
            return false;
        }
        return m_sourceIdentifier.equals(that.m_sourceIdentifier) && m_rowRange.equals(that.m_rowRange);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Source");
        if (!m_rowRange.allSelected()) {
            sb.append(" from ").append(m_rowRange.fromIndex());
            sb.append(" to ").append(m_rowRange.toIndex());
        }
        sb.append(" m_sourceIdentifier=").append(m_sourceIdentifier);
        return sb.toString();
    }

}
