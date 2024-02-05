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

    private final long m_numRows;

    public SourceTransformSpec(final UUID sourceIdentifier, final SourceTableProperties properties) {
        this(sourceIdentifier, properties, RowRangeSelection.all());
    }

    public SourceTransformSpec(final UUID sourceIdentifier, final SourceTableProperties properties, final RowRangeSelection rowRange) {
        m_sourceIdentifier = sourceIdentifier;
        m_properties = properties;
        m_rowRange = rowRange;
        m_numRows = numRows(properties, rowRange);
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

    /**
     * Get the number of rows of this source.
     *
     * @return the number of rows, or a negative number if the number of rows is unknown.
     */
    public long numRows() {
        return m_numRows;
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

    private static long numRows(SourceTableProperties properties, RowRangeSelection rowRange) {
        long numRows = properties.numRows();
        if (numRows < 0) {
            return -1;
        }
        if (rowRange.allSelected()) {
            return numRows;
        } else {
            final long from = rowRange.fromIndex();
            final long to = rowRange.toIndex();
            return Math.max(0, Math.min(numRows, to) - from);
        }
    }
}
