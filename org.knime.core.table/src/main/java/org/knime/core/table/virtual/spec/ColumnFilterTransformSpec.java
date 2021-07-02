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
 *   Apr 13, 2021 (marcel): created
 */
package org.knime.core.table.virtual.spec;

import java.util.Arrays;

import org.knime.core.table.virtual.serialization.AbstractTableTransformSpecSerializer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

public final class ColumnFilterTransformSpec implements TableTransformSpec {

    private final int[] m_columnIndices;

    // TODO interface for ColumnSelection. Reasoning: Wide tables with many selected columns may result in too large
    // arrays. Also "all selected".
    public ColumnFilterTransformSpec(final int[] selection) {
        m_columnIndices = selection.clone();
        int previousIndex = -1;
        for (final int columnIndex : m_columnIndices) {
            if (columnIndex < 0) {
                throw new IndexOutOfBoundsException(
                    "Column filter contains negative indices: " + Arrays.toString(m_columnIndices));
            }
            if (columnIndex <= previousIndex) {
                throw new IllegalArgumentException(
                    "Column filter contains duplicate or unordered indices: " + Arrays.toString(m_columnIndices));
            }
            previousIndex = columnIndex;
        }
    }

    /**
     * @return The column indices selected for inclusion.
     */
    public int[] getColumnSelection() {
        return m_columnIndices.clone();
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(m_columnIndices);
    }

    @Override
    public boolean equals(final Object obj) {
        return obj instanceof ColumnFilterTransformSpec &&
            Arrays.equals(m_columnIndices, ((ColumnFilterTransformSpec)obj).m_columnIndices);
    }

    @Override
    public String toString() {
        return "Column filter " + Arrays.toString(m_columnIndices);
    }

    public static final class ColumnFilterTransformSpecSerializer
        extends AbstractTableTransformSpecSerializer<ColumnFilterTransformSpec> {

        public ColumnFilterTransformSpecSerializer() {
            super("column_filter", 0);
        }

        @Override
        protected JsonNode saveInternal(final ColumnFilterTransformSpec spec, final JsonNodeFactory output) {
            final ObjectNode config = output.objectNode();
            final ArrayNode columnIndicesConfig = config.putArray("included_columns");
            for (final int columnIndex : spec.m_columnIndices) {
                columnIndicesConfig.add(columnIndex);
            }
            return config;
        }

        @Override
        protected ColumnFilterTransformSpec loadInternal(final JsonNode input) {
            final ObjectNode root = (ObjectNode)input;
            final ArrayNode columnIndicesConfig = (ArrayNode)root.get("included_columns");
            final int[] columnIndices = new int[columnIndicesConfig.size()];
            for (int i = 0; i < columnIndices.length; i++) {
                columnIndices[i] = columnIndicesConfig.get(i).intValue();
            }
            return new ColumnFilterTransformSpec(columnIndices);
        }
    }
}
