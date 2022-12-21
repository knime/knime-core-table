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

public final class SelectColumnsTransformSpec implements TableTransformSpec {

    private final int[] m_columnIndices;

    // TODO interface for ColumnSelection. Reasoning: Wide tables with many selected columns may result in too large
    // arrays. Also "all selected".
    public SelectColumnsTransformSpec(final int[] selection) {
        m_columnIndices = selection.clone();
        if (Arrays.stream(m_columnIndices).anyMatch(i -> i < 0)) {
            throw new IndexOutOfBoundsException(
                    "Column filter contains negative indices: " + Arrays.toString(m_columnIndices));
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
        return obj instanceof SelectColumnsTransformSpec &&
            Arrays.equals(m_columnIndices, ((SelectColumnsTransformSpec)obj).m_columnIndices);
    }

    @Override
    public String toString() {
        return "SelectColumns " + Arrays.toString(m_columnIndices);
    }

    public static final class SelectColumnsTransformSpecSerializer
        extends AbstractTableTransformSpecSerializer<SelectColumnsTransformSpec> {

        public SelectColumnsTransformSpecSerializer() {
            super("select_columns", 0);
        }

        @Override
        protected JsonNode saveInternal(final SelectColumnsTransformSpec spec, final JsonNodeFactory output) {
            final ObjectNode config = output.objectNode();
            final ArrayNode columnIndicesConfig = config.putArray("included_columns");
            for (final int columnIndex : spec.m_columnIndices) {
                columnIndicesConfig.add(columnIndex);
            }
            return config;
        }

        @Override
        protected SelectColumnsTransformSpec loadInternal(final JsonNode input) {
            final ObjectNode root = (ObjectNode)input;
            final ArrayNode columnIndicesConfig = (ArrayNode)root.get("included_columns");
            final int[] columnIndices = new int[columnIndicesConfig.size()];
            for (int i = 0; i < columnIndices.length; i++) {
                columnIndices[i] = columnIndicesConfig.get(i).intValue();
            }
            return new SelectColumnsTransformSpec(columnIndices);
        }
    }

    /**
     * Helper to compute the column selection resulting from dropping a set of indices.
     * <p>
     * Builds index list {@code 0,1,...,numIndices-1} and removes all indices that occur in {@code indicesToDrop}.
     * {@code indicesToDrop} may be unordered and may contain duplicates.
     */
    public static int[] indicesAfterDrop(final int numIndices, final int[] indicesToDrop)
    {
        final int[] dropped = indicesToDrop.clone();
        Arrays.sort(dropped);
        final int[] remaining = new int[numIndices];
        int i = 0, d = 0, r = 0;
        while (i < numIndices && d < dropped.length) {
            if (dropped[d] == i)
            {
                // skip i
                i++;
                // skip duplicates in dropped[]
                while (++d < dropped.length && dropped[d] == dropped[d - 1]) {
                }
            }
            else
                remaining[r++] = i++;
        }
        while (i < numIndices) {
            remaining[r++] = i++;
        }
        return Arrays.copyOf(remaining, r);
    }

    /**
     * Helper to compute the column selection resulting from keeping only a set of indices.
     * <p>
     * Returns the indices occurring in {@code indicesToKeep} in ascending order, without duplicates.
     */
    public static int[] indicesAfterKeepOnly(final int[] indicesToKeep)
    {
        if (indicesToKeep.length <= 1)
            return indicesToKeep;
        final int[] remaining = indicesToKeep.clone();
        Arrays.sort(remaining);
        int i = 0, r = 0;
        while (true) {
            if (i != r)
                remaining[r] = remaining[i];
            ++r;
            do {
                if (++i == remaining.length)
                    return r == remaining.length ? remaining : Arrays.copyOf(remaining, r);
            } while (remaining[i] == remaining[i - 1]);
        }
    }
}
