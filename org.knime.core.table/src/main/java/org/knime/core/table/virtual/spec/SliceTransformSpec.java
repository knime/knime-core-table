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
 *   Apr 14, 2021 (marcel): created
 */
package org.knime.core.table.virtual.spec;

import org.knime.core.table.row.Selection.RowRangeSelection;
import org.knime.core.table.virtual.serialization.AbstractTableTransformSpecSerializer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

public final class SliceTransformSpec implements TableTransformSpec {

    private final RowRangeSelection m_range;

    /**
     * @param from The start index of the slice (inclusive).
     * @param to The end index of the slice (exclusive).
     */
    public SliceTransformSpec(final long from, final long to) {
        m_range = RowRangeSelection.all().retain(from, to);
    }

    /**
     * @param rowRange The rows of the slice.
     */
    public SliceTransformSpec(RowRangeSelection rowRange) {
        if (rowRange.allSelected())
            throw new IllegalArgumentException();
        m_range = rowRange;
    }

    /**
     * @return The sliced range of rows.
     */
    public RowRangeSelection getRowRangeSelection() {
        return m_range;
    }

    @Override
    public int hashCode() {
        return m_range.hashCode();
    }

    @Override
    public boolean equals(final Object obj) {
        return obj instanceof SliceTransformSpec && m_range.equals(((SliceTransformSpec)obj).m_range);
    }

    @Override
    public String toString() {
        return "Slice from " + m_range.fromIndex() + " to " + m_range.toIndex();
    }

    public static final class SliceTransformSpecSerializer
        extends AbstractTableTransformSpecSerializer<SliceTransformSpec> {

        public SliceTransformSpecSerializer() {
            super("slice", 0);
        }

        @Override
        protected JsonNode saveInternal(final SliceTransformSpec spec, final JsonNodeFactory output) {
            final ObjectNode config = output.objectNode();
            config.put("from", spec.m_range.fromIndex());
            config.put("to", spec.m_range.toIndex());
            return config;
        }

        @Override
        protected SliceTransformSpec loadInternal(final JsonNode input) {
            final ObjectNode config = (ObjectNode)input;
            final long from = config.get("from").longValue();
            final long to = config.get("to").longValue();
            return new SliceTransformSpec(from, to);
        }
    }
}
