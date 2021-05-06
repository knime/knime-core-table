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
package org.knime.core.table.virtual;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.schema.ColumnarSchema;

public final class SliceTransformSpec implements TableTransformSpec {

    private final long m_from;

    private final long m_to;

    /**
     * @param from The start index of the slice (inclusive).
     * @param to The end index of the slice (exclusive).
     */
    public SliceTransformSpec(final long from, final long to) {
        if (from < 0) {
            throw new IndexOutOfBoundsException("Negative 'from' index.");
        }
        if (to < 0) {
            throw new IndexOutOfBoundsException("Negative 'to' index.");
        }
        m_from = from;
        m_to = to;
    }

    @Override
    public List<ColumnarSchema> transformSchemas(final List<ColumnarSchema> schemas) {
        return schemas;
    }

    @SuppressWarnings("resource") // Created tables are to be closed by clients.
    @Override
    public List<RowAccessible> transformTables(final List<RowAccessible> tables) {
        final RowAccessible table = tables.get(0);
        final ColumnarSchema schema = table.getSchema();
        return Arrays.asList(new SlicedTable(table, m_from, m_to, schema));
    }

    @Override
    public int hashCode() {
        return Long.hashCode(m_from) * 31 + Long.hashCode(m_to);
    }

    @Override
    public boolean equals(final Object obj) {
        if (!(obj instanceof SliceTransformSpec)) {
            return false;
        }
        final SliceTransformSpec other = (SliceTransformSpec)obj;
        return m_from == other.m_from && m_to == other.m_to;
    }

    @Override
    public String toString() {
        return "Slice from " + m_from + " to " + m_to;
    }

    public static final class SliceTransformSpecSerializer
        extends AbstractTableTransformSpecSerializer<SliceTransformSpec> {

        public SliceTransformSpecSerializer() {
            super(SliceTransformSpec.class, 0);
        }

        @Override
        public void write(final SliceTransformSpec spec, final DataOutput output) throws IOException {
            output.writeLong(spec.m_from);
            output.writeLong(spec.m_to);
        }

        @Override
        public SliceTransformSpec read(final DataInput input) throws IOException {
            final long from = input.readLong();
            final long to = input.readLong();
            return new SliceTransformSpec(from, to);
        }
    }
}
