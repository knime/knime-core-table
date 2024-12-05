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

import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.access.WriteAccess;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.virtual.spec.MapTransformSpec.MapperFactory;

public final class AppendMissingValuesTransformSpec implements TableTransformSpec {

    private final ColumnarSchema m_columns;

    /**
     * @param columns The specs of the missing columns to append.
     */
    public AppendMissingValuesTransformSpec(final ColumnarSchema columns) {
        m_columns = columns;
    }

    /**
     * @return The schema of the appended all-missing columns.
     */
    public ColumnarSchema getAppendedSchema() {
        return m_columns;
    }

    @Override
    public int hashCode() {
        return m_columns.hashCode();
    }

    @Override
    public boolean equals(final Object obj) {
        return obj instanceof AppendMissingValuesTransformSpec that && m_columns.equals(that.m_columns);
    }

    @Override
    public String toString() {
        return "Append all-missing " + m_columns.toString();
    }

    public MapTransformSpec toMap() {
        return new MapTransformSpec(new int[0], new MissingValueMapperFactory(m_columns));
    }

    static class MissingValueMapperFactory implements MapperFactory {

        private final ColumnarSchema m_schema;

        MissingValueMapperFactory(final ColumnarSchema schema) {
            m_schema = schema;
        }

        @Override
        public ColumnarSchema getOutputSchema() {
            return m_schema;
        }

        @Override
        public Runnable createMapper(final ReadAccess[] inputs, final WriteAccess[] outputs) {
            for (WriteAccess output : outputs) {
                output.setMissing();
            }
            return () -> {};
        }

        @Override
        public final boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof MissingValueMapperFactory that)) {
                return false;
            }

            return m_schema.equals(that.m_schema);
        }

        @Override
        public int hashCode() {
            return m_schema.hashCode();
        }
    }
}
