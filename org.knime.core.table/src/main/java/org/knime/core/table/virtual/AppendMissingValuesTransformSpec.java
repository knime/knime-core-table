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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.schema.DefaultColumnarSchema;

public final class AppendMissingValuesTransformSpec implements TableTransformSpec {

    private final List<DataSpec> m_columns;

    /**
     * @param columns The specs of the missing columns to append.
     */
    public AppendMissingValuesTransformSpec(final List<DataSpec> columns) {
        m_columns = new ArrayList<>(columns);
    }

    @Override
    public List<ColumnarSchema> transformSchemas(final List<ColumnarSchema> schemas) {
        return Arrays.asList(createSchema(schemas.get(0)));
    }

    private ColumnarSchema createSchema(final ColumnarSchema schema) {
        final List<DataSpec> appendedSpecs = new ArrayList<>(schema.numColumns() + m_columns.size());
        for (int i = 0; i < schema.numColumns(); i++) {
            appendedSpecs.add(schema.getSpec(i));
        }
        for (DataSpec spec : m_columns) {
            appendedSpecs.add(spec);
        }
        return new DefaultColumnarSchema(appendedSpecs);
    }

    @SuppressWarnings("resource") // Created tables are to be closed by clients.
    @Override
    public List<RowAccessible> transformTables(final List<RowAccessible> tables) {
        final RowAccessible table = tables.get(0);
        final ColumnarSchema schema = createSchema(table.getSchema());
        return Arrays.asList(new AppendedMissingValuesTable(table, schema));
    }

    @Override
    public int hashCode() {
        return m_columns.hashCode();
    }

    @Override
    public boolean equals(final Object obj) {
        return obj instanceof AppendMissingValuesTransformSpec &&
            m_columns.equals(((AppendMissingValuesTransformSpec)obj).m_columns);
    }

    @Override
    public String toString() {
        return "Append all-missing columns " + m_columns.toString();
    }

    public static final class AppendMissingValuesTransformSpecSerializer
        extends AbstractTableTransformSpecSerializer<AppendMissingValuesTransformSpec> {

        public AppendMissingValuesTransformSpecSerializer() {
            super(AppendMissingValuesTransformSpec.class, 0);
        }

        @Override
        public void write(final AppendMissingValuesTransformSpec spec, final DataOutput output) throws IOException {
            final List<DataSpec> columns = spec.m_columns;
            output.writeInt(columns.size());
            for (final DataSpec column : columns) {
                SerializationUtil.writeDataSpec(column, output);
            }
        }

        @Override
        public AppendMissingValuesTransformSpec read(final DataInput input) throws IOException {
            final DataSpec[] columns = new DataSpec[input.readInt()];
            for (int i = 0; i < columns.length; i++) {
                columns[i] = SerializationUtil.readDataSpec(input);
            }
            return new AppendMissingValuesTransformSpec(Arrays.asList(columns));
        }
    }
}
