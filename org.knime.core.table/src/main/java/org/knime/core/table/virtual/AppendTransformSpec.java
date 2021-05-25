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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.schema.DefaultColumnarSchema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.google.common.collect.Lists;

// TODO: in its current state, this spec can be converted into a singleton. Let's wait and see if there will be any parametrization.
public final class AppendTransformSpec implements TableTransformSpec {

    @Override
    public List<ColumnarSchema> transformSchemas(final List<ColumnarSchema> schemas) {
        return Arrays.asList(createSchema(schemas));
    }

    private static ColumnarSchema createSchema(final List<ColumnarSchema> schemas) {
        int totalNumColumns = 0;
        for (final ColumnarSchema schema : schemas) {
            totalNumColumns += schema.numColumns();
        }
        final List<DataSpec> appendedSpecs = new ArrayList<>(totalNumColumns);
        for (final ColumnarSchema schema : schemas) {
            final int numColumns = schema.numColumns();
            for (int i = 0; i < numColumns; i++) {
                appendedSpecs.add(schema.getSpec(i));
            }
        }
        return new DefaultColumnarSchema(appendedSpecs);
    }

    @SuppressWarnings("resource") // Created tables are to be closed by clients.
    @Override
    public List<RowAccessible> transformTables(final List<RowAccessible> tables) {
        final ColumnarSchema schema = createSchema(Lists.transform(tables, RowAccessible::getSchema));
        return Arrays.asList(new AppendedTable(tables, schema));
    }

    @Override
    public int hashCode() {
        return AppendTransformSpec.class.hashCode();
    }

    @Override
    public boolean equals(final Object obj) {
        return obj instanceof AppendTransformSpec;
    }

    @Override
    public String toString() {
        return "Append";
    }

    public static final class AppendTransformSpecSerializer
        extends AbstractTableTransformSpecSerializer<AppendTransformSpec> {

        public AppendTransformSpecSerializer() {
            super("append", 0);
        }

        @Override
        protected JsonNode saveInternal(final AppendTransformSpec spec, final JsonNodeFactory output) {
            // Nothing to serialize.
            return null;
        }

        @Override
        protected AppendTransformSpec loadInternal(final JsonNode input) {
            return new AppendTransformSpec();
        }
    }
}
