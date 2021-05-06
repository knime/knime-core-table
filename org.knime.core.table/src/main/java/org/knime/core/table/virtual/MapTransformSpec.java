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
 *   May 4, 2021 (marcel): created
 */
package org.knime.core.table.virtual;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.row.WriteAccessRow;
import org.knime.core.table.schema.ColumnarSchema;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 */
public final class MapTransformSpec implements TableTransformSpec {

    private final Function<ColumnarSchema, ColumnarSchema> m_schemaMapper;

    private final BiConsumer<ReadAccessRow, WriteAccessRow> m_rowMapper;

    // TODO: the mappers expected here are probably the most generic ones that can accomplish a row-wise map of a table.
    // They, however, do not make any guarantees as to which cells of the input row are replaced/left untouched/etc. to
    // produce the output row. Some more specialized/limited kind of map (e.g. one that only replaces a single cell per
    // row, of the same column across all rows) would allow for more optimization (because we know that all other cells
    // were not touched in the example).
    public MapTransformSpec(final Function<ColumnarSchema, ColumnarSchema> schemaMapper,
        final BiConsumer<ReadAccessRow, WriteAccessRow> rowMapper) {
        m_schemaMapper = schemaMapper;
        m_rowMapper = rowMapper;
    }

    @Override
    public List<ColumnarSchema> transformSchemas(final List<ColumnarSchema> schemas) {
        return Arrays.asList(createSchema(schemas.get(0)));
    }

    private ColumnarSchema createSchema(final ColumnarSchema schema) {
        return m_schemaMapper.apply(schema);
    }

    @Override
    public List<RowAccessible> transformTables(final List<RowAccessible> tables) {
        final RowAccessible table = tables.get(0);
        final ColumnarSchema schema = createSchema(table.getSchema());
        return Arrays.asList(new MappedTable(table, m_rowMapper, schema));
    }

    // TODO: hashCode, equals, toString

    public static final class MapTransformSpecSerializer
        extends AbstractTableTransformSpecSerializer<MapTransformSpec> {

        public MapTransformSpecSerializer() {
            super(MapTransformSpec.class, 0);
        }

        @Override
        public void write(final MapTransformSpec object, final DataOutput output) throws IOException {
            // TODO: implement serializer registry
            // TODO: we need a mechanism to find out whether a serialized spec is cross-lanugage compatible or not (the
            // the map spec currently is not).
            throw new IllegalStateException("not yet implemented");
        }

        @Override
        public MapTransformSpec read(final DataInput input) throws IOException, ClassNotFoundException {
            throw new IllegalStateException("not yet implemented"); // TODO: implement
        }
    }
}
