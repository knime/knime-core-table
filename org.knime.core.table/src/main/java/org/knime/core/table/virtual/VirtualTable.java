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
 *   Mar 25, 2021 (dietzc): created
 */
package org.knime.core.table.virtual;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiConsumer;

import org.knime.core.table.cursor.Cursor;
import org.knime.core.table.row.ReadAccessRow;
import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.row.WriteAccessRow;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.schema.DataSpec;

import com.google.common.collect.Collections2;

public final class VirtualTable {

    private final TableTransform m_transform;

    private final ColumnarSchema m_schema;

    public VirtualTable(final UUID sourceIdentifier, final ColumnarSchema schema) {
        m_transform = new TableTransform(new SourceTransformSpec(sourceIdentifier));
        m_schema = schema;
    }

    public VirtualTable(final TableTransform producingTransform, final ColumnarSchema schema) {
        m_transform = producingTransform;
        m_schema = schema;
    }

    public TableTransform getProducingTransform() {
        return m_transform;
    }

    public ColumnarSchema getSchema() {
        return m_schema;
    }

    public VirtualTable append(final List<VirtualTable> tables) {
        final TableTransformSpec transformSpec = new AppendTransformSpec();
        final List<ColumnarSchema> schemas = new ArrayList<>(1 + tables.size());
        schemas.add(m_schema);
        schemas.addAll(Collections2.transform(tables, VirtualTable::getSchema));
        final ColumnarSchema schema = transformSpec.transformSchemas(schemas).get(0);
        final List<TableTransform> transforms = new ArrayList<>(1 + tables.size());
        transforms.add(m_transform);
        transforms.addAll(Collections2.transform(tables, VirtualTable::getProducingTransform));
        return new VirtualTable(new TableTransform(transforms, transformSpec), schema);
    }

    public VirtualTable appendMissingValueColumns(final List<DataSpec> columns) {
        final TableTransformSpec transformSpec = new AppendMissingValuesTransformSpec(columns);
        final ColumnarSchema schema = transformSpec.transformSchemas(Arrays.asList(m_schema)).get(0);
        return new VirtualTable(new TableTransform(Arrays.asList(m_transform), transformSpec), schema);
    }

    public VirtualTable concatenate(final List<VirtualTable> tables) {
        final TableTransformSpec transformSpec = new ConcatenateTransformSpec();
        final List<ColumnarSchema> schemas = new ArrayList<>(1 + tables.size());
        schemas.add(m_schema);
        schemas.addAll(Collections2.transform(tables, VirtualTable::getSchema));
        final ColumnarSchema schema = transformSpec.transformSchemas(schemas).get(0);
        final List<TableTransform> transforms = new ArrayList<>(1 + tables.size());
        transforms.add(m_transform);
        transforms.addAll(Collections2.transform(tables, VirtualTable::getProducingTransform));
        return new VirtualTable(new TableTransform(transforms, transformSpec), schema);
    }

    public VirtualTable filterColumns(final int... columnIndices) {
        final TableTransformSpec transformSpec = new ColumnFilterTransformSpec(columnIndices);
        final ColumnarSchema schema = transformSpec.transformSchemas(Arrays.asList(m_schema)).get(0);
        return new VirtualTable(new TableTransform(Arrays.asList(m_transform), transformSpec), schema);
    }

    public VirtualTable map(final BiConsumer<ReadAccessRow, WriteAccessRow> mapper, final ColumnarSchema outputSchema) {
        final TableTransformSpec transformSpec = new MapTransformSpec(ignore -> outputSchema, mapper);
        return new VirtualTable(new TableTransform(Arrays.asList(m_transform), transformSpec), outputSchema);
    }

    public VirtualTable permute(final int... permutation) {
        final TableTransformSpec transformSpec = new PermuteTransformSpec(permutation);
        final ColumnarSchema schema = transformSpec.transformSchemas(Arrays.asList(m_schema)).get(0);
        return new VirtualTable(new TableTransform(Arrays.asList(m_transform), transformSpec), schema);

    }

    public VirtualTable slice(final long from, final long to) {
        final TableTransformSpec transformSpec = new SliceTransformSpec(from, to);
        final ColumnarSchema schema = transformSpec.transformSchemas(Arrays.asList(m_schema)).get(0);
        return new VirtualTable(new TableTransform(Arrays.asList(m_transform), transformSpec), schema);
    }

    /**
     * @param destination The {@link WriteAccessRow} of the destination must be compatible to the {@link #getSchema()
     *            schema} of this instance. The destination must be {@link Cursor#close() closed} by the caller of this
     *            method.
     */
    public void copy(final Map<UUID, RowAccessible> sources, final Cursor<WriteAccessRow> destination)
        throws IOException {
        // TODO: optimize graph
        try (final RowAccessible transformedTable = new TableTransformer(sources, m_transform).transform()) {
            try (final Cursor<ReadAccessRow> source = transformedTable.createCursor()) {
                final ReadAccessRow readAccess = source.access();
                final WriteAccessRow writeAccess = destination.access();
                while (source.forward() && destination.forward()) {
                    writeAccess.setFrom(readAccess);
                }
            }
        }
    }
}

/* optimize append columns:

// TODO: at the moment, tables and schemas need to be simplified separately (below, only the table is
// simplified; simplification of the schema would have to be done in its respective constructor).
// How to get rid of this redundancy? Build schema graph first, optimize, and then build table graph based upon
// the optimized schema graph? That is, virtual tables would not be aware of any optimization mechanisms.
final List<RowAccessible> tmp = new ArrayList<>();
for (final RowAccessible t : tables) {
    if (t instanceof AppendedTable) {
        final RowAccessible[] innerTables = ((ConcatenatedTable)t).getInnerTables();
        for (final RowAccessible inner : innerTables) {
            tmp.add(inner); // NOSONAR
        }
    } else {
        tmp.add(t);
    }
}

*/

/* optimize concatenate:

// TODO
//        final List<RowAccessible> tmp = new ArrayList<>();
//        for (final RowAccessible t : tables) {
//            if (t instanceof ConcatenatedTable) {
//                final RowAccessible[] innerTables = ((ConcatenatedTable)t).getInnerTables();
//                for (final RowAccessible inner : innerTables) {
//                    tmp.add(inner); // NOSONAR
//                }
//            } else {
//                tmp.add(t);
//            }
//        }

*/