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

import static org.knime.core.table.virtual.spec.SelectColumnsTransformSpec.indicesAfterDrop;
import static org.knime.core.table.virtual.spec.SelectColumnsTransformSpec.indicesAfterKeepOnly;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.schema.DefaultColumnarSchema;
import org.knime.core.table.schema.traits.DataTraits;
import org.knime.core.table.virtual.spec.AppendMissingValuesTransformSpec;
import org.knime.core.table.virtual.spec.AppendTransformSpec;
import org.knime.core.table.virtual.spec.ConcatenateTransformSpec;
import org.knime.core.table.virtual.spec.MapTransformSpec;
import org.knime.core.table.virtual.spec.MapTransformSpec.MapperFactory;
import org.knime.core.table.virtual.spec.MaterializeTransformSpec;
import org.knime.core.table.virtual.spec.ProgressListenerTransformSpec.ProgressListenerFactory;
import org.knime.core.table.virtual.spec.ProgressListenerTransformSpec.ProgressListenerWithRowIndexFactory;
import org.knime.core.table.virtual.spec.ProgressListenerTransformSpec.ProgressListenerWithRowIndexFactory.ProgressListener;
import org.knime.core.table.virtual.spec.RowFilterTransformSpec;
import org.knime.core.table.virtual.spec.RowFilterTransformSpec.RowFilterFactory;
import org.knime.core.table.virtual.spec.SelectColumnsTransformSpec;
import org.knime.core.table.virtual.spec.SliceTransformSpec;
import org.knime.core.table.virtual.spec.SourceTableProperties;
import org.knime.core.table.virtual.spec.SourceTransformSpec;
import org.knime.core.table.virtual.spec.TableTransformSpec;

import com.google.common.collect.Collections2;

// TODO rename?
public final class VirtualTable {

    private final TableTransform m_transform;

    private final ColumnarSchema m_schema;

    // TODO missing:
    // 1. Distinguish between VirtualTable with "size()" method and Virtual Table without "size()"
    // --- use-case: I read a file but don't know how many rows I'll be reading
    // --- I just know I can iterate over the source.
    // --- Still I want to perform operations on the schema (filter columns) or perform other row-wise operations.
    //     Resulting table doesn't know size but can be iterated.

    // 2. Additional methods:
    // --- FlatMap (Row to Rows)
    // --- Predicate<Row>
    // --- aggregate() - e.g. sum over column or mean or
    // --- (later) sort() with indication that table is sorted
    // --- (later) join() ...
    // --- for infinite case (later): rows to row (windowing aggregate)

    // 3. Optimization of graph
    // --- simple optimization can be done while actually building the graph (e.g. squash two subsequent column filters
    //     or create map stages).
    // --- more complicated optimization (e.g. push down a predicate<row> as close to the source as possible needs to be
    //     done after the graph has been constructed.

    // 4. make all of that an interface and provide proper naming

    // Regarding the sized vs unsized, unsorted vs sorted, etc. distinctions: check how Java 8 streams do this ("stream
    // characteristics").

    /**
     * Construct a VirtualTable that wraps a source table with the given UUID and properties.
     *
     * @param sourceIdentifier unique identifier for the source. This is later used by the {@code VirtualTableExecutor}
     *            to attach a {@code RowAccessible}.
     * @param properties properties of the source table, such as its {@code ColumnarSchema}, whether it supports
     *            LookaheadCursors, etc.
     */
    public VirtualTable(final UUID sourceIdentifier, final SourceTableProperties properties) {
        m_transform = new TableTransform(new SourceTransformSpec(sourceIdentifier, properties));
        m_schema = properties.getSchema();
    }

    /**
     * Construct a VirtualTable that wraps a source table with the given UUID and schema.
     *
     * @param sourceIdentifier unique identifier for the source. This is later used by the {@code VirtualTableExecutor}
     *            to attach a {@code RowAccessible}.
     * @param schema the {@code ColumnarSchema} of the source
     * @param lookahead whether the source supports LookaheadCursors (meaning the {@code RowAccessible} attached to this
     *            source will be {@code LookaheadRowAccessible})
     */
    public VirtualTable(final UUID sourceIdentifier, final ColumnarSchema schema, final boolean lookahead) {
        this(sourceIdentifier, new SourceTableProperties(schema, lookahead));
    }

    /**
     * Construct a VirtualTable that wraps a source table with the given UUID and schema. It is assumed that the source
     * table does not support LookaheadCursors.
     *
     * @param sourceIdentifier unique identifier for the source. This is later used by the {@code VirtualTableExecutor}
     *            to attach a {@code RowAccessible}.
     * @param schema the {@code ColumnarSchema} of the source
     */
    public VirtualTable(final UUID sourceIdentifier, final ColumnarSchema schema) {
        this(sourceIdentifier, new SourceTableProperties(schema, false));
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
        final List<ColumnarSchema> schemas = collectSchemas(tables);
        final ColumnarSchema schema = ColumnarSchemas.append(schemas);
        final List<TableTransform> transforms = collectTransforms(tables);
        return new VirtualTable(new TableTransform(transforms, transformSpec), schema);
    }

    public VirtualTable append(final VirtualTable table) {
        return append(List.of(table));
    }

    private List<ColumnarSchema> collectSchemas(final List<VirtualTable> tables) {
        final List<ColumnarSchema> schemas = new ArrayList<>(1 + tables.size());
        schemas.add(m_schema);
        schemas.addAll(Collections2.transform(tables, VirtualTable::getSchema));
        return schemas;
    }

    private List<TableTransform> collectTransforms(final List<VirtualTable> tables) {
        final List<TableTransform> transforms = new ArrayList<>(1 + tables.size());
        transforms.add(m_transform);
        transforms.addAll(Collections2.transform(tables, VirtualTable::getProducingTransform));
        return transforms;
    }

    public VirtualTable appendMissingValueColumns(final List<DataSpec> columns, final List<DataTraits> traits) {
        var appendSchema =
            new DefaultColumnarSchema(columns.toArray(DataSpec[]::new), traits.toArray(DataTraits[]::new));
        final AppendMissingValuesTransformSpec transformSpec = new AppendMissingValuesTransformSpec(appendSchema);
        final ColumnarSchema schema = ColumnarSchemas.append(List.of(m_schema, appendSchema));
        return new VirtualTable(new TableTransform(m_transform, transformSpec), schema);
    }

    public VirtualTable concatenate(final List<VirtualTable> tables) {
        final TableTransformSpec transformSpec = new ConcatenateTransformSpec();
        final List<ColumnarSchema> schemas = collectSchemas(tables);
        final ColumnarSchema schema = ColumnarSchemas.concatenate(schemas);
        final List<TableTransform> transforms = collectTransforms(tables);
        return new VirtualTable(new TableTransform(transforms, transformSpec), schema);
    }

    public VirtualTable concatenate(final VirtualTable table) {
        return concatenate(List.of(table));
    }

    @Deprecated
    public VirtualTable filterColumns(final int... columnIndices) {
        return keepOnlyColumns(columnIndices);
    }

    @Deprecated
    public VirtualTable permute(final int... permutation) {
        return selectColumns(permutation);
    }

    /**
     * Create virtual table containing only the columns specified by {@code
     * columnIndices} in the specified order.
     *
     * @param columnIndices indices of the columns to select
     * @return virtual table containing only the specified columns, in the specified order.
     */
    public VirtualTable selectColumns(final int... columnIndices) {
        final TableTransformSpec transformSpec = new SelectColumnsTransformSpec(columnIndices);
        final ColumnarSchema schema = ColumnarSchemas.select(m_schema, columnIndices);
        return new VirtualTable(new TableTransform(m_transform, transformSpec), schema);
    }

    /**
     * Create virtual table containing all columns of this table, except the
     * ones specified by {@code columnIndices}.
     *
     * @param columnIndices indices of the columns to drop. may be in any order and contain duplicates.
     * @return virtual table where the specified columns have been removed
     */
    public VirtualTable dropColumns(final int... columnIndices) {
        return selectColumns(indicesAfterDrop(m_schema.numColumns(), columnIndices));
    }

    /**
     * Create virtual table containing only the columns specified by {@code
     * columnIndices}.
     * <p>
     * The order or the columns in the new table is the same as the order of
     * columns in this table. (The order of {@code columnIndices} does not
     * matter.)
     *
     * @param columnIndices indices of the columns to keep. may be in any order and contain duplicates.
     * @return virtual table containing only the specified columns
     */
    public VirtualTable keepOnlyColumns(final int... columnIndices) {
        return selectColumns(indicesAfterKeepOnly(columnIndices));
    }

    public VirtualTable slice(final long from, final long to) {
        final TableTransformSpec transformSpec = new SliceTransformSpec(from, to);
        return new VirtualTable(new TableTransform(m_transform, transformSpec), m_schema);
    }

    // TODO (TP) Implement RowIndex propagation.
    //      (*) MapperWithRowIndexFactory should probably get the actual row index from a Source node?
    //      (*) How to handle sliced Sources? Should indices start at 0 or at slice.from?
    //      (*) Add a signature
    //          VirtualTable.map(int[], MapperWithRowIndexFactory, VirtualTable),
    //          where the VirtualTable argument specifies which VirtualTable the row index should be taken from.
    //          Then the method below would be equivalent to
    //          VirtualTable.map(int[] c, MapperWithRowIndexFactory f) {
    //              return map(c,f,this);
    //          }
    public VirtualTable map(final int[] columnIndices, final MapperFactory mapperFactory) {
        final TableTransformSpec transformSpec = new MapTransformSpec(columnIndices, mapperFactory);
        return new VirtualTable(new TableTransform(m_transform, transformSpec), mapperFactory.getOutputSchema());
    }

    /**
     * Create a {@code new VirtualTable} by including only rows from this {@code
     * VirtualTable} that match a given predicate. This is defined by an array
     * of {@code n} column indices that form the inputs of the ({@code n}-ary}
     * filter predicate. The predicate is evaluated on the values of the
     * respective columns for each row. Rows for which the predicate evaluates
     * to {@code true} will be included, rows for which the filter predicate
     * evaluates to {@code false} will be removed (skipped). The filter is given
     * by a {@code RowFilterFactory} which can be used to create multiple
     * instances of the filter predicate for processing multiple lines in
     * parallel. (Each filter predicate is used single-threaded.) The order in
     * which {@code columnIndices} are given matters. For example if {@code
     * columnIndices = {5,1,4}}, then values from the 5th, 1st, and 4th column
     * are provided as inputs 0, 1, and 2, respectively, to the filter
     * predicate.
     *
     * @param columnIndices the indices of the columns that are passed to the filter predicate
     * @param filterFactory factory to create instances of the filter predicate
     * @return the filtered table
     */
    public VirtualTable filterRows(final int[] columnIndices, final RowFilterFactory filterFactory) {
        final TableTransformSpec transformSpec = new RowFilterTransformSpec(columnIndices, filterFactory);
        return new VirtualTable(new TableTransform(m_transform, transformSpec), m_schema);
    }

    // TODO: Should this take TargetTableProperties analogous to SourceTableProperties
    public VirtualTable materialize(final UUID sinkIdentifier) {
        final MaterializeTransformSpec transformSpec = new MaterializeTransformSpec(sinkIdentifier);
        return new VirtualTable(new TableTransform(m_transform, transformSpec), ColumnarSchema.of());
    }

    public VirtualTable resolveSources(final Map<UUID, VirtualTable> sourceMap) {
        var reSourcedTransform = m_transform.reSource(
            sourceMap.entrySet().stream()//
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getProducingTransform()))
            );
        return new VirtualTable(reSourcedTransform, m_schema);
    }

    // TODO (TP) Implement ProgressTransformSpec handling.
    //      As a workaround, we use a RowFilter that always evaluates to {@code
    //      true} but this should be fixed, because it stands in the way of
    //      optimizations, destroys lookahead capability for no reason, etc...
    public VirtualTable progress(final int[] columnIndices, final ProgressListenerFactory factory) {
        final TableTransformSpec transformSpec = new RowFilterTransformSpec(columnIndices, wrapAsRowFilterFactory(factory));
        return new VirtualTable(new TableTransform(m_transform, transformSpec), m_schema);
    }

    private static RowFilterFactory wrapAsRowFilterFactory(final ProgressListenerFactory factory) {
        return inputs -> {
            Runnable progress = factory.createProgressListener(inputs);
            return () -> {
                progress.run();
                return true;
            };
        };
    }

    public VirtualTable progress(final int[] columnIndices, final ProgressListenerWithRowIndexFactory factory) {
        return progress(columnIndices, wrapAsProgressListenerFactory(factory));
    }

    private static ProgressListenerFactory wrapAsProgressListenerFactory(final ProgressListenerWithRowIndexFactory factory) {
        return new ProgressListenerFactory() {
            @Override
            public Runnable createProgressListener(final ReadAccess[] inputs) {
                ProgressListener progress = factory.createProgressListener(inputs);
                return new Runnable() {
                    private long m_rowIndex = 0;

                    @Override
                    public void run() {
                        progress.update(m_rowIndex);
                        m_rowIndex++;
                    }
                };
            }
        };
    }
}
