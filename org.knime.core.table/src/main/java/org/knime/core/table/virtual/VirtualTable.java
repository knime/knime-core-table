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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.schema.DataSpecs;
import org.knime.core.table.schema.DataSpecs.DataSpecWithTraits;
import org.knime.core.table.schema.traits.DataTraits;
import org.knime.core.table.schema.traits.DefaultDataTraits;
import org.knime.core.table.virtual.expression.Ast;
import org.knime.core.table.virtual.expression.AstType;
import org.knime.core.table.virtual.expression.Exec;
import org.knime.core.table.virtual.expression.Exec.Computer;
import org.knime.core.table.virtual.expression.ExpressionGrammar;
import org.knime.core.table.virtual.expression.ExpressionGrammar.Expr;
import org.knime.core.table.virtual.expression.Typing;
import org.knime.core.table.virtual.spec.AppendMissingValuesTransformSpec;
import org.knime.core.table.virtual.spec.AppendTransformSpec;
import org.knime.core.table.virtual.spec.ColumnFilterTransformSpec;
import org.knime.core.table.virtual.spec.ConcatenateTransformSpec;
import org.knime.core.table.virtual.spec.MapTransformSpec;
import org.knime.core.table.virtual.spec.MapTransformSpec.MapperFactory;
import org.knime.core.table.virtual.spec.PermuteTransformSpec;
import org.knime.core.table.virtual.spec.RowFilterTransformSpec;
import org.knime.core.table.virtual.spec.RowFilterTransformSpec.RowFilterFactory;
import org.knime.core.table.virtual.spec.SliceTransformSpec;
import org.knime.core.table.virtual.spec.SourceTableProperties;
import org.knime.core.table.virtual.spec.SourceTransformSpec;
import org.knime.core.table.virtual.spec.TableTransformSpec;
import org.rekex.parser.ParseResult;
import org.rekex.parser.PegParser;

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
        final AppendMissingValuesTransformSpec transformSpec =
            new AppendMissingValuesTransformSpec(columns.toArray(DataSpec[]::new), traits.toArray(DataTraits[]::new));
        final ColumnarSchema schema = ColumnarSchemas.append(List.of(m_schema, transformSpec.getAppendedSchema()));
        return new VirtualTable(new TableTransform(List.of(m_transform), transformSpec), schema);
    }

    public VirtualTable concatenate(final List<VirtualTable> tables) {
        final TableTransformSpec transformSpec = new ConcatenateTransformSpec();
        final List<ColumnarSchema> schemas = collectSchemas(tables);
        final ColumnarSchema schema = ColumnarSchemas.concatenate(schemas);
        final List<TableTransform> transforms = collectTransforms(tables);
        return new VirtualTable(new TableTransform(transforms, transformSpec), schema);
    }

    public VirtualTable filterColumns(final int... columnIndices) {
        final TableTransformSpec transformSpec = new ColumnFilterTransformSpec(columnIndices);
        final ColumnarSchema schema = ColumnarSchemas.filter(m_schema, columnIndices);
        return new VirtualTable(new TableTransform(Arrays.asList(m_transform), transformSpec), schema);
    }

    public VirtualTable permute(final int... permutation) {
        final TableTransformSpec transformSpec = new PermuteTransformSpec(permutation);
        final ColumnarSchema schema = ColumnarSchemas.permute(m_schema, permutation);
        return new VirtualTable(new TableTransform(List.of(m_transform), transformSpec), schema);

    }

    public VirtualTable slice(final long from, final long to) {
        final TableTransformSpec transformSpec = new SliceTransformSpec(from, to);
        return new VirtualTable(new TableTransform(List.of(m_transform), transformSpec), m_schema);
    }

    public VirtualTable map(final int[] columnIndices, final MapperFactory mapperFactory) {
        final TableTransformSpec transformSpec = new MapTransformSpec(columnIndices, mapperFactory);
        return new VirtualTable(new TableTransform(List.of(m_transform), transformSpec), mapperFactory.getOutputSchema());
    }

    /**
     * @param expression expression that computes a new column
     * @param outputSpec DataSpec of the computed column. Must be assignable from the result type of the expression
     */
    public VirtualTable map(final String expression, final DataSpecWithTraits outputSpec) {
        final PegParser<Expr> parser = ExpressionGrammar.parser();
        final ParseResult<Expr> result = parser.parse(expression);
        if (result instanceof ParseResult.Full<Expr> full) {
            final Ast.Node ast = full.value().ast();
            final List<Ast.Node> postorder = Ast.postorder(ast);
            final Ast.RequiredColumns columns = Ast.getRequiredColumns(postorder);

            final IntFunction<AstType> columnIndexToAstType = i -> m_schema.getSpec(i).accept(Typing.toAstType);
            Typing.inferTypes(postorder, columnIndexToAstType);

            final IntFunction<Function<ReadAccess[], ? extends Computer>> columnIndexToComputerFactory =
                    columnIndex -> {
                        int inputIndex = columns.getInputIndex(columnIndex);
                        Function<ReadAccess, ? extends Computer> createComputer =
                                m_schema.getSpec(columnIndex).accept(Exec.toReaderFactory);
                        return readAccesses -> createComputer.apply(readAccesses[inputIndex]);
                    };

            var mapperFactory = Exec.createMapperFactory(ast, columnIndexToComputerFactory, outputSpec);
            return map(columns.columnIndices(), mapperFactory);
        } else {
            System.err.println("parse error:\n" + result);
            throw new IllegalArgumentException();
        }
    }

    public VirtualTable map(final String expression, final DataSpec outputSpec) {
        return map(expression, new DataSpecWithTraits(outputSpec, DefaultDataTraits.EMPTY));
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
        return new VirtualTable(new TableTransform(List.of(m_transform), transformSpec), m_schema);
    }

    /**
     * @param expression expression for filter predicate
     */
    public VirtualTable filterRows(final String expression) {
        final PegParser<Expr> parser = ExpressionGrammar.parser();
        final ParseResult<Expr> result = parser.parse(expression);
        if (result instanceof ParseResult.Full<Expr> full) {
            final Ast.Node ast = full.value().ast();
            final List<Ast.Node> postorder = Ast.postorder(ast);
            final Ast.RequiredColumns columns = Ast.getRequiredColumns(postorder);

            final IntFunction<AstType> columnIndexToAstType = i -> m_schema.getSpec(i).accept(Typing.toAstType);
            Typing.inferTypes(postorder, columnIndexToAstType);

            final IntFunction<Function<ReadAccess[], ? extends Computer>> columnIndexToComputerFactory =
                    columnIndex -> {
                        int inputIndex = columns.getInputIndex(columnIndex);
                        Function<ReadAccess, ? extends Computer> createComputer =
                                m_schema.getSpec(columnIndex).accept(Exec.toReaderFactory);
                        return readAccesses -> createComputer.apply(readAccesses[inputIndex]);
                    };

            var filterFactory = Exec.createRowFilterFactory(ast, columnIndexToComputerFactory);
            return filterRows(columns.columnIndices(), filterFactory);
        } else {
            System.err.println("parse error:\n" + result);
            throw new IllegalArgumentException();
        }
    }

    public VirtualTable resolveSources(final Map<UUID, VirtualTable> sourceMap) {
        var reSourcedTransform = m_transform.reSource(
            sourceMap.entrySet().stream()//
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getProducingTransform()))
            );
        return new VirtualTable(reSourcedTransform, m_schema);
    }

    //    /**
    //     * @param destination The {@link WriteAccessRow} of the destination must be compatible to the {@link #getSchema()
    //     *            schema} of this instance. The destination must be {@link Cursor#close() closed} by the caller of this
    //     *            method.
    //     */
    //    public void copy(final Cursor<WriteAccessRow> destination) throws IOException {
    //        // TODO: optimize graph
    //        try (final RowAccessible transformedTable = new TableTransformer(m_transform).transform()) {
    //            try (final Cursor<ReadAccessRow> source = transformedTable.createCursor()) {
    //                final ReadAccessRow readAccess = source.access();
    //                final WriteAccessRow writeAccess = destination.access();
    //                while (source.forward() && destination.forward()) {
    //                    writeAccess.setFrom(readAccess);
    //                }
    //            }
    //        }
    //    }
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
