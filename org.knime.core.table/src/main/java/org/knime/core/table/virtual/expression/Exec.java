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
 */
package org.knime.core.table.virtual.expression;

import static org.knime.core.expressions.Expressions.getInferredType;
import static org.knime.core.expressions.Expressions.getResolvedColumnIdx;

import java.util.Arrays;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.IntFunction;

import org.knime.core.expressions.Ast;
import org.knime.core.expressions.Computer;
import org.knime.core.expressions.Computer.BooleanComputer;
import org.knime.core.expressions.Computer.FloatComputer;
import org.knime.core.expressions.Computer.IntegerComputer;
import org.knime.core.expressions.Computer.StringComputer;
import org.knime.core.expressions.Expressions;
import org.knime.core.expressions.ValueType;
import org.knime.core.table.access.BooleanAccess;
import org.knime.core.table.access.BooleanAccess.BooleanReadAccess;
import org.knime.core.table.access.ByteAccess.ByteReadAccess;
import org.knime.core.table.access.DoubleAccess;
import org.knime.core.table.access.DoubleAccess.DoubleReadAccess;
import org.knime.core.table.access.FloatAccess.FloatReadAccess;
import org.knime.core.table.access.IntAccess.IntReadAccess;
import org.knime.core.table.access.LongAccess;
import org.knime.core.table.access.LongAccess.LongReadAccess;
import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.access.StringAccess;
import org.knime.core.table.access.StringAccess.StringReadAccess;
import org.knime.core.table.access.WriteAccess;
import org.knime.core.table.schema.BooleanDataSpec;
import org.knime.core.table.schema.ByteDataSpec;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.schema.DataSpecs;
import org.knime.core.table.schema.DataSpecs.DataSpecWithTraits;
import org.knime.core.table.schema.DoubleDataSpec;
import org.knime.core.table.schema.FloatDataSpec;
import org.knime.core.table.schema.IntDataSpec;
import org.knime.core.table.schema.ListDataSpec;
import org.knime.core.table.schema.LongDataSpec;
import org.knime.core.table.schema.StringDataSpec;
import org.knime.core.table.schema.StructDataSpec;
import org.knime.core.table.schema.VarBinaryDataSpec;
import org.knime.core.table.schema.VoidDataSpec;
import org.knime.core.table.virtual.spec.MapTransformSpec;
import org.knime.core.table.virtual.spec.MapTransformSpec.MapperFactory;
import org.knime.core.table.virtual.spec.RowFilterTransformSpec.RowFilterFactory;

import com.fasterxml.jackson.databind.annotation.JsonSerialize.Typing;

// Connects Expressions with virtual tables
@SuppressWarnings("restriction")
public interface Exec {

    /**
     * A visitor that maps {@code DataSpec} to the corresponding {@code AstType}.
     */
    DataSpec.Mapper<ValueType> DATA_SPEC_TO_AST_TYPE_MAPPER = new DataSpecToAstTypeMapper();

    // create Computer (of appropriate type) that reads from a ReadAccess
    DataSpec.Mapper<Function<ReadAccess, ? extends Computer>> toReaderFactory = new DataSpec.Mapper<>() {
        @Override
        public Function<ReadAccess, BooleanComputer> visit(final BooleanDataSpec spec) {
            return ra -> BooleanComputer.of(((BooleanReadAccess)ra)::getBooleanValue, ra::isMissing);
        }

        @Override
        public Function<ReadAccess, IntegerComputer> visit(final ByteDataSpec spec) {
            return ra -> IntegerComputer.of(((ByteReadAccess)ra)::getByteValue, ra::isMissing);
        }

        @Override
        public Function<ReadAccess, FloatComputer> visit(final DoubleDataSpec spec) {
            return ra -> FloatComputer.of(((DoubleReadAccess)ra)::getDoubleValue, ra::isMissing);
        }

        @Override
        public Function<ReadAccess, FloatComputer> visit(final FloatDataSpec spec) {
            return ra -> FloatComputer.of(((FloatReadAccess)ra)::getFloatValue, ra::isMissing);
        }

        @Override
        public Function<ReadAccess, IntegerComputer> visit(final IntDataSpec spec) {
            return ra -> IntegerComputer.of(((IntReadAccess)ra)::getIntValue, ra::isMissing);
        }

        @Override
        public Function<ReadAccess, IntegerComputer> visit(final LongDataSpec spec) {
            return ra -> IntegerComputer.of(((LongReadAccess)ra)::getLongValue, ra::isMissing);
        }

        @Override
        public Function<ReadAccess, Computer> visit(final VarBinaryDataSpec spec) {
            throw new IllegalArgumentException("TODO: How to handle VarBinaryDataSpec in expressions?"); // TODO
        }

        @Override
        public Function<ReadAccess, Computer> visit(final VoidDataSpec spec) {
            throw new IllegalArgumentException("TODO: How to handle VoidDataSpec in expressions?"); // TODO
        }

        @Override
        public Function<ReadAccess, Computer> visit(final StructDataSpec spec) {
            throw new IllegalArgumentException("TODO: How to handle StructDataSpec in expressions?"); // TODO
        }

        @Override
        public Function<ReadAccess, Computer> visit(final ListDataSpec listDataSpec) {
            throw new IllegalArgumentException("TODO: How to handle ListDataSpec in expressions?"); // TODO
        }

        @Override
        public Function<ReadAccess, StringComputer> visit(final StringDataSpec spec) {
            return ra -> StringComputer.of(((StringReadAccess)ra)::getStringValue, ra::isMissing);
        }
    };

    // create Runnable that writes value from Computer (of appropriate type) to WriteAccess (of appropriate type)
    DataSpec.Mapper<BiFunction<WriteAccess, Computer, Runnable>> toWriterFactory = new DataSpec.Mapper<>() {

        private Runnable setMissingOrSetValue(final Computer c, final WriteAccess a, final Runnable setValue) {
            return () -> {
                if (c.isMissing()) {
                    a.setMissing();
                } else {
                    setValue.run();
                }
            };
        }

        @Override
        public BiFunction<WriteAccess, Computer, Runnable> visit(final BooleanDataSpec spec) {
            return (access, computer) -> {
                var a = (BooleanAccess.BooleanWriteAccess)access;
                var c = (BooleanComputer)computer;
                return setMissingOrSetValue(c, a, () -> a.setBooleanValue(c.compute()));
            };
        }

        @Override
        public BiFunction<WriteAccess, Computer, Runnable> visit(final DoubleDataSpec spec) {
            return (access, computer) -> {
                var a = (DoubleAccess.DoubleWriteAccess)access;
                var c = (FloatComputer)computer;
                return setMissingOrSetValue(c, a, () -> a.setDoubleValue(c.compute()));
            };
        }

        @Override
        public BiFunction<WriteAccess, Computer, Runnable> visit(final LongDataSpec spec) {
            return (access, computer) -> {
                var a = (LongAccess.LongWriteAccess)access;
                var c = (IntegerComputer)computer;
                return setMissingOrSetValue(c, a, () -> a.setLongValue(c.compute()));
            };
        }

        @Override
        public BiFunction<WriteAccess, Computer, Runnable> visit(final StringDataSpec spec) {
            return (access, computer) -> {
                var a = (StringAccess.StringWriteAccess)access;
                var c = (StringComputer)computer;
                return setMissingOrSetValue(c, a, () -> a.setStringValue(c.compute()));
            };
        }

        @Override
        public BiFunction<WriteAccess, Computer, Runnable> visit(final IntDataSpec spec) {
            throw new IllegalArgumentException("TODO: How to handle VarBinaryDataSpec in expressions?"); // TODO
        }

        @Override
        public BiFunction<WriteAccess, Computer, Runnable> visit(final FloatDataSpec spec) {
            throw new IllegalArgumentException("TODO: How to handle VarBinaryDataSpec in expressions?"); // TODO
        }

        @Override
        public BiFunction<WriteAccess, Computer, Runnable> visit(final ByteDataSpec spec) {
            throw new IllegalArgumentException("TODO: How to handle VarBinaryDataSpec in expressions?"); // TODO
        }

        @Override
        public BiFunction<WriteAccess, Computer, Runnable> visit(final VarBinaryDataSpec spec) {
            throw new IllegalArgumentException("TODO: How to handle VarBinaryDataSpec in expressions?"); // TODO
        }

        @Override
        public BiFunction<WriteAccess, Computer, Runnable> visit(final VoidDataSpec spec) {
            throw new IllegalArgumentException("TODO: How to handle VoidDataSpec in expressions?"); // TODO
        }

        @Override
        public BiFunction<WriteAccess, Computer, Runnable> visit(final StructDataSpec spec) {
            throw new IllegalArgumentException("TODO: How to handle StructDataSpec in expressions?"); // TODO
        }

        @Override
        public BiFunction<WriteAccess, Computer, Runnable> visit(final ListDataSpec listDataSpec) {
            throw new IllegalArgumentException("TODO: How to handle ListDataSpec in expressions?"); // TODO
        }
    };

    /**
     * Create a {@code MapTransformSpec.MapperFactory}, that is, the final realization of the expression.
     *
     * @param ast root note of AST of the expression.
     * @param columnIndexToComputerFactory function that maps column index to a factory that produces {@code Computer}
     *            (of matching type) from the {@code ReadAccess[]} array given to the {@code MapperFactory}.
     * @param outputSpec spec of result column
     * @return a {@code MapperFactory} that implements the given expression.
     * @throws IllegalArgumentException TODO: Hrmm ... is there a better exception type? if the {@code DataSpec} of the
     *             result column is incompatible with the {@link Typing#getType(Ast) inferred type} of the expression.
     */
    // TODO get rid of outputSpec argument
    static MapperFactory createMapperFactory( //
        final Ast ast, //
        final IntFunction<Function<ReadAccess[], ? extends Computer>> columnIndexToComputerFactory, //
        final DataSpecs.DataSpecWithTraits outputSpec //
    ) {
        final ValueType astType = getInferredType(ast);
        final ValueType colType = outputSpec.spec().accept(DATA_SPEC_TO_AST_TYPE_MAPPER);
        final BiFunction<WriteAccess, Computer, Runnable> writerFactory = outputSpec.spec().accept(toWriterFactory);
        final Function<ReadAccess[], Computer> computerFactory =
            createComputerFactory(ast, columnIndexToComputerFactory);
        final BiFunction<ReadAccess[], WriteAccess[], Runnable> factory =
            (readAccesses, writeAccesses) -> writerFactory.apply(writeAccesses[0], computerFactory.apply(readAccesses));

        return MapperFactory.of(ColumnarSchema.of(outputSpec), factory);
    }

    /**
     * TODO javadoc
     *
     * @param ast
     * @param columnIndexToComputerFactory
     * @return
     */
    static RowFilterFactory createRowFilterFactory( //
        final Ast ast, //
        final IntFunction<Function<ReadAccess[], ? extends Computer>> columnIndexToComputerFactory //
    ) {
        // TODO assert by inferred type that the result cannot be missing
        final Function<ReadAccess[], Computer> computerFactory =
            createComputerFactory(ast, columnIndexToComputerFactory);
        return inputs -> ((BooleanComputer)computerFactory.apply(inputs))::compute;
    }

    /**
     * TODO javadoc
     *
     * @param ast
     * @param columnIndexToComputerFactory
     * @return
     */
    static Function<ReadAccess[], Computer> createComputerFactory( //
        final Ast ast, //
        final IntFunction<Function<ReadAccess[], ? extends Computer>> columnIndexToComputerFactory //
    ) {
        // TODO can we delete the postorder function now?
        return readAccesses -> {
            try {
                return Expressions.evaluate(ast, //
                    columnAccess -> columnIndexToComputerFactory.apply(getResolvedColumnIdx(columnAccess))
                        .apply(readAccesses)//
                );
            } catch (Exception ex) {
                // TODO handle exception properly
                throw new IllegalStateException(ex);
            }
        };

    }

    final class DataSpecToAstTypeMapper implements DataSpec.Mapper<ValueType> {

        @Override
        public ValueType visit(final BooleanDataSpec spec) {
            return ValueType.BOOLEAN;
        }

        @Override
        public ValueType visit(final ByteDataSpec spec) {
            return ValueType.INTEGER;
        }

        @Override
        public ValueType visit(final DoubleDataSpec spec) {
            return ValueType.FLOAT;
        }

        @Override
        public ValueType visit(final FloatDataSpec spec) {
            return ValueType.FLOAT;
        }

        @Override
        public ValueType visit(final IntDataSpec spec) {
            return ValueType.INTEGER;
        }

        @Override
        public ValueType visit(final LongDataSpec spec) {
            return ValueType.INTEGER;
        }

        @Override
        public ValueType visit(final VarBinaryDataSpec spec) {
            throw new IllegalArgumentException("Binary columns are not supported in expressions");
        }

        @Override
        public ValueType visit(final VoidDataSpec spec) {
            throw new IllegalArgumentException("Void columns are not supported in expressions");
        }

        @Override
        public ValueType visit(final StructDataSpec spec) {
            throw new IllegalArgumentException("Struct columns are not supported in expressions?");
        }

        @Override
        public ValueType visit(final ListDataSpec listDataSpec) {
            throw new IllegalArgumentException("List columns are not supported in expressions?");
        }

        @Override
        public ValueType visit(final StringDataSpec spec) {
            return ValueType.STRING;
        }
    }

    /**
     * Map AstTypes to DataSpecs
     *
     * @param valueType
     * @return DataSpec
     */
    static DataSpecWithTraits toDataSpec(final ValueType valueType) {
        if (ValueType.BOOLEAN.equals(valueType.baseType())) {
            return DataSpecs.BOOLEAN;
        } else if (ValueType.INTEGER.equals(valueType.baseType())) {
            return DataSpecs.LONG;
        } else if (ValueType.FLOAT.equals(valueType.baseType())) {
            return DataSpecs.DOUBLE;
        } else if (ValueType.STRING.equals(valueType.baseType())) {
            return DataSpecs.STRING;
        } else {
            throw new IllegalArgumentException("The value type " + valueType.name() + " cannot be mapped to DataSpecs");
        }
    }

    /**
     * Determine the input columns occurring in the given {@code Ast.Node}s. Compute a map from column index (inputs to
     * the table, that is AstColumnIndex.columnIndex()) to input index of the
     * {@link MapTransformSpec.MapperFactory#createMapper mapper} function. For example, if an expression uses (only)
     * "$[2]" and "$[5]" then these would map to input indices 0 and 1, respectively.
     *
     * @param expression
     * @return mapping from column index to mapper input index, and vice versa
     */
    static <D> RequiredColumns getRequiredColumns(final Ast expression) {
        var nodes = Ast.postorder(expression);
        int[] columnIndices = nodes.stream().mapToInt(node -> {
            if (node instanceof Ast.ColumnAccess n) {
                return getResolvedColumnIdx(n);
            } else {
                return -1;
            }
        }).filter(i -> i != -1).distinct().toArray();
        return new RequiredColumns(columnIndices);
    }

    record RequiredColumns(int[] columnIndices) {
        public int getInputIndex(final int columnIndex) {
            for (int i = 0; i < columnIndices.length; i++) {
                if (columnIndices[i] == columnIndex) {
                    return i;
                }
            }
            throw new IndexOutOfBoundsException();
        }

        @Override
        public String toString() {
            return "RequiredColumns" + Arrays.toString(columnIndices);
        }
    }
}
