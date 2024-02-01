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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;
import java.util.function.DoubleSupplier;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.knime.core.table.access.BooleanAccess;
import org.knime.core.table.access.ByteAccess;
import org.knime.core.table.access.DoubleAccess;
import org.knime.core.table.access.FloatAccess;
import org.knime.core.table.access.IntAccess;
import org.knime.core.table.access.LongAccess;
import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.access.StringAccess;
import org.knime.core.table.access.WriteAccess;
import org.knime.core.table.schema.BooleanDataSpec;
import org.knime.core.table.schema.ByteDataSpec;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.schema.DataSpecs;
import org.knime.core.table.schema.DoubleDataSpec;
import org.knime.core.table.schema.FloatDataSpec;
import org.knime.core.table.schema.IntDataSpec;
import org.knime.core.table.schema.ListDataSpec;
import org.knime.core.table.schema.LongDataSpec;
import org.knime.core.table.schema.StringDataSpec;
import org.knime.core.table.schema.StructDataSpec;
import org.knime.core.table.schema.VarBinaryDataSpec;
import org.knime.core.table.schema.VoidDataSpec;
import org.knime.core.table.virtual.spec.MapTransformSpec.MapperFactory;
import org.knime.core.table.virtual.spec.RowFilterTransformSpec.RowFilterFactory;

public interface Exec {

    interface Computer {
    }

    @FunctionalInterface
    interface BooleanComputer extends Computer, BooleanSupplier {

        static BooleanComputer column(final ReadAccess access) {
            var a = (BooleanAccess.BooleanReadAccess)access;
            return a::getBooleanValue;
        }

        static BooleanComputer unary(final Ast.UnaryOp.Operator op, final BooleanComputer arg1) {
            return switch (op) {
                case NOT -> () -> !arg1.getAsBoolean();
                case MINUS -> throw new IllegalStateException("Unary operator " + op + " is not applicable to BOOLEAN");
            };
        }

        static BooleanComputer binary(final Ast.BinaryOp.Operator op, final BooleanComputer arg1,
            final BooleanComputer arg2) {
            return switch (op) {
                case EQUAL_TO -> () -> arg1.getAsBoolean() == arg2.getAsBoolean();
                case NOT_EQUAL_TO -> () -> arg1.getAsBoolean() != arg2.getAsBoolean();
                case CONDITIONAL_AND -> () -> arg1.getAsBoolean() && arg2.getAsBoolean();
                case CONDITIONAL_OR -> () -> arg1.getAsBoolean() || arg2.getAsBoolean();
                case PLUS, MINUS, MULTIPLY, DIVIDE, REMAINDER, LESS_THAN, //
                        LESS_THAN_EQUAL, GREATER_THAN, GREATER_THAN_EQUAL //
                        -> throw new IllegalStateException("Binary operator " + op + " is not applicable to BOOLEAN");
            };
        }

        static BooleanComputer comparison(final Ast.BinaryOp.Operator op, final BooleanComputer arg1,
            final BooleanComputer arg2) {
            return switch (op) {
                case EQUAL_TO -> () -> arg1.getAsBoolean() == arg2.getAsBoolean();
                case NOT_EQUAL_TO -> () -> arg1.getAsBoolean() != arg2.getAsBoolean();
                case PLUS, MINUS, MULTIPLY, DIVIDE, REMAINDER, LESS_THAN, LESS_THAN_EQUAL, //
                        GREATER_THAN, GREATER_THAN_EQUAL, CONDITIONAL_AND, CONDITIONAL_OR //
                        -> throw new IllegalStateException("Unexpected operator " + op);
            };
        }
    }

    @FunctionalInterface
    interface DoubleComputer extends Computer, DoubleSupplier {

        static DoubleComputer column(final ReadAccess access) {
            var a = (DoubleAccess.DoubleReadAccess)access;
            return () -> a.getDoubleValue();
        }

        static DoubleComputer unary(final Ast.UnaryOp.Operator op, final DoubleComputer arg1) {
            return switch (op) {
                case MINUS -> () -> -arg1.getAsDouble();
                default -> throw new IllegalStateException("Unary operator " + op + " is not applicable to DOUBLE");
            };
        }

        static DoubleComputer binary(final Ast.BinaryOp.Operator op, final DoubleComputer arg1,
            final DoubleComputer arg2) {
            return switch (op) {
                case PLUS -> () -> arg1.getAsDouble() + arg2.getAsDouble();
                case MINUS -> () -> arg1.getAsDouble() - arg2.getAsDouble();
                case MULTIPLY -> () -> arg1.getAsDouble() * arg2.getAsDouble();
                case DIVIDE -> () -> arg1.getAsDouble() / arg2.getAsDouble();
                case REMAINDER -> () -> arg1.getAsDouble() % arg2.getAsDouble();
                case EQUAL_TO, NOT_EQUAL_TO, LESS_THAN, LESS_THAN_EQUAL, GREATER_THAN, //
                        GREATER_THAN_EQUAL, CONDITIONAL_AND, CONDITIONAL_OR //
                        -> throw new IllegalStateException("Binary operator " + op + " is not applicable to DOUBLE");
            };
        }

        static BooleanComputer comparison(final Ast.BinaryOp.Operator op, final DoubleComputer arg1,
            final DoubleComputer arg2) {
            return switch (op) {
                case EQUAL_TO -> () -> arg1.getAsDouble() == arg2.getAsDouble();
                case NOT_EQUAL_TO -> () -> arg1.getAsDouble() != arg2.getAsDouble();
                case LESS_THAN -> () -> arg1.getAsDouble() < arg2.getAsDouble();
                case LESS_THAN_EQUAL -> () -> arg1.getAsDouble() <= arg2.getAsDouble();
                case GREATER_THAN -> () -> arg1.getAsDouble() > arg2.getAsDouble();
                case GREATER_THAN_EQUAL -> () -> arg1.getAsDouble() >= arg2.getAsDouble();
                case PLUS, MINUS, MULTIPLY, DIVIDE, REMAINDER, CONDITIONAL_AND, CONDITIONAL_OR //
                        -> throw new IllegalStateException("Unexpected operator " + op);
            };
        }
    }

    @FunctionalInterface
    interface FloatComputer extends DoubleComputer {

        float getAsFloat();

        @Override
        default double getAsDouble() {
            return getAsFloat();
        }

        static FloatComputer column(final ReadAccess access) {
            var a = (FloatAccess.FloatReadAccess)access;
            return () -> a.getFloatValue();
        }

        static FloatComputer unary(final Ast.UnaryOp.Operator op, final FloatComputer arg1) {
            return switch (op) {
                case MINUS -> () -> -arg1.getAsFloat();
                default -> throw new IllegalStateException("Unary operator " + op + " is not applicable to FLOAT");
            };
        }

        static FloatComputer binary(final Ast.BinaryOp.Operator op, final FloatComputer arg1,
            final FloatComputer arg2) {
            return switch (op) {
                case PLUS -> () -> arg1.getAsFloat() + arg2.getAsFloat();
                case MINUS -> () -> arg1.getAsFloat() - arg2.getAsFloat();
                case MULTIPLY -> () -> arg1.getAsFloat() * arg2.getAsFloat();
                case DIVIDE -> () -> arg1.getAsFloat() / arg2.getAsFloat();
                case REMAINDER -> () -> arg1.getAsFloat() % arg2.getAsFloat();
                case EQUAL_TO, NOT_EQUAL_TO, LESS_THAN, LESS_THAN_EQUAL, GREATER_THAN, //
                        GREATER_THAN_EQUAL, CONDITIONAL_AND, CONDITIONAL_OR //
                        -> throw new IllegalStateException("Binary operator " + op + " is not applicable to FLOAT");
            };
        }

        static BooleanComputer comparison(final Ast.BinaryOp.Operator op, final FloatComputer arg1,
            final FloatComputer arg2) {
            return switch (op) {
                case EQUAL_TO -> () -> arg1.getAsFloat() == arg2.getAsFloat();
                case NOT_EQUAL_TO -> () -> arg1.getAsFloat() != arg2.getAsFloat();
                case LESS_THAN -> () -> arg1.getAsFloat() < arg2.getAsFloat();
                case LESS_THAN_EQUAL -> () -> arg1.getAsFloat() <= arg2.getAsFloat();
                case GREATER_THAN -> () -> arg1.getAsFloat() > arg2.getAsFloat();
                case GREATER_THAN_EQUAL -> () -> arg1.getAsFloat() >= arg2.getAsFloat();
                case PLUS, MINUS, MULTIPLY, DIVIDE, REMAINDER, CONDITIONAL_AND, CONDITIONAL_OR //
                        -> throw new IllegalStateException("Unexpected operator " + op);
            };
        }
    }

    @FunctionalInterface
    interface LongComputer extends FloatComputer, LongSupplier {

        @Override
        default float getAsFloat() {
            return getAsLong();
        }

        @Override
        default double getAsDouble() {
            return getAsLong();
        }

        static LongComputer column(final ReadAccess access) {
            var a = (LongAccess.LongReadAccess)access;
            return () -> a.getLongValue();
        }

        static LongComputer unary(final Ast.UnaryOp.Operator op, final LongComputer arg1) {
            return switch (op) {
                case MINUS -> () -> -arg1.getAsLong();
                default -> throw new IllegalStateException("Unary operator " + op + " is not applicable to LONG");
            };
        }

        static LongComputer binary(final Ast.BinaryOp.Operator op, final LongComputer arg1, final LongComputer arg2) {
            return switch (op) {
                case PLUS -> () -> arg1.getAsLong() + arg2.getAsLong();
                case MINUS -> () -> arg1.getAsLong() - arg2.getAsLong();
                case MULTIPLY -> () -> arg1.getAsLong() * arg2.getAsLong();
                case DIVIDE -> () -> arg1.getAsLong() / arg2.getAsLong();
                case REMAINDER -> () -> arg1.getAsLong() % arg2.getAsLong();
                case EQUAL_TO, NOT_EQUAL_TO, LESS_THAN, LESS_THAN_EQUAL, GREATER_THAN, //
                        GREATER_THAN_EQUAL, CONDITIONAL_AND, CONDITIONAL_OR //
                        -> throw new IllegalStateException("Binary operator " + op + " is not applicable to LONG");
            };
        }

        static BooleanComputer comparison(final Ast.BinaryOp.Operator op, final LongComputer arg1,
            final LongComputer arg2) {
            return switch (op) {
                case EQUAL_TO -> () -> arg1.getAsLong() == arg2.getAsLong();
                case NOT_EQUAL_TO -> () -> arg1.getAsLong() != arg2.getAsLong();
                case LESS_THAN -> () -> arg1.getAsLong() < arg2.getAsLong();
                case LESS_THAN_EQUAL -> () -> arg1.getAsLong() <= arg2.getAsLong();
                case GREATER_THAN -> () -> arg1.getAsLong() > arg2.getAsLong();
                case GREATER_THAN_EQUAL -> () -> arg1.getAsLong() >= arg2.getAsLong();
                case PLUS, MINUS, MULTIPLY, DIVIDE, REMAINDER, CONDITIONAL_AND, CONDITIONAL_OR //
                        -> throw new IllegalStateException("Unexpected operator " + op);
            };
        }
    }

    @FunctionalInterface
    interface IntComputer extends LongComputer, IntSupplier {

        @Override
        default long getAsLong() {
            return getAsInt();
        }

        @Override
        default float getAsFloat() {
            return getAsInt();
        }

        @Override
        default double getAsDouble() {
            return getAsInt();
        }

        static IntComputer column(final ReadAccess access) {
            var a = (IntAccess.IntReadAccess)access;
            return () -> a.getIntValue();
        }

        static IntComputer unary(final Ast.UnaryOp.Operator op, final IntComputer arg1) {
            return switch (op) {
                case MINUS -> () -> -arg1.getAsInt();
                default -> throw new IllegalStateException("Unary operator " + op + " is not applicable to INT");
            };
        }

        static IntComputer binary(final Ast.BinaryOp.Operator op, final IntComputer arg1, final IntComputer arg2) {
            return switch (op) {
                case PLUS -> () -> arg1.getAsInt() + arg2.getAsInt();
                case MINUS -> () -> arg1.getAsInt() - arg2.getAsInt();
                case MULTIPLY -> () -> arg1.getAsInt() * arg2.getAsInt();
                case DIVIDE -> () -> arg1.getAsInt() / arg2.getAsInt();
                case REMAINDER -> () -> arg1.getAsInt() % arg2.getAsInt();
                case EQUAL_TO, NOT_EQUAL_TO, LESS_THAN, LESS_THAN_EQUAL, GREATER_THAN, //
                        GREATER_THAN_EQUAL, CONDITIONAL_AND, CONDITIONAL_OR //
                        -> throw new IllegalStateException("Binary operator " + op + " is not applicable to INT");
            };
        }

        static BooleanComputer comparison(final Ast.BinaryOp.Operator op, final IntComputer arg1,
            final IntComputer arg2) {
            return switch (op) {
                case EQUAL_TO -> () -> arg1.getAsInt() == arg2.getAsInt();
                case NOT_EQUAL_TO -> () -> arg1.getAsInt() != arg2.getAsInt();
                case LESS_THAN -> () -> arg1.getAsInt() < arg2.getAsInt();
                case LESS_THAN_EQUAL -> () -> arg1.getAsInt() <= arg2.getAsInt();
                case GREATER_THAN -> () -> arg1.getAsInt() > arg2.getAsInt();
                case GREATER_THAN_EQUAL -> () -> arg1.getAsInt() >= arg2.getAsInt();
                case PLUS, MINUS, MULTIPLY, DIVIDE, REMAINDER, CONDITIONAL_AND, CONDITIONAL_OR //
                        -> throw new IllegalStateException("Unexpected operator " + op);
            };
        }
    }

    @FunctionalInterface
    interface ByteComputer extends IntComputer {

        byte getAsByte();

        @Override
        default int getAsInt() {
            return getAsByte();
        }

        @Override
        default long getAsLong() {
            return getAsByte();
        }

        @Override
        default float getAsFloat() {
            return getAsByte();
        }

        @Override
        default double getAsDouble() {
            return getAsByte();
        }

        static ByteComputer column(final ReadAccess access) {
            var a = (ByteAccess.ByteReadAccess)access;
            return () -> a.getByteValue();
        }

        static BooleanComputer comparison(final Ast.BinaryOp.Operator op, final ByteComputer arg1,
            final ByteComputer arg2) {
            return switch (op) {
                case EQUAL_TO -> () -> arg1.getAsByte() == arg2.getAsByte();
                case NOT_EQUAL_TO -> () -> arg1.getAsByte() != arg2.getAsByte();
                case LESS_THAN -> () -> arg1.getAsByte() < arg2.getAsByte();
                case LESS_THAN_EQUAL -> () -> arg1.getAsByte() <= arg2.getAsByte();
                case GREATER_THAN -> () -> arg1.getAsByte() > arg2.getAsByte();
                case GREATER_THAN_EQUAL -> () -> arg1.getAsByte() >= arg2.getAsByte();
                case PLUS, MINUS, MULTIPLY, DIVIDE, REMAINDER, CONDITIONAL_AND, CONDITIONAL_OR //
                        -> throw new IllegalStateException("Unexpected operator " + op);
            };
        }
    }

    @FunctionalInterface
    interface StringComputer extends Computer, Supplier<String> {

        static StringComputer column(final ReadAccess access) {
            var a = (StringAccess.StringReadAccess)access;
            return () -> a.getStringValue();
        }

        static BooleanComputer comparison(final Ast.BinaryOp.Operator op, final StringComputer arg1,
            final StringComputer arg2) {
            return switch (op) {
                case EQUAL_TO -> () -> Objects.equals(arg1.get(), arg2.get());
                case NOT_EQUAL_TO -> () -> !Objects.equals(arg1.get(), arg2.get());
                case PLUS, MINUS, MULTIPLY, DIVIDE, REMAINDER, LESS_THAN, LESS_THAN_EQUAL, //
                        GREATER_THAN, GREATER_THAN_EQUAL, CONDITIONAL_AND, CONDITIONAL_OR //
                        -> throw new IllegalStateException("Unexpected operator " + op);
            };
        }
    }

    static Computer binary(final Ast.BinaryOp n, final Function<Ast.Node, Computer> computers) {
        var arg1 = computers.apply(n.arg1());
        var arg2 = computers.apply(n.arg2());
        var t1 = n.arg1().inferredType();
        var t2 = n.arg2().inferredType();
        final Ast.BinaryOp.Operator op = n.op();
        return switch (op.type()) {
            case ARITHMETIC, LOGICAL -> binary(n.inferredType(), op, arg1, arg2);
            case EQUALITY -> compare(Typing.equalityType(t1, t2), op, arg1, arg2);
            case ORDERING -> compare(Typing.orderingType(t1, t2), op, arg1, arg2);
        };
    }

    // create Computer for arithmetic BinaryOp
    static Computer binary(final AstType type, final Ast.BinaryOp.Operator op, final Computer arg1,
        final Computer arg2) {
        return switch (type) {
            case STRING -> throw new UnsupportedOperationException("TODO: not implemented"); // TODO
            case BYTE -> throw new IllegalStateException("no binary op should have BYTE as a result");
            case BOOLEAN -> BooleanComputer.binary(op, (BooleanComputer)arg1, (BooleanComputer)arg2);
            case INT -> IntComputer.binary(op, (IntComputer)arg1, (IntComputer)arg2);
            case LONG -> LongComputer.binary(op, (LongComputer)arg1, (LongComputer)arg2);
            case FLOAT -> FloatComputer.binary(op, (FloatComputer)arg1, (FloatComputer)arg2);
            case DOUBLE -> DoubleComputer.binary(op, (DoubleComputer)arg1, (DoubleComputer)arg2);
        };
    }

    // create Computer for order-comparison BinaryOp
    static BooleanComputer compare(final AstType type, final Ast.BinaryOp.Operator op, final Computer arg1,
        final Computer arg2) {
        return switch (type) {
            case STRING -> StringComputer.comparison(op, (StringComputer)arg1, (StringComputer)arg2);
            case BOOLEAN -> BooleanComputer.comparison(op, (BooleanComputer)arg1, (BooleanComputer)arg2);
            case BYTE -> ByteComputer.comparison(op, (ByteComputer)arg1, (ByteComputer)arg2);
            case INT -> IntComputer.comparison(op, (IntComputer)arg1, (IntComputer)arg2);
            case LONG -> LongComputer.comparison(op, (LongComputer)arg1, (LongComputer)arg2);
            case FLOAT -> FloatComputer.comparison(op, (FloatComputer)arg1, (FloatComputer)arg2);
            case DOUBLE -> DoubleComputer.comparison(op, (DoubleComputer)arg1, (DoubleComputer)arg2);
        };
    }

    // create Computer for UnaryOp
    static Computer unary(final AstType type, final Ast.UnaryOp.Operator op, final Computer arg1) {
        return switch (type) {
            case STRING -> throw new UnsupportedOperationException("TODO: not implemented"); // TODO
            case BYTE -> throw new IllegalStateException("no unary op should have BYTE as a result");
            case BOOLEAN -> BooleanComputer.unary(op, (BooleanComputer)arg1);
            case INT -> IntComputer.unary(op, (IntComputer)arg1);
            case LONG -> LongComputer.unary(op, (LongComputer)arg1);
            case FLOAT -> FloatComputer.unary(op, (FloatComputer)arg1);
            case DOUBLE -> DoubleComputer.unary(op, (DoubleComputer)arg1);
        };
    }

    // create Computer (of appropriate type) that reads from a ReadAccess
    DataSpec.Mapper<Function<ReadAccess, ? extends Computer>> toReaderFactory = new DataSpec.Mapper<>() {
        @Override
        public Function<ReadAccess, Computer> visit(final BooleanDataSpec spec) {
            throw new IllegalArgumentException("TODO not implemented"); // TODO
        }

        @Override
        public Function<ReadAccess, ByteComputer> visit(final ByteDataSpec spec) {
            return ByteComputer::column;
        }

        @Override
        public Function<ReadAccess, DoubleComputer> visit(final DoubleDataSpec spec) {
            return DoubleComputer::column;
        }

        @Override
        public Function<ReadAccess, FloatComputer> visit(final FloatDataSpec spec) {
            return FloatComputer::column;
        }

        @Override
        public Function<ReadAccess, IntComputer> visit(final IntDataSpec spec) {
            return IntComputer::column;
        }

        @Override
        public Function<ReadAccess, Computer> visit(final LongDataSpec spec) {
            return LongComputer::column;
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
        public Function<ReadAccess, Computer> visit(final StringDataSpec spec) {
            throw new IllegalArgumentException("TODO not implemented"); // TODO
        }
    };

    // create Runnable that writes value from Computer (of appropriate type) to WriteAccess (of appropriate type)
    DataSpec.Mapper<BiFunction<WriteAccess, Computer, Runnable>> toWriterFactory = new DataSpec.Mapper<>() {
        @Override
        public BiFunction<WriteAccess, Computer, Runnable> visit(final BooleanDataSpec spec) {
            return (access, computer) -> {
                var a = (BooleanAccess.BooleanWriteAccess)access;
                var c = (BooleanComputer)computer;
                return () -> a.setBooleanValue(c.getAsBoolean());
            };
        }

        @Override
        public BiFunction<WriteAccess, Computer, Runnable> visit(final ByteDataSpec spec) {
            return (access, computer) -> {
                var a = (ByteAccess.ByteWriteAccess)access;
                var c = (ByteComputer)computer;
                return () -> a.setByteValue(c.getAsByte());
            };
        }

        @Override
        public BiFunction<WriteAccess, Computer, Runnable> visit(final DoubleDataSpec spec) {
            return (access, computer) -> {
                var a = (DoubleAccess.DoubleWriteAccess)access;
                var c = (DoubleComputer)computer;
                return () -> a.setDoubleValue(c.getAsDouble());
            };
        }

        @Override
        public BiFunction<WriteAccess, Computer, Runnable> visit(final FloatDataSpec spec) {
            return (access, computer) -> {
                var a = (FloatAccess.FloatWriteAccess)access;
                var c = (FloatComputer)computer;
                return () -> a.setFloatValue(c.getAsFloat());
            };
        }

        @Override
        public BiFunction<WriteAccess, Computer, Runnable> visit(final IntDataSpec spec) {
            return (access, computer) -> {
                var a = (IntAccess.IntWriteAccess)access;
                var c = (IntComputer)computer;
                return () -> a.setIntValue(c.getAsInt());
            };
        }

        @Override
        public BiFunction<WriteAccess, Computer, Runnable> visit(final LongDataSpec spec) {
            return (access, computer) -> {
                var a = (LongAccess.LongWriteAccess)access;
                var c = (LongComputer)computer;
                return () -> a.setLongValue(c.getAsLong());
            };
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

        @Override
        public BiFunction<WriteAccess, Computer, Runnable> visit(final StringDataSpec spec) {
            throw new IllegalArgumentException("TODO not implemented"); // TODO
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
     *             result column is incompatible with the {@link Ast.Node#inferredType() inferred type} of the
     *             expression.
     */
    static MapperFactory createMapperFactory( //
        final Ast.Node ast, //
        final IntFunction<Function<ReadAccess[], ? extends Computer>> columnIndexToComputerFactory, //
        final DataSpecs.DataSpecWithTraits outputSpec) //
    {
        final AstType astType = ast.inferredType();
        final AstType colType = outputSpec.spec().accept(Typing.toAstType);
        if (!isAssignableTo(astType, colType)) {
            throw new IllegalArgumentException("Expression of type \"" + astType
                + "\" cannot be assigned to column of type \"" + outputSpec.spec() + "\"");
        }

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
        final Ast.Node ast, //
        final IntFunction<Function<ReadAccess[], ? extends Computer>> columnIndexToComputerFactory) //
    {
        final Function<ReadAccess[], Computer> computerFactory =
            createComputerFactory(ast, columnIndexToComputerFactory);
        return inputs -> (BooleanComputer)computerFactory.apply(inputs);
    }

    /**
     * TODO javadoc
     *
     * @param ast
     * @param columnIndexToComputerFactory
     * @return
     */
    static Function<ReadAccess[], Computer> createComputerFactory( //
        final Ast.Node ast, //
        final IntFunction<Function<ReadAccess[], ? extends Computer>> columnIndexToComputerFactory) //
    {
        final List<Ast.Node> postorder = Ast.postorder(ast);
        return readAccesses -> {
            // TODO: Possible optimization here would be to use node indices and Computer[] instead of a Map.
            //       (For this, add an index field to Ast.Node and set according to postorder.)
            final Map<Ast.Node, Computer> computers = new HashMap<>();
            for (var node : postorder) {
                if (node instanceof Ast.IntConstant c) {
                    var computer = switch (node.inferredType()) {
                        case BYTE -> (ByteComputer)() -> (byte)c.value();
                        case INT -> (IntComputer)() -> (int)c.value();
                        case LONG -> (LongComputer)() -> c.value();
                        default -> throw new IllegalStateException("Unexpected inferred type: " + node.inferredType());
                    };
                    computers.put(node, computer);
                } else if (node instanceof Ast.FloatConstant c) {
                    computers.put(node, (DoubleComputer)() -> c.value());
                } else if (node instanceof Ast.StringConstant) {
                    throw new UnsupportedOperationException("TODO: not implemented"); // TODO
                } else if (node instanceof Ast.ColumnRef) {
                    throw new UnsupportedOperationException("TODO: cannot handle named columns yet."); // TODO
                } else if (node instanceof Ast.ColumnIndex n) {
                    var computer = columnIndexToComputerFactory.apply(n.columnIndex()).apply(readAccesses);
                    computers.put(node, computer);
                } else if (node instanceof Ast.BinaryOp n) {
                    var computer = binary(n, computers::get);
                    computers.put(node, computer);
                } else if (node instanceof Ast.UnaryOp n) {
                    var computer = unary(n.inferredType(), n.op(), computers.get(n.arg()));
                    computers.put(node, computer);
                }
            }
            return computers.get(ast);
        };
    }

    static boolean isAssignableTo(final AstType src, final AstType dest) {
        return switch (src) {
            case BYTE -> switch (dest) {
                case BYTE, INT, LONG, FLOAT, DOUBLE, STRING -> true;
                case BOOLEAN -> false;
            };
            case INT -> switch (dest) {
                case INT, LONG, FLOAT, DOUBLE, STRING -> true;
                case BYTE, BOOLEAN -> false;
            };
            case LONG -> switch (dest) {
                case LONG, FLOAT, DOUBLE, STRING -> true;
                case BYTE, INT, BOOLEAN -> false;
            };
            case FLOAT -> switch (dest) {
                case FLOAT, DOUBLE, STRING -> true;
                case BYTE, INT, LONG, BOOLEAN -> false;
            };
            case DOUBLE -> switch (dest) {
                case DOUBLE, STRING -> true;
                case BYTE, INT, LONG, FLOAT, BOOLEAN -> false;
            };
            case BOOLEAN -> switch (dest) {
                case BOOLEAN, STRING -> true;
                case BYTE, INT, LONG, FLOAT, DOUBLE -> false;
            };
            case STRING -> switch (dest) {
                case STRING -> true;
                case BYTE, INT, LONG, FLOAT, DOUBLE, BOOLEAN -> false;
            };
        };
    }
}
