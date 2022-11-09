package org.knime.core.table.virtual.expression;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.DoubleSupplier;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.IntSupplier;

import org.knime.core.table.access.DoubleAccess;
import org.knime.core.table.access.IntAccess;
import org.knime.core.table.access.ReadAccess;
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
import org.knime.core.table.virtual.spec.MapTransformSpec;

public interface Exec {

    interface Computer {
    }

    interface DoubleComputer extends Computer, DoubleSupplier {
        static DoubleComputer column(ReadAccess access) {
            var a = (DoubleAccess.DoubleReadAccess)access;
            return () -> a.getDoubleValue();
        }

        static DoubleComputer unary(Ast.UnaryOp.Operator op, DoubleComputer arg1) {
            return switch (op) {
                case MINUS -> () -> -arg1.getAsDouble();
            };
        }

        static DoubleComputer binary(Ast.BinaryOp.Operator op, DoubleComputer arg1, DoubleComputer arg2) {
            return switch (op) {
                case PLUS -> () -> arg1.getAsDouble() + arg2.getAsDouble();
                case MINUS -> () -> arg1.getAsDouble() - arg2.getAsDouble();
                case MULTIPLY -> () -> arg1.getAsDouble() * arg2.getAsDouble();
                case DIVIDE -> () -> arg1.getAsDouble() / arg2.getAsDouble();
                case REMAINDER -> () -> arg1.getAsDouble() % arg2.getAsDouble();
            };
        }
    }

    interface IntComputer extends DoubleComputer, IntSupplier {
        @Override
        default double getAsDouble() {
            return getAsInt();
        }

        static IntComputer unary(Ast.UnaryOp.Operator op, IntComputer arg1) {
            return switch (op) {
                case MINUS -> () -> -arg1.getAsInt();
            };
        }

        static IntComputer binary(Ast.BinaryOp.Operator op, IntComputer arg1, IntComputer arg2) {
            return switch (op) {
                case PLUS -> () -> arg1.getAsInt() + arg2.getAsInt();
                case MINUS -> () -> arg1.getAsInt() - arg2.getAsInt();
                case MULTIPLY -> () -> arg1.getAsInt() * arg2.getAsInt();
                case DIVIDE -> () -> arg1.getAsInt() / arg2.getAsInt();
                case REMAINDER -> () -> arg1.getAsInt() % arg2.getAsInt();
            };
        }
    }

    // create Computer for BinaryOp
    static Computer binary(AstType type, Ast.BinaryOp.Operator op, Computer arg1, Computer arg2) {
        return switch (type) {
            case BYTE, LONG, FLOAT, BOOLEAN, STRING ->
                    throw new UnsupportedOperationException("TODO: not implemented");
            case INT -> IntComputer.binary(op, (IntComputer)arg1, (IntComputer)arg2);
            case DOUBLE -> DoubleComputer.binary(op, (DoubleComputer)arg1, (DoubleComputer)arg2);
        };
    }

    // create Computer for UnaryOp
    static Computer unary(AstType type, Ast.UnaryOp.Operator op, Computer arg1) {
        return switch (type) {
            case BYTE, LONG, FLOAT, BOOLEAN, STRING ->
                    throw new UnsupportedOperationException("TODO: not implemented");
            case INT -> IntComputer.unary(op, (IntComputer)arg1);
            case DOUBLE -> DoubleComputer.unary(op, (DoubleComputer)arg1);
        };
    }

    // create Computer (of appropriate type) that reads from a ReadAccess
    DataSpec.Mapper<Function<ReadAccess, ? extends Computer>> toReaderFactory = new DataSpec.Mapper<>() {
        @Override
        public Function<ReadAccess, Computer> visit(BooleanDataSpec spec) {
            throw new IllegalArgumentException("TODO not implemented");
        }

        @Override
        public Function<ReadAccess, Computer> visit(ByteDataSpec spec) {
            throw new IllegalArgumentException("TODO not implemented");
        }

        @Override
        public Function<ReadAccess, DoubleComputer> visit(DoubleDataSpec spec) {
            return DoubleComputer::column;
        }

        @Override
        public Function<ReadAccess, Computer> visit(FloatDataSpec spec) {
            throw new IllegalArgumentException("TODO not implemented");
        }

        @Override
        public Function<ReadAccess, IntComputer> visit(IntDataSpec spec) {
            return access -> {
                var intAccess = (IntAccess.IntReadAccess)access;
                return () -> intAccess.getIntValue();
            };
        }

        @Override
        public Function<ReadAccess, Computer> visit(LongDataSpec spec) {
            throw new IllegalArgumentException("TODO not implemented");
        }

        @Override
        public Function<ReadAccess, Computer> visit(VarBinaryDataSpec spec) {
            throw new IllegalArgumentException("TODO: How to handle VarBinaryDataSpec in expressions?");
        }

        @Override
        public Function<ReadAccess, Computer> visit(VoidDataSpec spec) {
            throw new IllegalArgumentException("TODO: How to handle VoidDataSpec in expressions?");
        }

        @Override
        public Function<ReadAccess, Computer> visit(StructDataSpec spec) {
            throw new IllegalArgumentException("TODO: How to handle StructDataSpec in expressions?");
        }

        @Override
        public Function<ReadAccess, Computer> visit(ListDataSpec listDataSpec) {
            throw new IllegalArgumentException("TODO: How to handle ListDataSpec in expressions?");
        }

        @Override
        public Function<ReadAccess, Computer> visit(StringDataSpec spec) {
            throw new IllegalArgumentException("TODO not implemented");
        }
    };

    // create Runnable that writes value from Computer (of appropriate type) to WriteAccess (of appropriate type)
    DataSpec.Mapper<BiFunction<WriteAccess, Computer, Runnable>> toWriterFactory = new DataSpec.Mapper<>() {
        @Override
        public BiFunction<WriteAccess, Computer, Runnable> visit(BooleanDataSpec spec) {
            throw new IllegalArgumentException("TODO not implemented");
        }

        @Override
        public BiFunction<WriteAccess, Computer, Runnable> visit(ByteDataSpec spec) {
            throw new IllegalArgumentException("TODO not implemented");
        }

        @Override
        public BiFunction<WriteAccess, Computer, Runnable> visit(DoubleDataSpec spec) {
            return (access, computer) -> {
                var a = (DoubleAccess.DoubleWriteAccess)access;
                var c = (DoubleComputer)computer;
                return () -> a.setDoubleValue(c.getAsDouble());
            };
        }

        @Override
        public BiFunction<WriteAccess, Computer, Runnable> visit(FloatDataSpec spec) {
            throw new IllegalArgumentException("TODO not implemented");
        }

        @Override
        public BiFunction<WriteAccess, Computer, Runnable> visit(IntDataSpec spec) {
            return (access, computer) -> {
                var a = (IntAccess.IntWriteAccess)access;
                var c = (IntComputer)computer;
                return () -> a.setIntValue(c.getAsInt());
            };
        }

        @Override
        public BiFunction<WriteAccess, Computer, Runnable> visit(LongDataSpec spec) {
            throw new IllegalArgumentException("TODO not implemented");
        }

        @Override
        public BiFunction<WriteAccess, Computer, Runnable> visit(VarBinaryDataSpec spec) {
            throw new IllegalArgumentException("TODO: How to handle VarBinaryDataSpec in expressions?");
        }

        @Override
        public BiFunction<WriteAccess, Computer, Runnable> visit(VoidDataSpec spec) {
            throw new IllegalArgumentException("TODO: How to handle VoidDataSpec in expressions?");
        }

        @Override
        public BiFunction<WriteAccess, Computer, Runnable> visit(StructDataSpec spec) {
            throw new IllegalArgumentException("TODO: How to handle StructDataSpec in expressions?");
        }

        @Override
        public BiFunction<WriteAccess, Computer, Runnable> visit(ListDataSpec listDataSpec) {
            throw new IllegalArgumentException("TODO: How to handle ListDataSpec in expressions?");
        }

        @Override
        public BiFunction<WriteAccess, Computer, Runnable> visit(StringDataSpec spec) {
            throw new IllegalArgumentException("TODO not implemented");
        }
    };

    /**
     * Create a {@code MapTransformSpec.MapperFactory}, that is, the final
     * realization of the expression.
     *
     * @param ast                          root note of AST of the expression.
     * @param columnIndexToComputerFactory function that maps column index to a
     *                                     factory that produces {@code Computer} (of matching type) from the
     *                                     {@code ReadAccess[]} array given to the {@code MapperFactory}.
     * @param outputSpec                   spec of result column
     * @return a {@code MapperFactory} that implements the given expression.
     * @throws IllegalArgumentException TODO: Hrmm ... is there a better exception type?
     *                                  if the {@code DataSpec} of the result column is incompatible with
     *                                  the {@link Ast.Node#inferredType() inferred type} of the expression.
     */
    static MapTransformSpec.MapperFactory createMapperFactory( //
            final Ast.Node ast, //
            final IntFunction<Function<ReadAccess[], ? extends Computer>> columnIndexToComputerFactory, //
            final DataSpecs.DataSpecWithTraits outputSpec) //
    {
        final AstType astType = ast.inferredType();
        final AstType colType = outputSpec.spec().accept(Typing.toAstType);
        if (!isAssignableTo(astType, colType)) {
            throw new IllegalArgumentException(
                    "Expression of type \"" + astType + "\" cannot be assigned to column of type \"" + outputSpec.spec() + "\"");
        }

        final ColumnarSchema schema = ColumnarSchema.of(outputSpec);

        final List<Ast.Node> postorder = Ast.postorder(ast);
        final BiFunction<WriteAccess, Computer, Runnable> writerFactory = outputSpec.spec().accept(toWriterFactory);
        final BiFunction<ReadAccess[], WriteAccess[], Runnable> factory = (readAccesses, writeAccesses) -> {
            final Map<Ast.Node, Computer> computers = new HashMap<>();
            for (var node : postorder) {
                if (node instanceof Ast.IntConstant c) {
                    computers.put(node, (IntComputer)() -> (int)c.value()); // TODO respect c.inferredType()! The type might be BYTE or LONG, too.
                } else if (node instanceof Ast.FloatConstant c) {
                    computers.put(node, (DoubleComputer)() -> c.value());
                } else if (node instanceof Ast.StringConstant) {
                    throw new UnsupportedOperationException("TODO: not implemented");
                } else if (node instanceof Ast.ColumnRef) {
                    throw new UnsupportedOperationException("TODO: cannot handle named columns yet.");
                } else if (node instanceof Ast.ColumnIndex n) {
                    var computer = columnIndexToComputerFactory.apply(n.columnIndex()).apply(readAccesses);
                    computers.put(node, computer);
                } else if (node instanceof Ast.BinaryOp n) {
                    var computer =
                            binary(n.inferredType(), n.op(), computers.get(n.arg1()), computers.get(n.arg2()));
                    computers.put(node, computer);
                } else if (node instanceof Ast.UnaryOp n) {
                    var computer =
                            unary(n.inferredType(), n.op(), computers.get(n.arg()));
                    computers.put(node, computer);
                }
            }

            // expression always has single output (currently)
            return writerFactory.apply(writeAccesses[0], computers.get(ast));
        };

        return new MapTransformSpec.DefaultMapperFactory(schema, factory);
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
