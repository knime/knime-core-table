package org.knime.core.table.virtual.expression;

import static org.knime.core.table.schema.DataSpecs.DOUBLE;
import static org.knime.core.table.virtual.expression.Ast.BinaryOp.Operator.PLUS;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
import org.knime.core.table.schema.traits.DefaultDataTraits;
import org.knime.core.table.virtual.VirtualTable;
import org.knime.core.table.virtual.expression.ExpressionGrammar.Expr;
import org.knime.core.table.virtual.spec.MapTransformSpec;
import org.knime.core.table.virtual.spec.MapTransformSpec.MapperFactory;
import org.rekex.parser.ParseResult;
import org.rekex.parser.ParseResult.Full;
import org.rekex.parser.PegParser;

public class VT {

    public static VirtualTable map(final VirtualTable table, final String expression, final DataSpecWithTraits outputSpec) {
//        final TableTransformSpec transformSpec = new MapTransformSpec(columnIndices, mapperFactory);
//        return new VirtualTable(new TableTransform(List.of(m_transform), transformSpec), mapperFactory.getOutputSchema());

        final PegParser<Expr> parser = ExpressionGrammar.parser();
        final ParseResult<Expr> result = parser.parse(expression);
        if (result instanceof Full<Expr> full) {
            final Ast.Node ast = full.value().ast();
            System.out.println("ast = " + ast);

            final List<Ast.Node> postorder = postorder(ast);
            final Columns columns = getColumns(postorder);
            System.out.println("columns = " + columns);

            final ColumnarSchema schema = table.getSchema();
            final IntFunction<AstType> columnType = columnIndex -> schema.getSpec(columnIndex).accept(toAstType);
            final Function<Ast.Node, AstType> nodeType = getTypes(postorder, columnType);

            for (int columnIndex : columns.columnIndices()) {
                DataSpec spec = schema.getSpec(columnIndex);
                AstType astType = spec.accept(toAstType);
                System.out.println("columnIndex = " + columnIndex + ", spec = " + spec + ", astType = " + astType);
            }

            final IntFunction<Function<ReadAccess[], ? extends MapBuilder.Exec.Computer>> columnIndexToComputerFactory =
                    columnIndex -> {
                        int inputIndex = columns.getInputIndex(columnIndex);
                        Function<ReadAccess, ? extends MapBuilder.Exec.Computer> createComputer =
                                schema.getSpec(columnIndex).accept(MapBuilder.Exec.toReaderFactory);
                        return readAccesses -> createComputer.apply(readAccesses[inputIndex]);
                    };

            final MapperFactory mapperFactory = new MapTransformSpec.DefaultMapperFactory(ColumnarSchema.of(outputSpec),
                    new MapBuilder.MyCreateMapper(ast, nodeType, columnIndexToComputerFactory, outputSpec.spec()));
            return table.map(columns.columnIndices(), mapperFactory);

        } else {
            System.err.println(result);
        }

        return null;
    }

    public static VirtualTable map(final VirtualTable table, final String expression, final DataSpec outputSpec) {
        return map(table, expression, new DataSpecWithTraits(outputSpec, DefaultDataTraits.EMPTY));
    }


    record Columns(int[] columnIndices) {
        int getColumnIndex(int inputIndex) {
            return columnIndices[inputIndex];
        }

        int getInputIndex(int columnIndex) {
            for (int i = 0; i < columnIndices.length; i++) {
                if ( columnIndices[i] == columnIndex )
                    return i;
            }
            throw new IndexOutOfBoundsException();
        }

        @Override
        public String toString() {
            return "Columns" + Arrays.toString(columnIndices);
        }
    }


    static class MapBuilder
    {
        // TODO: remove Exec wrapper interface
        public interface Exec {

            interface Computer {
            }

            interface DoubleComputer extends Computer, DoubleSupplier {
                static DoubleComputer column(ReadAccess access)
                {
                    var a = (DoubleAccess.DoubleReadAccess)access;
                    return () -> a.getDoubleValue();
                }

                static DoubleComputer binary(Ast.BinaryOp.Operator op, DoubleComputer arg1, DoubleComputer arg2)
                {
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
            }

            // create Computer for BinaryOp
            static Computer binary(AstType type, Ast.BinaryOp.Operator op, Computer arg1, Computer arg2)
            {
                return switch (type)
                {
                    case BYTE, INT, LONG, FLOAT, BOOLEAN, STRING -> throw new UnsupportedOperationException("TODO: not implemented");
                    case DOUBLE -> DoubleComputer.binary(op, (DoubleComputer)arg1, (DoubleComputer)arg2);
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
                    throw new IllegalArgumentException("TODO not implemented");
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
        }

        static class MyCreateMapper implements BiFunction<ReadAccess[], WriteAccess[], Runnable>
        {
            private final Ast.Node ast;
            private final List<Ast.Node> postorder;
            private final Function<Ast.Node, AstType> nodeType;
            private final IntFunction<Function<ReadAccess[], ? extends MapBuilder.Exec.Computer>> columnIndexToComputerFactory;
            private final DataSpec outputSpec;

            MyCreateMapper(
                    final Ast.Node ast,
                    final Function<Ast.Node, AstType> nodeType,
                    final IntFunction<Function<ReadAccess[], ? extends MapBuilder.Exec.Computer>> columnIndexToComputerFactory,
                    final DataSpec outputSpec)
            {
                this.ast = ast;
                this.postorder = postorder(ast);
                this.nodeType = nodeType;
                this.columnIndexToComputerFactory = columnIndexToComputerFactory;
                this.outputSpec = outputSpec;
            }

            @Override
            public Runnable apply(ReadAccess[] readAccesses, WriteAccess[] writeAccesses) {

                final Map<Ast.Node, Exec.Computer> computers = new HashMap<>();
                for (var node : postorder) {
                    if (node instanceof Ast.IntConstant c) {
                        computers.put(node, (Exec.IntComputer)() -> (int)c.value());
                    // TODO AstDoubleConst
                    } else if (node instanceof Ast.StringConstant) {
                        throw new UnsupportedOperationException("TODO: not implemented");
                    } else if (node instanceof Ast.ColumnRef) {
                        throw new UnsupportedOperationException("TODO: cannot handle named columns yet.");
                    } else if (node instanceof Ast.ColumnIndex n) {
                        var computer = columnIndexToComputerFactory.apply(n.columnIndex()).apply(readAccesses);
                        computers.put(node, computer);
                    } else if (node instanceof Ast.BinaryOp n) {
                        var computer = Exec.binary(nodeType.apply(n), n.op(), computers.get(n.arg1()), computers.get(n.arg2()));
                        computers.put(node, computer);
                    } else if ( node instanceof Ast.UnaryOp n) {
                        throw new UnsupportedOperationException("TODO: not implemented");
                    }
                }

                // expression always has single output (currently)
                var output = writeAccesses[0];
                return outputSpec.accept(Exec.toWriterFactory).apply(output, computers.get(ast));
            }
        }
    }


    public static List<Ast.Node> postorder(final Ast.Node root)
    {
        var nodes = new ArrayDeque<Ast.Node>();
        var visited = new ArrayList<Ast.Node>();
        for (var node = root; node != null; node = nodes.poll()) {
            visited.add(node);
            node.children().forEach(nodes::push);
        }
        Collections.reverse(visited);
        return visited;
    }

    /**
     * Determine the input columns occurring in the given {@code Ast.Node}s.
     * Compute a map from column index (inputs to the table, that is
     * AstColumnIndex.columnIndex()) to input index of the {@link
     * MapperFactory#createMapper mapper} function. For example, if an
     * expression uses (only) "$[2]" and "$[5]" then these would map to input
     * indices 0 and 1, respectively.
     *
     * @param nodes
     * @return a map from column index to mapper input index.
     */
    private static Columns getColumns(final List<Ast.Node> nodes) {
        int[] columnIndices = nodes.stream()
                .mapToInt(node -> {
                    if (node instanceof Ast.ColumnIndex n) {
                        return n.columnIndex();
                    } else {
                        return -1;
                    }
                })
                .filter(i -> i != -1)
                .distinct()
                .toArray();
        return new Columns(columnIndices);
    }

    /**
     * infer Ast.Node types
     * TODO constant propagation
     * TODO insert explicit cast Ast.Nodes ???
     *
     * @param postorder AST nodes sorted for post-order traversal
     * @param columnType map from column index (in input table, 0-based) to type
     * @return map AST nodes to their type
     */
    private static Function<Ast.Node, AstType> getTypes(List<Ast.Node> postorder, IntFunction<AstType> columnType) {
        final Map<Ast.Node, AstType> types = new HashMap<>();
        for (var node : postorder) {
            if (node instanceof Ast.IntConstant c) {
                var type = narrowestType(c.value());
                node.setInferredType(type);
                types.put(node, type);
            // AstFloatConst
            // AstDoubleConst
            } else if (node instanceof Ast.StringConstant) {
                node.setInferredType(AstType.STRING);
                types.put(node, AstType.STRING);
            } else if (node instanceof Ast.ColumnRef) {
                throw new UnsupportedOperationException("TODO: cannot handle named columns yet.");
            } else if (node instanceof Ast.ColumnIndex n) {
                var type  = columnType.apply(n.columnIndex());
                node.setInferredType(type);
                types.put(node, type);
            } else if (node instanceof Ast.BinaryOp n) {
                final AstType t1 = n.arg1().inferredType();
                final AstType t2 = n.arg2().inferredType();

                // string concatenation?
                if (n.op() == PLUS && (t1 == AstType.STRING || t2 == AstType.STRING)) {
                    node.setInferredType(AstType.STRING);
                    types.put(node, AstType.STRING);
                }

                // numeric operation?
                else if (t1.isNumeric() && t2.isNumeric()) {
                    var type = promotedNumericType(t1, t2);
                    node.setInferredType(type);
                    types.put(node, type);

                    // TODO: if both arguments are constant:
                    //       - do the computation immediately.
                    //       - replace this BinaryOp by a Float/Double/IntConstant
                    //         Either by re-linking arg of the parent node,
                    //         or by attaching a const value to this node and marking it as const.
                    //         The latter may be better for maintaining the original parse info?
                    //         But the former would be better in that we don't keep stuff that we don't want to iterate over again.
                }

                else {
                    throw new IllegalArgumentException("binary expression of unknown type.");
                }

                System.out.println("op " + n.op());
                System.out.println("  t1 = " + t1);
                System.out.println("  t2 = " + t2);
            } else if ( node instanceof Ast.UnaryOp n) {

            }
            System.out.println("node = " + node + ", type = " + types.get(node));
        }
        return types::get;
    }

    /**
     * Find the narrowest {@link AstType} that can represent the given {@code value}.
     *
     * @return {@link AstType#BYTE BYTE}, {@link AstType#INT INT}, or {@link AstType#LONG LONG}
     */
    private static AstType narrowestType(final long value) {
        if ( value >= Byte.MIN_VALUE && value <= Byte.MAX_VALUE ) {
            return AstType.BYTE;
        } else if ( value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE ) {
            return AstType.INT;
        } else {
            return AstType.LONG;
        }
    }

    // see JLS 5.6
    // https://docs.oracle.com/javase/specs/jls/se17/html/jls-5.html#jls-5.6
    //
    //        - If either operand is of type double, the other is converted to double.
    //        - Otherwise, if either operand is of type float, the other is converted to float.
    //        - Otherwise, if either operand is of type long, the other is converted to long.
    //        - Otherwise, both operands are converted to type int.
    //
    // TODO: narrowing conversion (short, byte) requires evaluation of constant
    //       expressions and const promotion.
    //
    // TODO: move method to AstType enum?
    private static AstType promotedNumericType(AstType t1, AstType t2) {
        if (!t1.isNumeric() || !t2.isNumeric()) {
            throw new IllegalArgumentException();
        }
        if (t1 == AstType.DOUBLE || t2 == AstType.DOUBLE ) {
            return AstType.DOUBLE;
        } else if (t1 == AstType.FLOAT || t2 == AstType.FLOAT ) {
            return AstType.FLOAT;
        } else if (t1 == AstType.LONG || t2 == AstType.LONG ) {
            return AstType.LONG;
        } else {
            return AstType.INT;
        }
    }

    private static final DataSpec.Mapper<AstType> toAstType = new DataSpec.Mapper<>() {
        @Override
        public AstType visit(BooleanDataSpec spec) {
            return AstType.BOOLEAN;
        }

        @Override
        public AstType visit(ByteDataSpec spec) {
            return AstType.BYTE;
        }

        @Override
        public AstType visit(DoubleDataSpec spec) {
            return AstType.DOUBLE;
        }

        @Override
        public AstType visit(FloatDataSpec spec) {
            return AstType.FLOAT;
        }

        @Override
        public AstType visit(IntDataSpec spec) {
            return AstType.INT;
        }

        @Override
        public AstType visit(LongDataSpec spec) {
            return AstType.LONG;
        }

        @Override
        public AstType visit(VarBinaryDataSpec spec) {
            throw new IllegalArgumentException("TODO: How to handle VarBinaryDataSpec in expressions?");
        }

        @Override
        public AstType visit(VoidDataSpec spec) {
            throw new IllegalArgumentException("TODO: How to handle VoidDataSpec in expressions?");
        }

        @Override
        public AstType visit(StructDataSpec spec) {
            throw new IllegalArgumentException("TODO: How to handle StructDataSpec in expressions?");
        }

        @Override
        public AstType visit(ListDataSpec listDataSpec) {
            throw new IllegalArgumentException("TODO: How to handle ListDataSpec in expressions?");
        }

        @Override
        public AstType visit(StringDataSpec spec) {
            return AstType.STRING;
        }
    };
}
