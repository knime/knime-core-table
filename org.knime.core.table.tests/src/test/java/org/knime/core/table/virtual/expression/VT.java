package org.knime.core.table.virtual.expression;

import static org.knime.core.table.virtual.expression.Ast.BinaryOp.Operator.PLUS;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.IntFunction;

import org.knime.core.table.access.ReadAccess;
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
import org.knime.core.table.virtual.expression.Exec.Computer;
import org.rekex.parser.ParseResult;
import org.rekex.parser.ParseResult.Full;
import org.rekex.parser.PegParser;

public class VT {

    /**
     * TODO This should become a non-static method in VirtualTable, with signature
     *      {@code     public VirtualTable map(final String expression, final MapperFactory mapperFactory)    }
     *      similar to the existing
     *      {@code     public VirtualTable map(final int[] columnIndices, final MapperFactory mapperFactory)  }
     *
     *
     * @param table input table
     * @param expression expression that computes a new column
     * @param outputSpec DataSpec of the computed column. Must be assignable from the result type of the expression
     * @return
     */
    public static VirtualTable map(final VirtualTable table, final String expression, final DataSpecWithTraits outputSpec) {
        final PegParser<Expr> parser = ExpressionGrammar.parser();
        final ParseResult<Expr> result = parser.parse(expression);
        if (result instanceof Full<Expr> full) {
            final Ast.Node ast = full.value().ast();
            final List<Ast.Node> postorder = Ast.postorder(ast);
            final Ast.RequiredColumns columns = Ast.getRequiredColumns(postorder);

//            System.out.println("ast = " + ast);
//            System.out.println("columns = " + columns);

            final ColumnarSchema schema = table.getSchema();
            final IntFunction<AstType> columnIndexToAstType = columnIndex -> schema.getSpec(columnIndex).accept(toAstType);
            final Function<Ast.Node, AstType> nodeToAstType = getTypes(postorder, columnIndexToAstType);

//            for (int columnIndex : columns.columnIndices()) {
//                DataSpec spec = schema.getSpec(columnIndex);
//                AstType astType = spec.accept(toAstType);
//                System.out.println("columnIndex = " + columnIndex + ", spec = " + spec + ", astType = " + astType);
//            }

            final IntFunction<Function<ReadAccess[], ? extends Computer>> columnIndexToComputerFactory =
                    columnIndex -> {
                        int inputIndex = columns.getInputIndex(columnIndex);
                        Function<ReadAccess, ? extends Computer> createComputer =
                                schema.getSpec(columnIndex).accept(Exec.toReaderFactory);
                        return readAccesses -> createComputer.apply(readAccesses[inputIndex]);
                    };

            var mapperFactory = Exec.createMapperFactory(ast, nodeToAstType, columnIndexToComputerFactory, outputSpec);
            return table.map(columns.columnIndices(), mapperFactory);

        } else {
            System.err.println(result);
        }

        return null;
    }

    public static VirtualTable map(final VirtualTable table, final String expression, final DataSpec outputSpec) {
        return map(table, expression, new DataSpecWithTraits(outputSpec, DefaultDataTraits.EMPTY));
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
                final AstType t1 = n.arg().inferredType();

                // numeric operation?
                if (t1.isNumeric()) {
                    node.setInferredType(t1);
                    types.put(node, t1);

                    // TODO: if argument is constant:
                    //       - do the computation immediately.
                    //       - replace this UnaryOp by a Float/Double/IntConstant
                }

                else {
                    throw new IllegalArgumentException("binary expression of unknown type.");
                }

                System.out.println("op " + n.op());
                System.out.println("  t1 = " + t1);
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
