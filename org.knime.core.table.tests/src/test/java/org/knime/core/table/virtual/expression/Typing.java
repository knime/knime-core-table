package org.knime.core.table.virtual.expression;

import static org.knime.core.table.virtual.expression.Ast.BinaryOp.Operator.PLUS;

import java.util.List;
import java.util.function.Function;
import java.util.function.IntFunction;

import org.knime.core.table.schema.BooleanDataSpec;
import org.knime.core.table.schema.ByteDataSpec;
import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.schema.DoubleDataSpec;
import org.knime.core.table.schema.FloatDataSpec;
import org.knime.core.table.schema.IntDataSpec;
import org.knime.core.table.schema.ListDataSpec;
import org.knime.core.table.schema.LongDataSpec;
import org.knime.core.table.schema.StringDataSpec;
import org.knime.core.table.schema.StructDataSpec;
import org.knime.core.table.schema.VarBinaryDataSpec;
import org.knime.core.table.schema.VoidDataSpec;

/**
 * Type inference and constant propagation on {@code Ast.Node}.
 */
public interface Typing {

    /**
     * infer Ast.Node types
     * TODO constant propagation
     * TODO insert "explicit cast" Ast.Nodes ???
     *
     * @param postorder  AST nodes sorted for post-order traversal
     * @param columnType map from column index (in input table, 0-based) to type
     */
    static Function<Ast.Node, AstType> inferTypes(List<Ast.Node> postorder, IntFunction<AstType> columnType) {
        for (var node : postorder) {
            Ast.Node replacement = null;
            if (node instanceof Ast.IntConstant c) {
                node.setInferredType(narrowestType(c.value()));
            } else if (node instanceof Ast.FloatConstant c) {
                // Do nothing. Type DOUBLE or FLOAT was already assigned during parsing
            } else if (node instanceof Ast.StringConstant) {
                node.setInferredType(AstType.STRING);
            } else if (node instanceof Ast.ColumnRef) {
                throw new UnsupportedOperationException("TODO: cannot handle named columns yet.");
            } else if (node instanceof Ast.ColumnIndex n) {
                var type = columnType.apply(n.columnIndex());
                node.setInferredType(type);
            } else if (node instanceof Ast.BinaryOp n) {
                final AstType t1 = n.arg1().inferredType();
                final AstType t2 = n.arg2().inferredType();
                System.out.println("op " + n.op() + " (" + t1 + ", " + t2 + ")");

                // string concatenation?
                if (n.op() == PLUS && (t1 == AstType.STRING || t2 == AstType.STRING)) {
                    node.setInferredType(AstType.STRING);
                }

                // numeric operation?
                else if (t1.isNumeric() && t2.isNumeric()) {
                    if (n.arg1().isConstant() && n.arg2().isConstant()) {
                        final Ast.Node result = evaluateConstExpr(n);
                        n.replaceWith(result);
                        replacement = result;
                    } else {
                        node.setInferredType(promotedNumericType(t1, t2));
                    }
                } else {
                    throw new IllegalArgumentException("binary expression of unknown type.");
                }

            } else if (node instanceof Ast.UnaryOp n) {
                final AstType t1 = n.arg().inferredType();
                System.out.println("op " + n.op() + " (" + t1 + ")");

                // numeric operation?
                if (t1.isNumeric()) {
                    if (n.arg().isConstant()) {
                        final Ast.Node result = evaluateConstExpr(n);
                        n.replaceWith(result);
                        replacement = result;
                    } else {
                        node.setInferredType(promotedNumericType(t1));
                    }
                } else {
                    throw new IllegalArgumentException("binary expression of unknown type.");
                }
            }
            System.out.println("node = " + node + ", type = " + node.inferredType());
            if (replacement != null)
                System.out.println("   ==> " + replacement + ", type = " + replacement.inferredType());
        }
        return Ast.Node::inferredType;
    }

    // returns a new constant Ast.Node to replace {@code node}.
    private static Ast.Node evaluateConstExpr(Ast.BinaryOp node) {
        Ast.Node arg1 = node.arg1();
        Ast.Node arg2 = node.arg2();
        AstType t1 = arg1.inferredType();
        AstType t2 = arg2.inferredType();

        if (t1 == AstType.DOUBLE || t2 == AstType.DOUBLE || t1 == AstType.FLOAT || t2 == AstType.FLOAT) {

            // At least one of the operands is a floating point value.
            // If one of the operands is DOUBLE, then the result is DOUBLE.
            // Otherwise, the result is FLOAT.

            double v1 = floatConstValue(arg1);
            double v2 = floatConstValue(arg2);
            double value = switch (node.op()) {
                case PLUS -> v1 + v2;
                case MINUS -> v1 - v2;
                case MULTIPLY -> v1 * v2;
                case DIVIDE -> v1 / v2;
                case REMAINDER -> v1 % v2;
            };
            Ast.Node result = new Ast.FloatConstant(value);
            result.setInferredType(promotedNumericType(t1, t2));
            return result;
        } else {

            // Both operands are integer values.
            // The result is BYTE, INT, or LONG, depending on the value.
            // (The narrowest type that can represent the value is chosen

            long v1 = longConstValue(arg1);
            long v2 = longConstValue(arg2);
            long value = switch (node.op()) {
                case PLUS -> v1 + v2;
                case MINUS -> v1 - v2;
                case MULTIPLY -> v1 * v2;
                case DIVIDE -> v1 / v2;
                case REMAINDER -> v1 % v2;
            };
            Ast.Node result = new Ast.IntConstant(value);
            result.setInferredType(narrowestType(value));
            return result;
        }
    }

    // returns a new constant Ast.Node to replace {@code node}.
    private static Ast.Node evaluateConstExpr(Ast.UnaryOp node) {
        Ast.Node arg1 = node.arg();
        AstType t1 = arg1.inferredType();
        if (t1 == AstType.DOUBLE || t1 == AstType.FLOAT) {
            // operand is a floating point value.
            double v1 = floatConstValue(arg1);
            double value = switch (node.op()) {
                case MINUS -> -v1;
            };
            Ast.Node result = new Ast.FloatConstant(value);
            result.setInferredType(promotedNumericType(t1));
            return result;
        } else {
            long v1 = longConstValue(arg1);
            long value = switch (node.op()) {
                case MINUS -> -v1;
            };
            Ast.Node result = new Ast.FloatConstant(value);
            result.setInferredType(narrowestType(value));
            return result;
        }
    }

    private static long longConstValue(Ast.Node node) {
        if (node instanceof Ast.IntConstant c) {
            return c.value();
        } else {
            throw new IllegalArgumentException();
        }
    }

    private static double floatConstValue(Ast.Node node) {
        if (node instanceof Ast.IntConstant c) {
            return c.value();
        } else if (node instanceof Ast.FloatConstant c) {
            return c.value();
        } else {
            throw new IllegalArgumentException();
        }
    }

    /**
     * Find the narrowest {@link AstType} that can represent the given {@code value}.
     *
     * @return {@link AstType#BYTE BYTE}, {@link AstType#INT INT}, or {@link AstType#LONG LONG}
     */
    private static AstType narrowestType(final long value) {
        if (value >= Byte.MIN_VALUE && value <= Byte.MAX_VALUE) {
            return AstType.BYTE;
        } else if (value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE) {
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
    private static AstType promotedNumericType(AstType t1, AstType t2) {
        if (!t1.isNumeric() || !t2.isNumeric()) {
            throw new IllegalArgumentException();
        }
        if (t1 == AstType.DOUBLE || t2 == AstType.DOUBLE) {
            return AstType.DOUBLE;
        } else if (t1 == AstType.FLOAT || t2 == AstType.FLOAT) {
            return AstType.FLOAT;
        } else if (t1 == AstType.LONG || t2 == AstType.LONG) {
            return AstType.LONG;
        } else {
            return AstType.INT;
        }
    }

    private static AstType promotedNumericType(AstType t1) {
        if (!t1.isNumeric()) {
            throw new IllegalArgumentException();
        }
        if (t1 == AstType.BYTE) {
            return AstType.INT;
        } else {
            return t1;
        }
    }

    DataSpec.Mapper<AstType> toAstType = new DataSpec.Mapper<>() {
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
