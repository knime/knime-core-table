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
     * Infer types of the given {@code Ast.Node}s.
     * Evaluate constant sub-expressions and replace them by single {@code Ast.Node} constants.
     * <p>
     * Types are determined as follows:
     * <ul>
     *   <li>Column references have the {@code AstType} corresponding to the {@code DataSpec} of the respective column.</li>
     *   <li>Integer literals have the narrowest integral {@code AstType} type that can fit the respective value.</li>
     *   <li>Floating point literals are {@code FLOAT} if the last char is "f" or "F", and {@code DOUBLE} otherwise.</li>
     *   <li>Type of unary and binary expression are determined as described in the <a href="https://docs.oracle.com/javase/specs/jls/se17/html/jls-5.html#jls-5.6">Java Language Specification</a>:
     *       <ol>
     *         <li>If either operand is of type double, the other is converted to double.</li>
     *         <li>Otherwise, if either operand is of type float, the other is converted to float.</li>
     *         <li>Otherwise, if either operand is of type long, the other is converted to long.</li>
     *         <li>Otherwise, both operands are converted to type int.</li>
     *       </ol>
     *       Constant sub-expressions are evaluated and replaced by single {@code Ast.Node}.
     *       The type of evaluated integer sub-expressions is the narrowest integral {@code AstType} type that can fit the respective value.
     *       </li>
     * </ul>
     *
     * @param postorder  AST nodes sorted for post-order traversal (so that types of children are known before parent type is determined)
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
                throw new UnsupportedOperationException("TODO: cannot handle named columns yet."); // TODO
            } else if (node instanceof Ast.ColumnIndex n) {
                var type = columnType.apply(n.columnIndex());
                node.setInferredType(type);
            } else if (node instanceof Ast.BinaryOp n) {
                final AstType t1 = n.arg1().inferredType();
                final AstType t2 = n.arg2().inferredType();

                // string concatenation?
                if (n.op() == PLUS && (t1 == AstType.STRING || t2 == AstType.STRING)) {
                    node.setInferredType(AstType.STRING);
                }

                // arithmetic operation?
                else if (t1.isNumeric() && t2.isNumeric() && n.op().isArithmetic() ) {
                    if (n.arg1().isConstant() && n.arg2().isConstant()) {
                        final Ast.Node result = evaluateConstExpr(n);
                        n.replaceWith(result);
                        replacement = result;
                    } else {
                        node.setInferredType(promotedNumericType(t1, t2));
                    }
                }

                // logical operation?
                else if (t1 ==AstType.BOOLEAN && t2 == AstType.BOOLEAN && n.op().isLogical() ) {
                    // TODO: Evaluate const expression
                    node.setInferredType(AstType.BOOLEAN);
                }

                // ordering comparison
                else if (n.op().isOrderingComparison()) {
                    if ( orderingType( t1, t2 ) == null ) {
                        throw new IllegalArgumentException("types " + t1 + " and " + t2 + " cannot be order-compared");
                    }
                    // TODO: Evaluate const expression
                    node.setInferredType(AstType.BOOLEAN);
                }

                // equality comparison
                else if (n.op().isEqualityComparison()) {
                    if ( equalityType( t1, t2 ) == null ) {
                        throw new IllegalArgumentException("types " + t1 + " and " + t2 + " cannot be equality-compared");
                    }
                    // TODO: Evaluate const expression
                    node.setInferredType(AstType.BOOLEAN);
                } else {
                    throw new IllegalArgumentException("binary expression of unknown type.");
                }

            } else if (node instanceof Ast.UnaryOp n) {
                final AstType t1 = n.arg().inferredType();

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
                    throw new IllegalArgumentException("unary expression of unknown type.");
                }
            }
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
                // TODO
                case EQUAL_TO -> 0.0;
                case NOT_EQUAL_TO -> 0.0;
                case LESS_THAN -> 0.0;
                case LESS_THAN_EQUAL -> 0.0;
                case GREATER_THAN -> 0.0;
                case GREATER_THAN_EQUAL -> 0.0;
                case CONDITIONAL_AND -> 0.0;
                case CONDITIONAL_OR -> 0.0;
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
                // TODO
                case EQUAL_TO -> 0L;
                case NOT_EQUAL_TO -> 0L;
                case LESS_THAN -> 0L;
                case LESS_THAN_EQUAL -> 0L;
                case GREATER_THAN -> 0L;
                case GREATER_THAN_EQUAL -> 0L;
                case CONDITIONAL_AND -> 0L;
                case CONDITIONAL_OR -> 0L;
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
                // TODO
                case NOT -> 0.0;
            };
            Ast.Node result = new Ast.FloatConstant(value);
            result.setInferredType(promotedNumericType(t1));
            return result;
        } else {
            long v1 = longConstValue(arg1);
            long value = switch (node.op()) {
                case MINUS -> -v1;
                // TODO
                case NOT -> 0L;
            };
            Ast.Node result = new Ast.FloatConstant(value);
            result.setInferredType(narrowestType(value));
            return result;
        }
    }

    /**
     * Get the {@code long} value of {@code node}.
     *
     * @throws IllegalArgumentException if {@code node} does not represent an integer numeric constant.
     */
    private static long longConstValue(Ast.Node node) {
        if (node instanceof Ast.IntConstant c) {
            return c.value();
        } else {
            throw new IllegalArgumentException();
        }
    }

    /**
     * Get the {@code double} value of {@code node}.
     *
     * @throws IllegalArgumentException if {@code node} does not represent a numeric constant.
     */
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

    /**
     * Equality Comparison can be applied if both arguments are numeric, or if both arguments have the same type.
     *
     * @return the type that should be used for comparison, or {@code null} if the argument types are not comparable.
     */
    static AstType equalityType(AstType t1, AstType t2) {
        if (t1 == t2) {
            return t1;
        } else {
            return orderingType(t1, t2);
        }
    }

    /**
     * Ordering Comparison can be applied if both arguments are numeric.
     *
     * @return the type that should be used for comparison, or {@code null} if the argument types are not comparable.
     */
    static AstType orderingType(AstType t1, AstType t2) {
        if (!t1.isNumeric() || !t2.isNumeric()) {
            return null;
        }
        return promotedNumericType(t1, t2);
    }

    // see JLS 5.6
    // https://docs.oracle.com/javase/specs/jls/se17/html/jls-5.html#jls-5.6
    //
    //        - If either operand is of type double, the other is converted to double.
    //        - Otherwise, if either operand is of type float, the other is converted to float.
    //        - Otherwise, if either operand is of type long, the other is converted to long.
    //        - Otherwise, both operands are converted to type int.
    static AstType promotedNumericType(AstType t1, AstType t2) {
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

    /**
     * A visitor that maps {@code DataSpec} to the corresponding {@code AstType}.
     */
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
            throw new IllegalArgumentException("TODO: How to handle VarBinaryDataSpec in expressions?"); // TODO
        }

        @Override
        public AstType visit(VoidDataSpec spec) {
            throw new IllegalArgumentException("TODO: How to handle VoidDataSpec in expressions?"); // TODO
        }

        @Override
        public AstType visit(StructDataSpec spec) {
            throw new IllegalArgumentException("TODO: How to handle StructDataSpec in expressions?"); // TODO
        }

        @Override
        public AstType visit(ListDataSpec listDataSpec) {
            throw new IllegalArgumentException("TODO: How to handle ListDataSpec in expressions?"); // TODO
        }

        @Override
        public AstType visit(StringDataSpec spec) {
            return AstType.STRING;
        }
    };
}
