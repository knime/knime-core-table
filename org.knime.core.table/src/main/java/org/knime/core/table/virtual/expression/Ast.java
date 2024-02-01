package org.knime.core.table.virtual.expression;

import static org.knime.core.table.virtual.expression.Ast.BinaryOp.OperatorType.ARITHMETIC;
import static org.knime.core.table.virtual.expression.Ast.BinaryOp.OperatorType.EQUALITY;
import static org.knime.core.table.virtual.expression.Ast.BinaryOp.OperatorType.LOGICAL;
import static org.knime.core.table.virtual.expression.Ast.BinaryOp.OperatorType.ORDERING;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.knime.core.table.virtual.spec.MapTransformSpec;

public interface Ast {

    abstract sealed class Node {
        public List<Node> children() {
            return List.of();
        }

        Node parent;

        public Node parent() {
            return parent;
        }

        void replaceChild(final Node child, final Node replacement) {
        }

        public void replaceWith(final Node node) {
            if (parent != null) {
                parent.replaceChild(this, node);
                parent = null;
            }
        }

        private AstType inferredType;

        public AstType inferredType() {
            return inferredType;
        }

        public void setInferredType(final AstType type) {
            this.inferredType = type;
        }

        public boolean isConstant() {
            return false;
        }
    }

    final class Call extends Node {
        private final String func;

        private final List<Node> args;

        public Call(final String func, final List<Node> args) {
            this.func = func;
            this.args = new ArrayList<>(args);
        }

        public int numArgs() {
            return args.size();
        }

        public Node arg(final int index) {
            return args.get(index);
        }

        @Override
        public List<Node> children() {
            return args;
        }

        @Override
        void replaceChild(final Node child, final Node replacement) {
            for (int i = 0; i < args.size(); i++) {
                if (args.get(i).equals(child)) {
                    args.set(i, replacement);
                    replacement.parent = this;
                }
            }
        }

        @Override
        public String toString() {
            final String arguments = args.stream().map(Node::toString).collect(Collectors.joining(", "));
            return "Call " + func + '(' + arguments + ')';
        }
    }

    final class IntConstant extends Node {
        private final long value;

        public IntConstant(final long value) {
            this.value = value;
        }

        public long value() {
            return value;
        }

        @Override
        public boolean isConstant() {
            return true;
        }

        @Override
        public String toString() {
            return "IntConstant[" + "value=" + value + ']';
        }
    }

    final class FloatConstant extends Node {
        private final double value;

        public FloatConstant(final double value) {
            this.value = value;
        }

        public double value() {
            return value;
        }

        @Override
        public boolean isConstant() {
            return true;
        }

        @Override
        public String toString() {
            return "FloatConstant[" + "value=" + value + ']';
        }
    }

    final class StringConstant extends Node {
        private final String value;

        public StringConstant(final String value) {
            this.value = value;
        }

        public String value() {
            return value;
        }

        @Override
        public boolean isConstant() {
            return true;
        }

        @Override
        public String toString() {
            return "StringConstant[" + "value=" + value + ']';
        }
    }

    final class ColumnRef extends Node {
        private final String name;

        public ColumnRef(final String name) {
            this.name = name;
        }

        public String name() {
            return name;
        }

        @Override
        public String toString() {
            return "ColumnRef[" + "name=" + name + ']';
        }
    }

    final class ColumnIndex extends Node {
        private final int columnIndex;

        public ColumnIndex(final int columnIndex) {
            this.columnIndex = columnIndex;
        }

        /**
         * Column index (0-based) in the input VirtualTable of the expression.
         *
         * @return column index
         */
        public int columnIndex() {
            return columnIndex;
        }

        @Override
        public String toString() {
            return "AstColumnIndex[" + "columnIndex=" + columnIndex + ']';
        }
    }

    final class BinaryOp extends Node {

        public enum OperatorType {
                ARITHMETIC, EQUALITY, ORDERING, LOGICAL
        }

        public enum Operator {
                PLUS("+", ARITHMETIC), //
                MINUS("-", ARITHMETIC), //
                MULTIPLY("*", ARITHMETIC), //
                DIVIDE("/", ARITHMETIC), //
                REMAINDER("%", ARITHMETIC), //
                EQUAL_TO("==", EQUALITY), //
                NOT_EQUAL_TO("!=", EQUALITY), //
                LESS_THAN("<", ORDERING), //
                LESS_THAN_EQUAL("<=", ORDERING), //
                GREATER_THAN(">", ORDERING), //
                GREATER_THAN_EQUAL(">=", ORDERING), //
                CONDITIONAL_AND("and", LOGICAL), //
                CONDITIONAL_OR("or", LOGICAL); //

            private final String symbol;

            private final OperatorType type;

            Operator(final String symbol, final OperatorType type) {
                this.symbol = symbol;
                this.type = type;
            }

            public String symbol() {
                return symbol;
            }

            public OperatorType type() {
                return type;
            }

            public boolean isArithmetic() {
                return type == ARITHMETIC;
            }

            public boolean isEqualityComparison() {
                return type == EQUALITY;
            }

            public boolean isOrderingComparison() {
                return type == ORDERING;
            }

            public boolean isLogical() {
                return type == LOGICAL;
            }
        }

        private Node arg1;

        private Node arg2;

        private final Operator op;

        public BinaryOp(final Node arg1, final Node arg2, final Operator op) {
            this.arg1 = arg1;
            this.arg2 = arg2;
            this.op = op;
            arg1.parent = this;
            arg2.parent = this;
        }

        public Node arg1() {
            return arg1;
        }

        public Node arg2() {
            return arg2;
        }

        public Operator op() {
            return op;
        }

        @Override
        public List<Node> children() {
            return List.of(arg1, arg2);
        }

        @Override
        void replaceChild(final Node child, final Node replacement) {
            if (arg1.equals(child)) {
                arg1 = replacement;
                replacement.parent = this;
            } else if (arg2.equals(child)) {
                arg2 = replacement;
                replacement.parent = this;
            }
        }

        @Override
        public String toString() {
            return "BinaryOp[" + "arg1=" + arg1 + ", " + "arg2=" + arg2 + ", " + "op=" + op + ']';
        }
    }

    final class UnaryOp extends Node {

        public enum Operator {
                MINUS("-"), //
                NOT("not"); //

            private final String symbol;

            Operator(final String symbol) {
                this.symbol = symbol;
            }

            public String symbol() {
                return symbol;
            }
        }

        private Node arg;

        private final Operator op;

        public UnaryOp(final Node arg, final Operator op) {
            this.arg = arg;
            this.op = op;
            arg.parent = this;
        }

        public Node arg() {
            return arg;
        }

        public Operator op() {
            return op;
        }

        @Override
        public List<Node> children() {
            return List.of(arg);
        }

        @Override
        void replaceChild(final Node child, final Node replacement) {
            if (arg.equals(child)) {
                arg = replacement;
                replacement.parent = this;
            }
        }

        @Override
        public String toString() {
            return "UnaryOp[" + "arg=" + arg + ", " + "op=" + op + ']';
        }
    }

    // TODO: Unify ColumnRef and ColumnIndex?

    //    final class AstColumnRef implements AstNode {
    //        private final String name;
    //
    //        private int columnIndex;
    //
    //        public AstColumnRef(final String name) {
    //            this.name = name;
    //        }
    //
    //        public String name() {
    //            return name;
    //        }
    //
    //        @Override
    //        public String toString() {
    //            return "AstColumnRef[" + "name=" + name + ", columnIndex=" + columnIndex + "]";
    //        }
    //    }

    /**
     * Collect nodes in the subtree under {@code root} by postorder traversal.
     *
     * @param root root of the tree
     * @return postorder traversal of the tree
     */
    static List<Node> postorder(final Node root) {
        var nodes = new ArrayDeque<Node>();
        var visited = new ArrayList<Node>();
        for (var node = root; node != null; node = nodes.poll()) {
            visited.add(node);
            node.children().forEach(nodes::push);
        }
        Collections.reverse(visited);
        return visited;
    }

    /**
     * Determine the input columns occurring in the given {@code Ast.Node}s. Compute a map from column index (inputs to
     * the table, that is AstColumnIndex.columnIndex()) to input index of the
     * {@link MapTransformSpec.MapperFactory#createMapper mapper} function. For example, if an expression uses (only)
     * "$[2]" and "$[5]" then these would map to input indices 0 and 1, respectively.
     *
     * @param nodes
     * @return mapping from column index to mapper input index, and vice versa
     */
    static RequiredColumns getRequiredColumns(final List<Node> nodes) {
        int[] columnIndices = nodes.stream().mapToInt(node -> {
            if (node instanceof ColumnIndex n) {
                return n.columnIndex();
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
