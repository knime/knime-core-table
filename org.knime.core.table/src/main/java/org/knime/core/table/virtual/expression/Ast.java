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

        Node m_parent;

        public Node parent() {
            return m_parent;
        }

        void replaceChild(final Node child, final Node replacement) {
        }

        public void replaceWith(final Node node) {
            if (m_parent != null) {
                m_parent.replaceChild(this, node);
                m_parent = null;
            }
        }

        private AstType m_inferredType;

        public AstType inferredType() {
            return m_inferredType;
        }

        public void setInferredType(final AstType type) {
            this.m_inferredType = type;
        }

        public boolean isConstant() {
            return false;
        }
    }

    final class Call extends Node {
        private final String m_func;

        private final List<Node> n_args;

        public Call(final String func, final List<Node> args) {
            this.m_func = func;
            this.n_args = new ArrayList<>(args);
        }

        public int numArgs() {
            return n_args.size();
        }

        public Node arg(final int index) {
            return n_args.get(index);
        }

        @Override
        public List<Node> children() {
            return n_args;
        }

        @Override
        void replaceChild(final Node child, final Node replacement) {
            for (int i = 0; i < n_args.size(); i++) {
                if (n_args.get(i).equals(child)) {
                    n_args.set(i, replacement);
                    replacement.m_parent = this;
                }
            }
        }

        @Override
        public String toString() {
            final String arguments = n_args.stream().map(Node::toString).collect(Collectors.joining(", "));
            return "Call " + m_func + '(' + arguments + ')';
        }
    }

    final class IntConstant extends Node {
        private final long m_value;

        public IntConstant(final long value) {
            this.m_value = value;
        }

        public long value() {
            return m_value;
        }

        @Override
        public boolean isConstant() {
            return true;
        }

        @Override
        public String toString() {
            return "IntConstant[" + "value=" + m_value + ']';
        }
    }

    final class FloatConstant extends Node {
        private final double m_value;

        public FloatConstant(final double value) {
            this.m_value = value;
        }

        public double value() {
            return m_value;
        }

        @Override
        public boolean isConstant() {
            return true;
        }

        @Override
        public String toString() {
            return "FloatConstant[" + "value=" + m_value + ']';
        }
    }

    final class StringConstant extends Node {
        private final String m_value;

        public StringConstant(final String value) {
            this.m_value = value;
        }

        public String value() {
            return m_value;
        }

        @Override
        public boolean isConstant() {
            return true;
        }

        @Override
        public String toString() {
            return "StringConstant[" + "value=" + m_value + ']';
        }
    }

    final class ColumnRef extends Node {
        private final String m_name;

        public ColumnRef(final String name) {
            this.m_name = name;
        }

        public String name() {
            return m_name;
        }

        @Override
        public String toString() {
            return "ColumnRef[" + "name=" + m_name + ']';
        }
    }

    final class ColumnIndex extends Node {
        private final int m_columnIndex;

        public ColumnIndex(final int columnIndex) {
            this.m_columnIndex = columnIndex;
        }

        /**
         * Column index (0-based) in the input VirtualTable of the expression.
         *
         * @return column index
         */
        public int columnIndex() {
            return m_columnIndex;
        }

        @Override
        public String toString() {
            return "AstColumnIndex[" + "columnIndex=" + m_columnIndex + ']';
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

            private final String m_symbol;

            private final OperatorType m_type;

            Operator(final String symbol, final OperatorType type) {
                this.m_symbol = symbol;
                this.m_type = type;
            }

            public String symbol() {
                return m_symbol;
            }

            public OperatorType type() {
                return m_type;
            }

            public boolean isArithmetic() {
                return m_type == ARITHMETIC;
            }

            public boolean isEqualityComparison() {
                return m_type == EQUALITY;
            }

            public boolean isOrderingComparison() {
                return m_type == ORDERING;
            }

            public boolean isLogical() {
                return m_type == LOGICAL;
            }
        }

        private Node m_arg1;

        private Node m_arg2;

        private final Operator op;

        public BinaryOp(final Node arg1, final Node arg2, final Operator op) {
            this.m_arg1 = arg1;
            this.m_arg2 = arg2;
            this.op = op;
            arg1.m_parent = this;
            arg2.m_parent = this;
        }

        public Node arg1() {
            return m_arg1;
        }

        public Node arg2() {
            return m_arg2;
        }

        public Operator op() {
            return op;
        }

        @Override
        public List<Node> children() {
            return List.of(m_arg1, m_arg2);
        }

        @Override
        void replaceChild(final Node child, final Node replacement) {
            if (m_arg1.equals(child)) {
                m_arg1 = replacement;
                replacement.m_parent = this;
            } else if (m_arg2.equals(child)) {
                m_arg2 = replacement;
                replacement.m_parent = this;
            }
        }

        @Override
        public String toString() {
            return "BinaryOp[" + "arg1=" + m_arg1 + ", " + "arg2=" + m_arg2 + ", " + "op=" + op + ']';
        }
    }

    final class UnaryOp extends Node {

        public enum Operator {
                MINUS("-"), //
                NOT("not"); //

            private final String m_symbol;

            Operator(final String symbol) {
                this.m_symbol = symbol;
            }

            public String symbol() {
                return m_symbol;
            }
        }

        private Node arg;

        private final Operator op;

        public UnaryOp(final Node arg, final Operator op) {
            this.arg = arg;
            this.op = op;
            arg.m_parent = this;
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
                replacement.m_parent = this;
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
