package org.knime.core.table.virtual.expression;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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

        private AstType inferredType;

        public AstType inferredType() {
            return inferredType;
        }

        public void setInferredType(final AstType type) {
            this.inferredType = type;
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
        public String toString() {
            return "IntConstant[" + "value=" + value + ']';
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

        public enum Operator {
            PLUS("+"), //
            MINUS("-"), //
            MULTIPLY("*"), //
            DIVIDE("/"), //
            REMAINDER("%"); //

            private final String symbol;

            Operator(final String symbol) {
                this.symbol = symbol;
            }

            public String symbol() {
                return symbol;
            }
        }

        private final Node arg1;
        private final Node arg2;
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
        public String toString() {
            return "BinaryOp[" + "arg1=" + arg1 + ", " + "arg2=" + arg2 + ", " + "op=" + op + ']';
        }
    }


    final class UnaryOp extends Node {

        public enum Operator {
            MINUS("-"); //

            private final String symbol;

            Operator(final String symbol) {
                this.symbol = symbol;
            }

            public String symbol() {
                return symbol;
            }
        }

        private final Node arg;
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
        public String toString() {
            return "UnaryOp[" + "arg=" + arg + ", " + "op=" + op + ']';
        }
    }

    //    record FloatConstant(float value) extends Node {} // TODO
    //    record DoubleConstant(double value) extends Node {} // TODO

    // TODO: Unify ColumnRef and ColumnIndex?
    // TODO: Should type information be attached to AstNode directly, or should there be a Map<AstNode, Type> ???

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
     * @param root root of the tree
     * @return postorder traversal of the tree
     */
    static List<Node> postorder(final Node root)
    {
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
     * Determine the input columns occurring in the given {@code Ast.Node}s.
     * Compute a map from column index (inputs to the table, that is
     * AstColumnIndex.columnIndex()) to input index of the {@link
     * MapTransformSpec.MapperFactory#createMapper mapper} function. For example, if an
     * expression uses (only) "$[2]" and "$[5]" then these would map to input
     * indices 0 and 1, respectively.
     *
     * @param nodes
     * @return mapping from column index to mapper input index, and vice versa
     */
    static RequiredColumns getRequiredColumns(final List<Node> nodes) {
        int[] columnIndices = nodes.stream()
                .mapToInt(node -> {
                    if (node instanceof ColumnIndex n) {
                        return n.columnIndex();
                    } else {
                        return -1;
                    }
                })
                .filter(i -> i != -1)
                .distinct()
                .toArray();
        return new RequiredColumns(columnIndices);
    }

    record RequiredColumns(int[] columnIndices) {
        int getInputIndex(int columnIndex) {
            for (int i = 0; i < columnIndices.length; i++) {
                if ( columnIndices[i] == columnIndex )
                    return i;
            }
            throw new IndexOutOfBoundsException();
        }

        @Override
        public String toString() {
            return "RequiredColumns" + Arrays.toString(columnIndices);
        }
    }

}
