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

import static org.knime.core.table.virtual.expression.Ast.OperatorType.ARITHMETIC;
import static org.knime.core.table.virtual.expression.Ast.OperatorType.EQUALITY;
import static org.knime.core.table.virtual.expression.Ast.OperatorType.LOGICAL;
import static org.knime.core.table.virtual.expression.Ast.OperatorType.ORDERING;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.knime.core.table.virtual.expression.Ast.BinaryOp;
import org.knime.core.table.virtual.expression.Ast.BooleanConstant;
import org.knime.core.table.virtual.expression.Ast.ColumnAccess;
import org.knime.core.table.virtual.expression.Ast.FloatConstant;
import org.knime.core.table.virtual.expression.Ast.FunctionCall;
import org.knime.core.table.virtual.expression.Ast.IntegerConstant;
import org.knime.core.table.virtual.expression.Ast.StringConstant;
import org.knime.core.table.virtual.expression.Ast.UnaryOp;

/**
 * An abstract syntax tree of an Expression according to the KNIME Expression Language.
 *
 * @author Tobias Pietzsch
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
public sealed interface Ast permits BooleanConstant, IntegerConstant, FloatConstant, StringConstant, ColumnAccess,
    UnaryOp, BinaryOp, FunctionCall {

    /**
     * @return a map of data that is attached to the node
     */
    Map<String, Object> data();

    /**
     * @param key
     * @return <code>true</code> if the key is present in the {@link #data() data}
     */
    default boolean hasData(final String key) {
        return data().containsKey(key);
    }

    /**
     * @param key
     * @return the value of the {@link #data() data} with the given key
     */
    default Object data(final String key) {
        return data().get(key);
    }

    /**
     * @return the children of this node
     */
    default List<Ast> children() {
        return List.of();
    }

    /**
     * @return a string representation of the AST for debugging - not the expression
     */
    @Override
    String toString();

    /**
     * @return the expression which is represented by this node
     */
    String toExpression();

    /**
     * Call the appropriate visit function of the given visitor.
     *
     * @param <O> the output type of a visit
     * @param <E> the type of an exception that might be thrown
     * @param visitor the visitor
     * @return the return value of the appropriate visit function
     * @throws E if the visitor throws
     */
    <O, E extends Exception> O accept(AstVisitor<O, E> visitor) throws E;

    // ======================================================
    // UTILITIES
    // ======================================================

    /**
     * Create a new data {@link Map} with all newly added data object
     *
     * @param data the existing data
     * @param key the key of the new object
     * @param value the value of the new object
     * @return a data {@link Map} containing the exiting data and the new object
     */
    static Map<String, Object> addData(final Map<String, Object> data, final String key, final Object value) {
        var newData = new HashMap<>(data);
        newData.put(key, value);
        return newData;
    }

    /**
     * Collect nodes in the subtree under {@code root} by postorder traversal.
     *
     * @param root root of the tree
     * @return postorder traversal of the tree
     */
    static List<Ast> postorder(final Ast root) {
        var nodes = new ArrayDeque<Ast>();
        var visited = new ArrayList<Ast>();
        for (var node = root; node != null; node = nodes.poll()) {
            visited.add(node);
            node.children().forEach(nodes::push);
        }
        Collections.reverse(visited);
        return visited;
    }

    // ======================================================
    // CREATORS
    // ======================================================

    /**
     * Create a new {@link BooleanConstant} with the given value and no data.
     *
     * @param value
     * @return the node
     */
    static BooleanConstant booleanConstant(final boolean value) {
        return booleanConstant(value, Map.of());
    }

    /**
     * Create a new {@link BooleanConstant} with the given value and data.
     *
     * @param value
     * @param data
     * @return the node
     */
    static BooleanConstant booleanConstant(final boolean value, final Map<String, Object> data) {
        return new BooleanConstant(value, data);
    }

    /**
     * Create a new {@link IntegerConstant} with the given value and no data.
     *
     * @param value
     * @return the node
     */
    static IntegerConstant integerConstant(final long value) {
        return integerConstant(value, Map.of());
    }

    /**
     * Create a new {@link IntegerConstant} with the given value and data.
     *
     * @param value
     * @param data
     * @return the node
     */
    static IntegerConstant integerConstant(final long value, final Map<String, Object> data) {
        return new IntegerConstant(value, data);
    }

    /**
     * Create a new {@link FloatConstant} with the given value and no data.
     *
     * @param value
     * @return the node
     */
    static FloatConstant floatConstant(final double value) {
        return floatConstant(value, Map.of());
    }

    /**
     * Create a new {@link FloatConstant} with the given value and data.
     *
     * @param value
     * @param data
     * @return the node
     */
    static FloatConstant floatConstant(final double value, final Map<String, Object> data) {
        return new FloatConstant(value, data);
    }

    /**
     * Create a new {@link StringConstant} with the given value and no data.
     *
     * @param value
     * @return the node
     */
    static StringConstant stringConstant(final String value) {
        return stringConstant(value, Map.of());
    }

    /**
     * Create a new {@link StringConstant} with the given value and data.
     *
     * @param value
     * @param data
     * @return the node
     */
    static StringConstant stringConstant(final String value, final Map<String, Object> data) {
        return new StringConstant(value, data);
    }

    /**
     * Create a new {@link ColumnAccess} for the given column name and with no data.
     *
     * @param name the column name
     * @return the node
     */
    static ColumnAccess columnAccess(final String name) {
        return columnAccess(name, Map.of());
    }

    /**
     * Create a new {@link ColumnAccess} for the given column name and data.
     *
     * @param name the column name
     * @param data
     * @return the node
     */
    static ColumnAccess columnAccess(final String name, final Map<String, Object> data) {
        return new ColumnAccess(name, data);
    }

    /**
     * Create a new {@link BinaryOp} on the given nodes and with no data.
     *
     * @param op the operator
     * @param arg1 the argument on the left
     * @param arg2 the argument on the right
     * @param data
     * @return the node
     */
    static BinaryOp binaryOp(final BinaryOperator op, final Ast arg1, final Ast arg2) {
        return binaryOp(op, arg1, arg2, Map.of());
    }

    /**
     * Create a new {@link BinaryOp} on the given nodes and with the given data.
     *
     * @param op the operator
     * @param arg1 the argument on the left
     * @param arg2 the argument on the right
     * @param data
     * @return the node
     */
    static BinaryOp binaryOp(final BinaryOperator op, final Ast arg1, final Ast arg2, final Map<String, Object> data) {
        return new BinaryOp(op, arg1, arg2, data);
    }

    /**
     * Create a new {@link UnaryOp} on the given node and with no data.
     *
     * @param op the operator
     * @param arg the argument
     * @param data
     * @return the node
     */
    static UnaryOp unaryOp(final UnaryOperator op, final Ast arg) {
        return unaryOp(op, arg, Map.of());
    }

    /**
     * Create a new {@link UnaryOp} on the given nodes and with the given data.
     *
     * @param op the operator
     * @param arg the argument
     * @param data
     * @return the node
     */
    static UnaryOp unaryOp(final UnaryOperator op, final Ast arg, final Map<String, Object> data) {
        return new UnaryOp(op, arg, data);
    }

    /**
     * Create a new {@link FunctionCall} with the given arguments and with no data.
     *
     * @param name the name of the function
     * @param args the arguments
     * @param data
     * @return the node
     */
    static FunctionCall functionCall(final String name, final List<Ast> args) {
        return functionCall(name, args);
    }

    /**
     * Create a new {@link FunctionCall} with the given arguments and with the given data.
     *
     * @param name the name of the function
     * @param args the arguments
     * @param data
     * @return the node
     */
    static FunctionCall functionCall(final String name, final List<Ast> args, final Map<String, Object> data) {
        return new FunctionCall(name, args, data);
    }

    // ======================================================
    // OPERATORS
    // ======================================================

    /** Categorization for operators. */
    enum OperatorType {
            ARITHMETIC, EQUALITY, ORDERING, LOGICAL
    }

    /** Available binary operators. Used by {@link BinaryOp}s. */
    enum BinaryOperator {
            PLUS("+", ARITHMETIC), //
            MINUS("-", ARITHMETIC), //
            MULTIPLY("*", ARITHMETIC), //
            DIVIDE("/", ARITHMETIC), //
            FLOOR_DIVIDE("//", ARITHMETIC), //
            EXPONENTIAL("**", ARITHMETIC), //
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

        BinaryOperator(final String symbol, final OperatorType type) {
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

    /** Available unary operators. Used by {@link UnaryOp}s. */
    enum UnaryOperator {
            MINUS("-", ARITHMETIC), //
            NOT("not", LOGICAL); //

        private final String m_symbol;

        private final OperatorType m_type;

        UnaryOperator(final String symbol, final OperatorType type) {
            m_symbol = symbol;
            m_type = type;
        }

        public String symbol() {
            return m_symbol;
        }

        public OperatorType getType() {
            return m_type;
        }
    }

    // ======================================================
    // VISITOR
    // ======================================================

    /**
     * Visitor for all implementations of nodes in an {@link Ast}.
     *
     * @param <O> the return type of a visit
     * @param <E> type of the exception that can be thrown when visiting a node
     */
    interface AstVisitor<O, E extends Exception> {

        O visit(BooleanConstant node) throws E;

        O visit(IntegerConstant node) throws E;

        O visit(FloatConstant node) throws E;

        O visit(StringConstant node) throws E;

        O visit(ColumnAccess node) throws E;

        O visit(BinaryOp node) throws E;

        O visit(UnaryOp node) throws E;

        O visit(FunctionCall node) throws E;
    }

    /**
     * Abstract implementation of an {@link AstVisitor} that just returns the input for all nodes without children and
     * recursively calls itself for all nodes with children, replacing the children with the return values. Overwrite
     * only the methods for the types of nodes that should be handled differently.
     *
     * @param <E> type of the exception that can be thrown when visiting a node
     */
    class RecursiveMappingAstVisitor<E extends Exception> implements AstVisitor<Ast, E> {

        @Override
        public Ast visit(final BooleanConstant node) throws E {
            return node;
        }

        @Override
        public Ast visit(final IntegerConstant node) throws E {
            return node;
        }

        @Override
        public Ast visit(final FloatConstant node) throws E {
            return node;
        }

        @Override
        public Ast visit(final StringConstant node) throws E {
            return node;
        }

        @Override
        public Ast visit(final ColumnAccess node) throws E {
            return node;
        }

        @Override
        public Ast visit(final BinaryOp node) throws E {
            return Ast.binaryOp(node.op(), node.arg1().accept(this), node.arg2().accept(this), node.data());
        }

        @Override
        public Ast visit(final UnaryOp node) throws E {
            return Ast.unaryOp(node.op(), node.arg().accept(this), node.data());
        }

        @Override
        public Ast visit(final FunctionCall node) throws E {
            var visitedArgs = new ArrayList<Ast>();
            for (var arg : node.children()) {
                visitedArgs.add(arg.accept(this));
            }
            return Ast.functionCall(node.name(), visitedArgs, node.data());
        }
    }

    // ======================================================
    // TREE IMPLEMENTATION
    // ======================================================

    /**
     * {@link Ast} representing a constant BOOLEAN value.
     *
     * @param value the value
     * @param data attached data
     */
    record BooleanConstant(boolean value, Map<String, Object> data) implements Ast {

        @Override
        public String toExpression() {
            return value ? "true" : "false";
        }

        @Override
        public <O, E extends Exception> O accept(final AstVisitor<O, E> visitor) throws E {
            return visitor.visit(this);
        }
    }

    /**
     * {@link Ast} representing a constant INTEGER value.
     *
     * @param value the value
     * @param data attached data
     */
    record IntegerConstant(long value, Map<String, Object> data) implements Ast {

        @Override
        public String toExpression() {
            return String.valueOf(value);
        }

        @Override
        public <O, E extends Exception> O accept(final AstVisitor<O, E> visitor) throws E {
            return visitor.visit(this);
        }
    }

    /**
     * {@link Ast} representing a constant FLOAT value.
     *
     * @param value the value
     * @param data attached data
     */
    record FloatConstant(double value, Map<String, Object> data) implements Ast {

        @Override
        public String toExpression() {
            return String.valueOf(value);
        }

        @Override
        public <O, E extends Exception> O accept(final AstVisitor<O, E> visitor) throws E {
            return visitor.visit(this);
        }
    }

    /**
     * {@link Ast} representing a constant STRING value.
     *
     * @param value the value
     * @param data attached data
     */
    record StringConstant(String value, Map<String, Object> data) implements Ast {

        @Override
        public String toExpression() {
            return "\"" //
                + value.replace("\\", "\\\\") //
                    .replace("\'", "\\'") //
                    .replace("\"", "\\\"") //
                    .replace("\b", "\\b") //
                    .replace("\f", "\\f") //
                    .replace("\n", "\\n") //
                    .replace("\r", "\\r") //
                    .replace("\t", "\\t") //
                + "\"";
        }

        @Override
        public <O, E extends Exception> O accept(final AstVisitor<O, E> visitor) throws E {
            return visitor.visit(this);
        }
    }

    /**
     * {@link Ast} representing a data column access.
     *
     * @param name the name of the column
     * @param data attached data
     */
    record ColumnAccess(String name, Map<String, Object> data) implements Ast {

        @Override
        public String toExpression() {
            return "$[\"" + name + "\"]";
        }

        @Override
        public <O, E extends Exception> O accept(final AstVisitor<O, E> visitor) throws E {
            return visitor.visit(this);
        }
    }

    /**
     * {@link Ast} representing a binary operation
     *
     * @param op the operator
     * @param arg1
     * @param arg2
     * @param data attached data
     */
    record BinaryOp(BinaryOperator op, Ast arg1, Ast arg2, Map<String, Object> data) implements Ast {

        @Override
        public String toExpression() {
            return "(" + arg1.toExpression() + " " + op.m_symbol + " " + arg2.toExpression() + ")";
        }

        @Override
        public <O, E extends Exception> O accept(final AstVisitor<O, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public List<Ast> children() {
            return List.of(arg1, arg2);
        }
    }

    /**
     * {@link Ast} representing a unary operation
     *
     * @param op the operator
     * @param arg
     * @param data attached data
     */
    record UnaryOp(UnaryOperator op, Ast arg, Map<String, Object> data) implements Ast {

        @Override
        public String toExpression() {
            return "(" + op.m_symbol + " " + arg().toExpression() + ")";
        }

        @Override
        public <O, E extends Exception> O accept(final AstVisitor<O, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public List<Ast> children() {
            return List.of(arg);
        }
    }

    /**
     * {@link Ast} representing a function call
     *
     * @param name the name of the function
     * @param args the arguments of the function
     * @param data attached data
     */
    record FunctionCall(String name, List<Ast> args, Map<String, Object> data) implements Ast {
        @Override
        public String toExpression() {
            var argsExpr = args.stream().map(Ast::toExpression).collect(Collectors.joining(", "));
            return name + "(" + argsExpr + ")";
        }

        @Override
        public <O, E extends Exception> O accept(final AstVisitor<O, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public List<Ast> children() {
            return args;
        }
    }

}
