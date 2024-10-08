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
 *
 * History
 *   Mar 25, 2024 (benjamin): created
 */
package org.knime.core.expressions;

import java.util.List;
import java.util.Map;

import org.knime.core.expressions.Ast.ConstantAst;
import org.knime.core.expressions.aggregations.ColumnAggregation;
import org.knime.core.expressions.functions.ExpressionFunction;

/**
 * Helpers to create {@link Ast}s with as few characters as possible. Only for tests.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin Germany
 */
public final class AstTestUtils {

    private AstTestUtils() {
    }

    /** @return a {@link Ast.MissingConstant} */
    public static final Ast.MissingConstant MIS() { // NOSONAR - name useful for visual clarity
        return Ast.missingConstant();
    }

    /**
     * @param value
     * @return a {@link Ast.BooleanConstant}
     */
    public static Ast.BooleanConstant BOOL(final boolean value) { // NOSONAR - name useful for visual clarity
        return Ast.booleanConstant(value);
    }

    /**
     * @param value
     * @return an {@link Ast.IntegerConstant}
     */
    public static Ast.IntegerConstant INT(final long value) { // NOSONAR - name useful for visual clarity
        return Ast.integerConstant(value);
    }

    /**
     * @param value
     * @return a {@link Ast.FloatConstant}
     */
    public static Ast.FloatConstant FLOAT(final double value) { // NOSONAR - name useful for visual clarity
        return Ast.floatConstant(value);
    }

    /**
     * @param value
     * @return a {@link Ast.StringConstant}
     */
    public static Ast.StringConstant STR(final String value) { // NOSONAR - name useful for visual clarity
        return Ast.stringConstant(value);
    }

    /**
     * @param name
     * @return a {@link Ast.ColumnAccess}
     */
    public static Ast.ColumnAccess COL(final String name) { // NOSONAR - name useful for visual clarity
        return COL(name, 0);
    }

    /**
     * Column access with an offset, accessing an adjacent row.
     *
     * @param name
     * @param offset
     * @return a {@link Ast.ColumnAccess}
     */
    public static Ast.ColumnAccess COL(final String name, final long offset) { // NOSONAR - name useful for visual clarity
        return Ast.columnAccess(name, offset);
    }

    /**
     * @return a {@link Ast.ColumnAccess}
     */
    public static Ast.ColumnAccess ROW_INDEX() { // NOSONAR - name useful for visual clarity
        return Ast.rowIndex();
    }

    /**
     * @return a {@link Ast.ColumnAccess}
     */
    public static Ast.ColumnAccess ROW_ID() { // NOSONAR - name useful for visual clarity
        return Ast.rowId();
    }

    /**
     * @param name
     * @return a {@link Ast.FlowVarAccess}
     */
    public static Ast.FlowVarAccess FLOW(final String name) { // NOSONAR - name useful for visual clarity
        return Ast.flowVarAccess(name);
    }

    /**
     * @param leftArg
     * @param op
     * @param rightArg
     * @return a {@link Ast.BinaryOp}
     */
    public static Ast.BinaryOp OP( // NOSONAR - name useful for visual clarity
        final Ast leftArg, final Ast.BinaryOperator op, final Ast rightArg) {
        return Ast.binaryOp(op, leftArg, rightArg);
    }

    /**
     * @param op
     * @param arg
     * @return an {@link Ast.UnaryOp}
     */
    public static Ast.UnaryOp OP(final Ast.UnaryOperator op, final Ast arg) { // NOSONAR - name useful for visual clarity
        return Ast.unaryOp(op, arg);
    }

    /**
     * @param name
     * @param args
     * @return a {@link Ast.FunctionCall}
     */
    public static Ast.FunctionCall FUN(final ExpressionFunction name, final Ast... args) { // NOSONAR - name useful for visual clarity
        return FUN(name, List.of(args), Map.of());
    }

    /**
     * @param expressionFunction
     * @param positionalArgs
     * @param namedArgs
     * @return a {@link Ast.FunctionCall}
     */
    public static Ast.FunctionCall FUN(final ExpressionFunction expressionFunction, final List<Ast> positionalArgs,
        final Map<String, Ast> namedArgs) { // NOSONAR - name useful for visual clarity
        var args = expressionFunction.signature(positionalArgs, namedArgs)
            .orElseThrow(cause -> new IllegalArgumentException(
                cause + " for function " + expressionFunction.name() + " with arguments " + positionalArgs + " and "
                    + namedArgs + ". But expected " + expressionFunction.description().arguments()));

        return Ast.functionCall(expressionFunction, args);
    }

    /**
     * @param name
     * @param positionalArgs
     * @return a {@link Ast.AggregationCall}
     */
    public static Ast.AggregationCall AGG(final ColumnAggregation name, final ConstantAst... positionalArgs) { // NOSONAR - name useful for visual clarity
        return AGG(name, List.of(positionalArgs), Map.of());
    }

    /**
     * @param columnAggregation
     * @param positionalArgs
     * @param namedArgs
     * @return a {@link Ast.AggregationCall}
     */
    public static Ast.AggregationCall AGG(final ColumnAggregation columnAggregation,
        final List<ConstantAst> positionalArgs, final Map<String, ConstantAst> namedArgs) { // NOSONAR - name useful for visual clarity

        var args = columnAggregation.signature(positionalArgs, namedArgs)
            .orElseThrow(cause -> new IllegalArgumentException(
                cause + " for aggregration " + columnAggregation.name() + " with arguments " + positionalArgs + " and "
                    + namedArgs + ". But expected " + columnAggregation.description().arguments()));

        return Ast.aggregationCall(columnAggregation, args);
    }
}
