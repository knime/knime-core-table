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
 *   Feb 13, 2024 (benjamin): created
 */
package org.knime.core.table.virtual.expression;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.knime.core.table.virtual.expression.Ast.BinaryOperator.CONDITIONAL_AND;
import static org.knime.core.table.virtual.expression.Ast.BinaryOperator.DIVIDE;
import static org.knime.core.table.virtual.expression.Ast.BinaryOperator.EQUAL_TO;
import static org.knime.core.table.virtual.expression.Ast.BinaryOperator.EXPONENTIAL;
import static org.knime.core.table.virtual.expression.Ast.BinaryOperator.FLOOR_DIVIDE;
import static org.knime.core.table.virtual.expression.Ast.BinaryOperator.GREATER_THAN;
import static org.knime.core.table.virtual.expression.Ast.BinaryOperator.MINUS;
import static org.knime.core.table.virtual.expression.Ast.BinaryOperator.MULTIPLY;
import static org.knime.core.table.virtual.expression.Ast.BinaryOperator.PLUS;
import static org.knime.core.table.virtual.expression.Ast.BinaryOperator.REMAINDER;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.knime.core.table.virtual.expression.Ast.BinaryOp;
import org.knime.core.table.virtual.expression.Ast.BinaryOperator;
import org.knime.core.table.virtual.expression.Ast.BooleanConstant;
import org.knime.core.table.virtual.expression.Ast.ColumnAccess;
import org.knime.core.table.virtual.expression.Ast.FloatConstant;
import org.knime.core.table.virtual.expression.Ast.FunctionCall;
import org.knime.core.table.virtual.expression.Ast.IntegerConstant;
import org.knime.core.table.virtual.expression.Ast.StringConstant;
import org.knime.core.table.virtual.expression.Expressions.SyntaxError;

/**
 * Tests for parsing expressions in the KNIME Expression language.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("static-method")
final class ExpressionParserTest {

    @ParameterizedTest
    @EnumSource(ValidExpr.class)
    void testValidExpressions(final ValidExpr exprTest) throws SyntaxError {
        var ast = Expressions.parse(exprTest.m_input);
        assertEquals(exprTest.m_expectedAst, ast, "Wrong result for expr '" + exprTest.m_input + "'");
    }

    // TODO check for good error messages
    @ParameterizedTest
    @EnumSource(InvalidExpr.class)
    void testSyntaxErrors(final InvalidExpr exprTest) {
        assertThrows(SyntaxError.class, () -> Expressions.parse(exprTest.m_input),
            "Expected syntax error for expr '" + exprTest.m_input + "'");
    }

    enum ValidExpr {

            // BOOLEAN Literal
            BOOL_TRUE("true", BOOL(true)), //
            BOOL_FALSE("false", BOOL(false)), //

            // INTEGER literal
            INT_ZERO("0", INT(0)), //
            INT_10("10", INT(10)), //
            INT_WITH_UNDERSCORE("10_200", INT(10_200)), //

            // FLOAT literal
            FLOAT_SIMPLE("0.1", FLOAT(0.1)), //
            FLOAT_STARTING_POINT(".1", FLOAT(0.1)), //
            FLOAT_UNDERSCORE("10_200.1_2_3", FLOAT(10200.123)), //
            FLOAT_EXPONENT("3e-4", FLOAT(3e-4)), //

            // STRING literal TODO

            // Binary Operator
            // Arithmetics
            OP_PLUS("10+20", BIN_OP(INT(10), PLUS, INT(20))), //
            OP_MINUS("10-20", BIN_OP(INT(10), MINUS, INT(20))), //
            OP_MULTIPLY("10 *20", BIN_OP(INT(10), MULTIPLY, INT(20))), //
            OP_DIVIDE("10 / 20", BIN_OP(INT(10), DIVIDE, INT(20))), //
            OP_FLOOR_DIVIDE("10 // 20", BIN_OP(INT(10), FLOOR_DIVIDE, INT(20))), //
            OP_EXPONENTIAL("10** 20", BIN_OP(INT(10), EXPONENTIAL, INT(20))), //
            OP_REMAINDER("10 % 20", BIN_OP(INT(10), REMAINDER, INT(20))), //

            // Operator precedence
            OP_PREC_PLUS_MULTIPLY("1+2*3", BIN_OP(INT(1), PLUS, BIN_OP(INT(2), MULTIPLY, INT(3)))), //
            OP_PREC_PLUS_MULTIPLY_EXPONENTIAL("0+1*2**3",
                BIN_OP(INT(0), PLUS, BIN_OP(INT(1), MULTIPLY, BIN_OP(INT(2), EXPONENTIAL, INT(3))))), //
            // TODO test all operators

            // Operator associativity
            OP_ASSO_PLUS("10+20+30", BIN_OP(BIN_OP(INT(10), PLUS, INT(20)), PLUS, INT(30))), //
            OP_ASSO_MULTIPLY_DIVIDE("10/20*30", BIN_OP(BIN_OP(INT(10), DIVIDE, INT(20)), MULTIPLY, INT(30))), //
            OP_ASSO_EXPONENTIAL("10**20**30", BIN_OP(INT(10), EXPONENTIAL, BIN_OP(INT(20), EXPONENTIAL, INT(30)))), //
            // TODO test all operators

            // Parentheses
            PAREN("(1+2)*3", BIN_OP(BIN_OP(INT(1), PLUS, INT(2)), MULTIPLY, INT(3))), //
            // TODO more complex cases

            // Function calls
            FUNC_NO_ARGS("my_func()", FUNC("my_func")), //
            FUNC_SINGLE_ARG("a(1)", FUNC("a", INT(1))), //
            FUNC_UPPERCASE_ID("AB_00(1)", FUNC("AB_00", INT(1))), //
            FUNC_STARTING_UNDERSCORE("_func__(1,2,3)", FUNC("_func__", INT(1), INT(2), INT(3))), //
            FUNC_TRAILING_COMMA("foo120(1,2,)", FUNC("foo120", INT(1), INT(2))), //

            // Special stuff

            // Ignoring white spaces of each kind
            IGNORE_WHITESPACES("10\n   \n+\t\n   20\n\t   \n\n", BIN_OP(INT(10), PLUS, INT(20))), //

            // Combined examples
            COMPLEX_1("not ($email = MISSING) or ($phone_number != MISSING and $opt_in_status == true)", INT(10)), // TODO expected tree
            COMPLEX_2("$$discount_rate * $price > $price - $$min_savings", INT(10)), // TODO expected tree
            COMPLEX_3("round(avg($age), 0) >= 30 and lower($department) = \"marketing\"", INT(10)), // TODO expected tree
            COMPLEX_4("($status = \"Active\" and $days_since_last_order <= 30) or "
                + "($status == \"Pending\" and $estimated_delivery_date <= NOW + interval('7 days'))", INT(10)), // TODO expected tree
            COMPLEX_5("$[\"Order \\\"ID\"] % 2 = 0 and $quantity ** 2 > 100", INT(10)), // TODO expected tree
            COMPLEX_6("($sales - $costs) / $sales > 0.2 and $region = \"EMEA\"\n", //
                BIN_OP( //
                    BIN_OP( //
                        BIN_OP(COL("sales"), MINUS, COL("costs")), //
                        DIVIDE, //
                        BIN_OP(COL("sales"), GREATER_THAN, FLOAT(0.2)) //
                    ), //
                    CONDITIONAL_AND, //
                    BIN_OP(COL("region"), EQUAL_TO, STRING("EMEA")) //
                ) //
            ); //

        private final String m_input;

        private final Ast m_expectedAst;

        ValidExpr(final String input, final Ast expectedAst) {
            m_input = input;
            m_expectedAst = expectedAst;

        }
    }

    enum InvalidExpr {
            INT_LEADING_ZERO("01");

        private final String m_input;

        private InvalidExpr(final String input) {
            m_input = input;
        }
    }

    // Dirty helpers to create ASTs with minimal amount of chars

    private static BooleanConstant BOOL(final boolean val) {
        return new BooleanConstant(val, Map.of());
    }

    private static IntegerConstant INT(final long val) {
        return new IntegerConstant(val, Map.of());
    }

    private static FloatConstant FLOAT(final double val) {
        return new FloatConstant(val, Map.of());
    }

    private static StringConstant STRING(final String val) {
        return new StringConstant(val, Map.of());
    }

    private static ColumnAccess COL(final String name) {
        return new ColumnAccess(name, Map.of());
    }

    private static BinaryOp BIN_OP(final Ast a, final BinaryOperator op, final Ast b) {
        return new BinaryOp(op, a, b, Map.of());
    }

    private static FunctionCall FUNC(final String name, final Ast... args) {
        return new FunctionCall(name, List.of(args), Map.of());
    }
}
