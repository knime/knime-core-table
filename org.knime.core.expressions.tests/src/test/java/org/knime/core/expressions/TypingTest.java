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
package org.knime.core.expressions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.knime.core.expressions.Ast.BinaryOperator.CONDITIONAL_AND;
import static org.knime.core.expressions.Ast.BinaryOperator.CONDITIONAL_OR;
import static org.knime.core.expressions.Ast.BinaryOperator.DIVIDE;
import static org.knime.core.expressions.Ast.BinaryOperator.EQUAL_TO;
import static org.knime.core.expressions.Ast.BinaryOperator.FLOOR_DIVIDE;
import static org.knime.core.expressions.Ast.BinaryOperator.GREATER_THAN;
import static org.knime.core.expressions.Ast.BinaryOperator.GREATER_THAN_EQUAL;
import static org.knime.core.expressions.Ast.BinaryOperator.LESS_THAN;
import static org.knime.core.expressions.Ast.BinaryOperator.LESS_THAN_EQUAL;
import static org.knime.core.expressions.Ast.BinaryOperator.MULTIPLY;
import static org.knime.core.expressions.Ast.BinaryOperator.NOT_EQUAL_TO;
import static org.knime.core.expressions.Ast.BinaryOperator.PLUS;
import static org.knime.core.expressions.Ast.BinaryOperator.REMAINDER;
import static org.knime.core.expressions.Ast.UnaryOperator.MINUS;
import static org.knime.core.expressions.Ast.UnaryOperator.NOT;
import static org.knime.core.expressions.AstTestUtils.BOOL;
import static org.knime.core.expressions.AstTestUtils.COL;
import static org.knime.core.expressions.AstTestUtils.FLOAT;
import static org.knime.core.expressions.AstTestUtils.INT;
import static org.knime.core.expressions.AstTestUtils.MIS;
import static org.knime.core.expressions.AstTestUtils.OP;
import static org.knime.core.expressions.AstTestUtils.STR;
import static org.knime.core.expressions.ValueType.MISSING;
import static org.knime.core.expressions.ValueType.OPT_BOOLEAN;
import static org.knime.core.expressions.ValueType.OPT_FLOAT;
import static org.knime.core.expressions.ValueType.OPT_INTEGER;
import static org.knime.core.expressions.ValueType.OPT_STRING;
import static org.knime.core.expressions.ValueType.NativeValueType.BOOLEAN;
import static org.knime.core.expressions.ValueType.NativeValueType.FLOAT;
import static org.knime.core.expressions.ValueType.NativeValueType.INTEGER;
import static org.knime.core.expressions.ValueType.NativeValueType.STRING;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.knime.core.expressions.Ast.ColumnAccess;
import org.knime.core.expressions.Expressions.MissingColumnError;
import org.knime.core.expressions.Expressions.TypingError;

/**
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("static-method")
final class TypingTest {

    @ParameterizedTest
    @EnumSource(TypingTestCase.class)
    void test(final TypingTestCase params) throws Exception {
        var ast = params.m_expression;
        var outputType = Expressions.inferTypes(ast, TEST_COLUMN_TO_TYPE);
        assertEquals(params.m_expectedType, outputType, "should fit output type");
        assertEquals(params.m_expectedType, Expressions.getInferredType(ast), "should fit output type");
        assertChildrenHaveTypes(ast);
    }

    private static enum TypingTestCase {

            // === Constants

            CONSTANT_MISSING(MIS, MISSING), //
            CONSTANT_BOOLEAN(BOOL(true), BOOLEAN), //
            CONSTANT_INTEGER(INT(100), INTEGER), //
            CONSTANT_FLOAT(FLOAT(1.0), FLOAT), //
            CONSTANT_STRING(STR("Foo"), STRING), //

            // === Column access

            INTEGER_COLUMN(COL("i"), INTEGER), //
            OPTIONAL_STRING_COLUMN(COL("s?"), OPT_STRING), //

            // === Arithmetic Operations

            // Unary Ops
            NEGATION_INTEGER(OP(MINUS, INT(-100)), INTEGER), //
            NEGATION_FLOAT(OP(MINUS, FLOAT(-100.5)), FLOAT), //
            // optional
            NEGATION_OPTIONAL_INTEGER(OP(MINUS, COL("i?")), OPT_INTEGER), //

            // Binary Ops
            SUM_OF_TWO_INTEGERS(OP(INT(10), PLUS, INT(20)), INTEGER), //
            SUM_OF_INTEGER_AND_FLOAT(OP(INT(10), PLUS, FLOAT(20.1)), FLOAT), //
            MULTIPLICATION_INT_FLOAT(OP(INT(10), MULTIPLY, FLOAT(2.5)), FLOAT), //
            MODULO_TWO_INTEGERS(OP(INT(10), REMAINDER, INT(3)), INTEGER), //
            DIVISION_OF_TWO_INTEGERS(OP(INT(10), DIVIDE, INT(20)), FLOAT), //
            DIVISION_OF_INTEGER_AND_FLOAT(OP(INT(10), DIVIDE, FLOAT(20.1)), FLOAT), //
            DIVISION_OF_TWO_FLOATS(OP(FLOAT(10.0), MULTIPLY, FLOAT(2.0)), FLOAT), //
            FLOOR_DIVISION(OP(INT(10), FLOOR_DIVIDE, INT(20)), INTEGER), //
            // optional
            SUM_OF_INTEGER_AND_OPT_INTEGER(OP(INT(10), PLUS, COL("i?")), OPT_INTEGER), //
            SUM_OF_TWO_OPT_INTEGER(OP(COL("i?"), PLUS, COL("i?")), OPT_INTEGER), //
            DIVISION_OF_OPTIONAL_INTEGERS(OP(INT(10), DIVIDE, COL("i?")), OPT_FLOAT), //
            FLOOR_DIVISION_OPTIONAL(OP(INT(10), FLOOR_DIVIDE, COL("i?")), OPT_INTEGER), //

            // === Comparison Operations

            // Ordering
            GREATER_THAN_INTEGER(OP(INT(5), GREATER_THAN, INT(20)), BOOLEAN), //
            LESS_THAN_INT_FLOAT(OP(INT(5), LESS_THAN, FLOAT(10.0)), BOOLEAN), //
            GREATER_THAN_EQ_OPT_INT(OP(COL("i?"), GREATER_THAN_EQUAL, INT(10)), BOOLEAN), //
            LESS_THAN_EQ_BOTH_OPT(OP(COL("i?"), LESS_THAN_EQUAL, COL("f?")), BOOLEAN), //

            // Equality
            EQUALITY_BOTH_STRING(OP(STR("a"), EQUAL_TO, STR("b")), BOOLEAN), //
            EQUALITY_BOTH_INT(OP(INT(10), NOT_EQUAL_TO, INT(20)), BOOLEAN), //
            EQUALITY_INT_FLOAT(OP(INT(10), EQUAL_TO, FLOAT(10.1)), BOOLEAN), //
            // optional
            EQUALITY_FLOAT_AND_OPT_FLOAT(OP(COL("f?"), EQUAL_TO, FLOAT(10.1)), BOOLEAN), //
            EQUALITY_INT_AND_OPT_FLOAT(OP(INT(10), EQUAL_TO, COL("f?")), BOOLEAN), //
            EQUALITY_BOTH_OPT_INT(OP(COL("i?"), NOT_EQUAL_TO, COL("i?")), BOOLEAN), //
            EQUALITY_STRING_AND_OPT_STRING(OP(STR("foo"), EQUAL_TO, COL("s?")), BOOLEAN), //
            // missing
            EQUALITY_INT_MISSING(OP(INT(10), EQUAL_TO, MIS), BOOLEAN), //
            EQUALITY_MISSING_OPT_STRING(OP(MIS, EQUAL_TO, COL("s?")), BOOLEAN), //
            EQUALITY_MISSING_MISSING(OP(MIS, EQUAL_TO, MIS), BOOLEAN), //

            // === Logical Operations

            LOGICAL_AND_TWO_BOOLEANS(OP(BOOL(true), CONDITIONAL_AND, BOOL(false)), BOOLEAN), //
            LOGICAL_OR_OPT_BOOLEAN(OP(COL("b?"), CONDITIONAL_OR, BOOL(true)), OPT_BOOLEAN), //
            LOGICAL_AND_OPT_BOOLEAN(OP(BOOL(true), CONDITIONAL_AND, COL("b?")), OPT_BOOLEAN), //
            LOGICAL_NOT_BOOLEAN(OP(NOT, BOOL(false)), BOOLEAN), //
            LOGICAL_NOT_OPTIONAL_BOOLEAN(OP(NOT, COL("b?")), OPT_BOOLEAN), //

            // === String Concatenation
            STRING_CONCAT_TWO_STRINGS(OP(STR("Hello, "), PLUS, STR("World!")), STRING), //
            STRING_CONCAT_OPT_STRING(OP(STR("Hello, "), PLUS, COL("s?")), STRING), //
            STRING_CONCAT_TWO_OPT_STRING(OP(COL("s?"), PLUS, COL("s?")), STRING), //
            STRING_CONCAT_INTEGER_AND_STRING(OP(INT(10), PLUS, STR("foo")), STRING), //
            STRING_CONCAT_STRING_AND_OPT_FLOAT(OP(STR("bar"), PLUS, COL("f?")), STRING), //
            STRING_CONCAT_STRING_AND_BOOL(OP(STR("bar"), PLUS, BOOL(true)), STRING), //

            // === Complex Expressions
            NESTED_BINARY_OPS(OP(OP(INT(2), PLUS, COL("i?")), MULTIPLY, FLOAT(4.0)), OPT_FLOAT), //
        ;

        private final Ast m_expression;

        private final ValueType m_expectedType;

        private TypingTestCase(final Ast expression, final ValueType expectedType) {
            m_expression = expression;
            m_expectedType = expectedType;
        }
    }

    @ParameterizedTest
    @EnumSource(TypingErrorTestCase.class)
    void testError(final TypingErrorTestCase params) throws Exception {
        var ast = params.m_expression;
        var typingError = assertThrows(TypingError.class, () -> Expressions.inferTypes(ast, TEST_COLUMN_TO_TYPE),
            "should fail type inferrence");
        var errorMessage = typingError.getMessage();
        for (var expectedSubstring : params.m_expectedErrorSubstrings) {
            assertTrue(errorMessage.contains(expectedSubstring),
                "error should contain '" + expectedSubstring + "', got '" + errorMessage + "'");
        }
    }

    private static enum TypingErrorTestCase {
            // === Arithmetic Operations
            ARITHMETICS_ON_BOOLEANS(OP(BOOL(true), PLUS, BOOL(false)), "+", "BOOLEAN"), //
            ARITHMETICS_ON_STRING_AND_BOOLEAN(OP(STR("foo"), DIVIDE, COL("b?")), "/", "STRING", "BOOLEAN?"), //
            ARITHMETICS_ON_INT_AND_BOOLEAN(OP(INT(5), PLUS, BOOL(false)), "+", "INTEGER", "BOOLEAN"), //
            ARITHMETICS_ON_INT_MISSING(OP(INT(5), PLUS, MIS), "+", "INTEGER", "MISSING"), //
            FLOOR_DIVISION_ON_FLOAT(OP(FLOAT(10.1), FLOOR_DIVIDE, FLOAT(2)), "//", "FLOAT"), //
            FLOOR_DIVISION_ON_INT_AND_FLOAT(OP(INT(2), FLOOR_DIVIDE, FLOAT(2.0)), "//", "FLOAT", "INTEGER"), //
            NEGATE_STRING(OP(MINUS, STR("foo")), "-", "STRING"), //
            NEGATE_MISSING(OP(MINUS, MIS), "-", "MISSING"), //

            // === Comparison Operations
            ORDERING_ON_INT_AND_BOOLEAN(OP(INT(100), GREATER_THAN, BOOL(false)), ">", "INTEGER", "BOOLEAN"), //
            ORDERING_ON_STRING(OP(STR("a"), LESS_THAN, STR("b")), "<", "STRING"), //
            ORDERING_ON_INT_AND_MISSING(OP(INT(20), LESS_THAN, MIS), "<", "INTEGER", "MISSING"), //
            EQUALITY_ON_STRING_AND_BOOLEAN(OP(STR("a"), NOT_EQUAL_TO, BOOL(false)), "Equality", "STRING", "BOOLEAN"), //
            EQUALITY_ON_INT_AND_STRING(OP(INT(20), EQUAL_TO, STR("bar")), "Equality", "INTEGER", "STRING"), //

            // === Logical Operations
            LOGICAL_ON_INTEGER(OP(INT(10), CONDITIONAL_AND, INT(20)), "and", "INTEGER"), //
            LOGICAL_ON_BOOL_AND_FLOAT(OP(BOOL(false), CONDITIONAL_OR, FLOAT(10.1)), "or", "FLOAT", "BOOLEAN"), //
            LOGICAL_ON_MISSING_AND_BOOL(OP(MIS, CONDITIONAL_AND, BOOL(false)), "and", "MISSING", "BOOLEAN"), //
            LOGICAL_NOT_ON_STRING(OP(NOT, STR("foo")), "not", "STRING"), //
            LOGICAL_NOT_ON_MISSING(OP(NOT, MIS), "not", "MISSING"), //

            // === String Concatenation
            STRING_CONCAT_STRING_AND_MISSING(OP(STR("foo"), PLUS, MIS), "+", "STRING", "MISSING"), //
            STRING_CONCAT_MISSING_AND_STRING(OP(MIS, PLUS, STR("foo")), "+", "STRING", "MISSING"), //
        ;

        private final Ast m_expression;

        private final String[] m_expectedErrorSubstrings;

        private TypingErrorTestCase(final Ast expression, final String... expectedErrorSubstrings) {
            m_expression = expression;
            m_expectedErrorSubstrings = expectedErrorSubstrings;
        }
    }

    @Test
    void testMissingColumn() throws Exception {
        var colName = "not_a_column";
        var ast = OP(INT(10), PLUS, COL(colName));
        var typingError = assertThrows(MissingColumnError.class, () -> Expressions.inferTypes(ast, TEST_COLUMN_TO_TYPE),
            "should fail type inferrence");
        var errorMessage = typingError.getMessage();
        assertTrue(errorMessage.contains(colName),
            "error message should contain column name '" + colName + "', was '" + errorMessage + "'");
    }

    private static final Map<String, ValueType> TEST_COLUMN_TYPES = Map.of( //
        "b", BOOLEAN, "b?", OPT_BOOLEAN, //
        "i", INTEGER, "i?", OPT_INTEGER, //
        "f", FLOAT, "f?", OPT_FLOAT, //
        "s", STRING, "s?", OPT_STRING //
    );

    private static final Function<ColumnAccess, Optional<ValueType>> TEST_COLUMN_TO_TYPE =
        c -> Optional.ofNullable(TEST_COLUMN_TYPES.get(c.name()));

    private static void assertChildrenHaveTypes(final Ast astWithTypes) {
        for (var child : astWithTypes.children()) {
            assertNotNull(Expressions.getInferredType(astWithTypes), "should have inferred type");
            assertChildrenHaveTypes(child);
        }
    }
}