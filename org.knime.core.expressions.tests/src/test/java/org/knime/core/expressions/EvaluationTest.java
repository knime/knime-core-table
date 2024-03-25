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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.knime.core.expressions.Ast.BinaryOperator.CONDITIONAL_AND;
import static org.knime.core.expressions.Ast.BinaryOperator.CONDITIONAL_OR;
import static org.knime.core.expressions.Ast.BinaryOperator.EQUAL_TO;
import static org.knime.core.expressions.Ast.BinaryOperator.LESS_THAN;
import static org.knime.core.expressions.Ast.BinaryOperator.MINUS;
import static org.knime.core.expressions.Ast.BinaryOperator.PLUS;
import static org.knime.core.expressions.Ast.UnaryOperator.NOT;
import static org.knime.core.expressions.AstTestUtils.BOOL;
import static org.knime.core.expressions.AstTestUtils.COL;
import static org.knime.core.expressions.AstTestUtils.FLOAT;
import static org.knime.core.expressions.AstTestUtils.INT;
import static org.knime.core.expressions.AstTestUtils.MIS;
import static org.knime.core.expressions.AstTestUtils.OP;
import static org.knime.core.expressions.AstTestUtils.STR;

import java.util.Arrays;
import java.util.Optional;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.knime.core.expressions.Ast.ColumnAccess;
import org.knime.core.expressions.Ast.UnaryOperator;
import org.knime.core.expressions.Computer.BooleanComputer;
import org.knime.core.expressions.Computer.FloatComputer;
import org.knime.core.expressions.Computer.IntegerComputer;
import org.knime.core.expressions.Computer.StringComputer;

/**
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("static-method")
final class EvaluationTest {

    /** A large long value for which <code>(double)(LARGE_NUMBER) == (double)(LARGE_NUMBER+1)</code> */
    private static final long LARGE_NUMBER = 9007199254740995L;

    @ParameterizedTest
    @EnumSource(ExecutionTest.class)
    void test(final ExecutionTest params) throws Exception {
        var ast = params.m_expression;
        Expressions.inferTypes(ast, FIND_TEST_COLUMN.andThen(c -> c.map(TestColumn::type)));
        var result = Evaluation.evaluate(ast, FIND_TEST_COLUMN.andThen(c -> c.orElseThrow().computer()));
        assertNotNull(result, "should output result");
        params.m_resultChecker.accept(result);
    }

    // TODO test failure cases
    // - Division by zero

    private static enum ExecutionTest {

            // === Constants

            // Missing
            CONSTANT_MISSING(AstTestUtils.MIS), //
            // Boolean
            CONSTANT_BOOLEAN_TRUE(BOOL(true), true), //
            CONSTANT_BOOLEAN_FALSE(BOOL(false), false), //
            // Integer
            CONSTANT_INTEGER_ZERO(INT(0), 0), //
            CONSTANT_INTEGER_NEGATIVE(INT(-1000), -1000), //
            CONSTANT_INTEGER_MAX_VALUE(INT(Long.MAX_VALUE), Long.MAX_VALUE), //
            CONSTANT_INTEGER_MIN_VALUE(INT(Long.MIN_VALUE), Long.MIN_VALUE), //
            // Float
            CONSTANT_FLOAT_ZERO(FLOAT(0.0), 0.0), //
            CONSTANT_FLOAT_ONE(FLOAT(1), 1.0), //
            CONSTANT_FLOAT_VERY_SMALL(FLOAT(Double.MIN_VALUE), Double.MIN_VALUE), //
            CONSTANT_FLOAT_VERY_LARGE(FLOAT(Double.MAX_VALUE), Double.MAX_VALUE), //
            // String
            CONSTANT_STRING_HELLO_WORLD(STR("Hello World"), "Hello World"), //
            CONSTANT_STRING_WITH_NEWLINES(STR("Hello\nWorld"), "Hello\nWorld"), //
            CONSTANT_STRING_WITH_SPECIAL_CHARS(STR("Hello 世界"), "Hello 世界"), //

            // === Column Access
            COLUMN_BOOLEAN(COL("BOOLEAN"), true), //
            COLUMN_INTEGER(COL("INTEGER"), 100), //
            COLUMN_FLOAT(COL("FLOAT"), 10.5), //
            COLUMN_STRING(COL("STRING"), "column value"), //
            COLUMN_MISSING(COL("INTEGER_MISSING")), //

            // === Arithmetic Operations

            // Addition
            ADDITION_OF_TWO_INTEGERS(OP(INT(10), PLUS, COL("INTEGER")), 110), //
            ADDITION_OF_INTEGER_AND_FLOAT(OP(INT(10), PLUS, COL("FLOAT")), 20.5), //
            ADDITION_OF_TWO_FLOATS(OP(COL("FLOAT"), PLUS, FLOAT(20.1)), 30.6), //
            ADDITION_OF_INTEGER_AND_MISSING(OP(INT(10), PLUS, COL("INTEGER_MISSING"))), //
            ADDITION_OF_FLOAT_AND_MISSING(OP(COL("INTEGER_MISSING"), PLUS, FLOAT(1.1))), //
            // Subtraction
            SUBTRACTION_OF_TWO_INTEGERS(OP(INT(10), MINUS, COL("INTEGER")), -90), //
            SUBTRACTION_OF_INTEGER_AND_FLOAT(OP(INT(11), MINUS, COL("FLOAT")), 0.5), //
            SUBTRACTION_OF_TWO_FLOATS(OP(COL("FLOAT"), MINUS, FLOAT(0.5)), 10.0), //
            SUBTRACTION_OF_INTEGER_AND_MISSING(OP(INT(10), MINUS, COL("INTEGER_MISSING"))), //
            SUBTRACTION_OF_FLOAT_AND_MISSING(OP(COL("INTEGER_MISSING"), MINUS, FLOAT(1.1))), //
            // Negation
            NEGATION_OF_INTEGER(OP(UnaryOperator.MINUS, COL("INTEGER")), -100), //
            NEGATION_OF_FLOAT(OP(UnaryOperator.MINUS, COL("FLOAT")), -10.5), //
            NEGATION_OF_MISSING(OP(UnaryOperator.MINUS, COL("INTEGER_MISSING"))), //
            // TODO Multiplication
            // TODO Division
            // TODO Floor division
            // TODO Exponentiation
            // TODO Modulo

            // === Comparison Operations

            // = Ordering

            // Less than
            LESS_THAN_TWO_INTEGERS(OP(INT(10), LESS_THAN, COL("INTEGER")), true), //
            LESS_THAN_TWO_LARGE_INTEGERS(OP(INT(LARGE_NUMBER), LESS_THAN, INT(LARGE_NUMBER + 1)), true), //
            LESS_THAN_TWO_FLOAT(OP(FLOAT(1.1), LESS_THAN, FLOAT(1.1)), false), //
            LESS_THAN_INTEGER_AND_FLOAT(OP(INT(100), LESS_THAN, FLOAT(99.9)), false), //
            LESS_THAN_LARGE_WITH_CAST_TO_FLOAT(OP(INT(LARGE_NUMBER), LESS_THAN, FLOAT(LARGE_NUMBER + 1)), false), //
            LESS_THAN_INTEGER_MISSING(OP(INT(Long.MIN_VALUE), LESS_THAN, COL("INTEGER_MISSING")), false), //
            LESS_THAN_FLOAT_MISSING(OP(COL("INTEGER_MISSING"), LESS_THAN, FLOAT(100)), false), //
            // TODO Less than or equal
            // TODO Greater than
            // TODO Greater than or equal

            // = Equality

            EQUAL_TWO_MISSING_LEFT(OP(MIS, EQUAL_TO, COL("INTEGER_MISSING")), true), //
            EQUAL_TWO_MISSING_RIGHT(OP(COL("INTEGER_MISSING"), EQUAL_TO, MIS), true), //
            EQUAL_TWO_INTEGERS(OP(INT(100), EQUAL_TO, COL("INTEGER")), true), //
            // TODO there is so much more to test!

            // === Logical Operations

            // And
            LOGICAL_AND_TRUE_TRUE(OP(COL("BOOLEAN"), CONDITIONAL_AND, BOOL(true)), true), //
            LOGICAL_AND_TRUE_MISSING(OP(COL("BOOLEAN"), CONDITIONAL_AND, COL("BOOLEAN_MISSING"))), //
            LOGICAL_AND_TRUE_FALSE(OP(COL("BOOLEAN"), CONDITIONAL_AND, BOOL(false)), false), //
            LOGICAL_AND_MISSING_TRUE(OP(COL("BOOLEAN_MISSING"), CONDITIONAL_AND, BOOL(true))), //
            LOGICAL_AND_MISSING_MISSING(OP(COL("BOOLEAN_MISSING"), CONDITIONAL_AND, COL("BOOLEAN_MISSING"))), //
            LOGICAL_AND_MISSING_FALSE(OP(COL("BOOLEAN_MISSING"), CONDITIONAL_AND, BOOL(false)), false), //
            LOGICAL_AND_FALSE_TRUE(OP(BOOL(false), CONDITIONAL_AND, BOOL(true)), false), //
            LOGICAL_AND_FALSE_MISSING(OP(BOOL(false), CONDITIONAL_AND, COL("BOOLEAN_MISSING")), false), //
            LOGICAL_AND_FALSE_FALSE(OP(BOOL(false), CONDITIONAL_AND, BOOL(false)), false), //
            // Or
            LOGICAL_OR_TRUE_TRUE(OP(COL("BOOLEAN"), CONDITIONAL_OR, BOOL(true)), true), //
            LOGICAL_OR_TRUE_MISSING(OP(COL("BOOLEAN"), CONDITIONAL_OR, COL("BOOLEAN_MISSING")), true), //
            LOGICAL_OR_TRUE_FALSE(OP(COL("BOOLEAN"), CONDITIONAL_OR, BOOL(false)), true), //
            LOGICAL_OR_MISSING_TRUE(OP(COL("BOOLEAN_MISSING"), CONDITIONAL_OR, BOOL(true)), true), //
            LOGICAL_OR_MISSING_MISSING(OP(COL("BOOLEAN_MISSING"), CONDITIONAL_OR, COL("BOOLEAN_MISSING"))), //
            LOGICAL_OR_MISSING_FALSE(OP(COL("BOOLEAN_MISSING"), CONDITIONAL_OR, BOOL(false))), //
            LOGICAL_OR_FALSE_TRUE(OP(BOOL(false), CONDITIONAL_OR, BOOL(true)), true), //
            LOGICAL_OR_FALSE_MISSING(OP(BOOL(false), CONDITIONAL_OR, COL("BOOLEAN_MISSING"))), //
            LOGICAL_OR_FALSE_FALSE(OP(BOOL(false), CONDITIONAL_OR, BOOL(false)), false), //
            // Not
            LOGICAL_NOT_TRUE(OP(NOT, BOOL(true)), false), //
            LOGICAL_NOT_MISSING(OP(NOT, COL("BOOLEAN_MISSING"))), //
            LOGICAL_NOT_FALE(OP(NOT, BOOL(false)), true), //

            // === String Concatenation

            STRING_CONCAT_TWO_STRINGS(OP(STR("Hello "), PLUS, STR("World!")), "Hello World!"), //
            STRING_CONCAT_STRING_AND_MISSING(OP(STR("Hello "), PLUS, COL("INTEGER_MISSING")), "Hello MISSING"), //
            STRING_CONCAT_MISSING_AND_MISSING(OP(COL("INTEGER_MISSING"), PLUS, COL("STRING_MISSING")),
                "MISSINGMISSING"), //
            STRING_CONCAT_STRING_AND_INTEGER(OP(STR("The solution is "), PLUS, INT(42)), "The solution is 42"), //
            STRING_CONCAT_STRING_AND_FLOAT(OP(FLOAT(0.0001), PLUS, STR(" is pretty small")), "1.0E-4 is pretty small"), //
            STRING_CONCAT_STRING_AND_TRUE(OP(STR("This is "), PLUS, BOOL(true)), "This is true"), //
            STRING_CONCAT_STRING_AND_FALSE(OP(BOOL(false), PLUS, STR(" it is")), "false it is"), //
        ;

        private final Ast m_expression;

        private final Consumer<Computer> m_resultChecker;

        private ExecutionTest(final Ast expression) {
            m_expression = expression;
            m_resultChecker = checkMissing();
        }

        private ExecutionTest(final Ast expression, final boolean expected) {
            m_expression = expression;
            m_resultChecker = checkBoolean(expected);
        }

        private ExecutionTest(final Ast expression, final long expected) {
            m_expression = expression;
            m_resultChecker = checkInteger(expected);
        }

        private ExecutionTest(final Ast expression, final double expected) {
            m_expression = expression;
            m_resultChecker = checkFloat(expected);
        }

        private ExecutionTest(final Ast expression, final String expected) {
            m_expression = expression;
            m_resultChecker = checkString(expected);
        }

        static Consumer<Computer> checkMissing() {
            return c -> {
                assertInstanceOf(Computer.class, c, "should eval to Computer");
                assertTrue(c.isMissing(), "should be missing");
            };
        }

        static Consumer<Computer> checkBoolean(final boolean expected) {
            return c -> {
                assertInstanceOf(BooleanComputer.class, c, "should eval to BOOLEAN");
                assertFalse(c.isMissing(), "should not be missing");
                assertEquals(expected, ((BooleanComputer)c).compute(), "should give correct result");
            };
        }

        static Consumer<Computer> checkInteger(final long expected) {
            return c -> {
                assertInstanceOf(IntegerComputer.class, c, "should eval to INTEGER");
                assertFalse(c.isMissing(), "should not be missing");
                assertEquals(expected, ((IntegerComputer)c).compute(), "should give correct result");
            };
        }

        static Consumer<Computer> checkFloat(final double expected) {
            return c -> {
                assertInstanceOf(FloatComputer.class, c, "should eval to FLOAT");
                assertFalse(c.isMissing(), "should not be missing");
                assertEquals(expected, ((FloatComputer)c).compute(), "should give correct result");
            };
        }

        static Consumer<Computer> checkString(final String expected) {
            return c -> {
                assertInstanceOf(StringComputer.class, c, "should eval to STRING");
                assertFalse(c.isMissing(), "should not be missing");
                assertEquals(expected, ((StringComputer)c).compute(), "should give correct result");
            };
        }

    }

    private static final Function<ColumnAccess, Optional<TestColumn>> FIND_TEST_COLUMN =
        colAccess -> Arrays.stream(TestColumn.values()).filter(t -> t.name().equals(colAccess.name())).findFirst();

    private static final LongSupplier THROWING_LONG_SUPPLIER = () -> {
        throw new AssertionError("should not call compute on missing values");
    };

    private static final BooleanSupplier THROWING_BOOL_SUPPLIER = () -> {
        throw new AssertionError("should not call compute on missing values");
    };

    private static final Supplier<String> THROWING_STRING_SUPPLIER = () -> {
        throw new AssertionError("should not call compute on missing values");
    };

    private static enum TestColumn {
            BOOLEAN(ValueType.OPT_BOOLEAN, BooleanComputer.of(() -> true, () -> false)), //
            INTEGER(ValueType.OPT_INTEGER, IntegerComputer.of(() -> 100, () -> false)), //
            FLOAT(ValueType.OPT_FLOAT, FloatComputer.of(() -> 10.5, () -> false)), //
            STRING(ValueType.OPT_STRING, StringComputer.of(() -> "column value", () -> false)), //
            INTEGER_MISSING(ValueType.OPT_INTEGER, IntegerComputer.of(THROWING_LONG_SUPPLIER, () -> true)), //
            BOOLEAN_MISSING(ValueType.OPT_BOOLEAN, BooleanComputer.of(THROWING_BOOL_SUPPLIER, () -> true)), //
            STRING_MISSING(ValueType.OPT_STRING, StringComputer.of(THROWING_STRING_SUPPLIER, () -> true)), //
        ;

        private final Computer m_computer;

        private final ValueType m_type;

        private TestColumn(final ValueType type, final Computer computer) {
            m_type = type;
            m_computer = computer;
        }

        Computer computer() {
            return m_computer;
        }

        ValueType type() {
            return m_type;
        }
    }
}
