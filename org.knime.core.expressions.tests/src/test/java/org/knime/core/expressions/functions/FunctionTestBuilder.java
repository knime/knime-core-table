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
 *   Apr 8, 2024 (benjamin): created
 */
package org.knime.core.expressions.functions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.junit.jupiter.api.DynamicContainer;
import org.junit.jupiter.api.DynamicNode;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.knime.core.expressions.Computer;
import org.knime.core.expressions.Computer.BooleanComputer;
import org.knime.core.expressions.Computer.FloatComputer;
import org.knime.core.expressions.Computer.IntegerComputer;
import org.knime.core.expressions.Computer.StringComputer;
import org.knime.core.expressions.TestUtils;
import org.knime.core.expressions.ValueType;

/**
 * Builder for dynamic tests for an {@link ExpressionFunction}. Add tests via {@link #typing}, {@link #illegalArgs}, and
 * {@link #impl}. Run the tests by returning the result of {@link #tests()} from a {@link TestFactory}.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin Germany
 */
public final class FunctionTestBuilder {

    // == Arguments for testing functions ==

    /**
     * An argument for tests. Can create computers that allow access to the value only once and only when it is "open".
     */
    public static final class TestingArgument {

        private final Object m_value;

        private final boolean m_isMissing;

        private boolean m_open = false;

        private boolean m_accessedValue = false;

        private boolean m_accessedIsMissing = false;

        private TestingArgument(final Object value, final boolean isMissing) {
            m_value = value;
            m_isMissing = isMissing;
        }

        private void setOpen() {
            m_open = true;
        }

        private boolean isMissing() {
            if (!m_open) {
                return fail("isMissing called during setup");
            } else if (m_accessedIsMissing) {
                return fail("isMissing called multiple times");
            } else {
                m_accessedIsMissing = true;
                return m_isMissing;
            }
        }

        private Object getValue() {
            if (!m_open) {
                return fail("compute called during setup");
            } else if (m_isMissing) {
                return fail("compute called for missing value");
            } else if (m_accessedValue) {
                return fail("compute called multiple times");
            } else {
                m_accessedValue = true;
                return m_value;
            }
        }

        private Computer computer() {
            if (m_value instanceof Boolean) {
                return BooleanComputer.of(() -> (Boolean)getValue(), this::isMissing);
            } else if (m_value instanceof Long) {
                return IntegerComputer.of(() -> (Long)getValue(), this::isMissing);
            } else if (m_value instanceof Double) {
                return FloatComputer.of(() -> (Double)getValue(), this::isMissing);
            } else if (m_value instanceof String) {
                return StringComputer.of(() -> (String)getValue(), this::isMissing);
            } else {
                return () -> isMissing();
            }
        }
    }

    /**
     * @param value
     * @return a BOOLEAN {@link TestingArgument}
     */
    public static TestingArgument arg(final boolean value) {
        return new TestingArgument(value, false);
    }

    /**
     * @param value
     * @return an INTEGER {@link TestingArgument}
     */
    public static TestingArgument arg(final long value) {
        return new TestingArgument(value, false);
    }

    /**
     * @param value
     * @return a FLOAT {@link TestingArgument}
     */
    public static TestingArgument arg(final double value) {
        return new TestingArgument(value, false);
    }

    /**
     * @param value
     * @return a STRING {@link TestingArgument}
     */
    public static TestingArgument arg(final String value) {
        return new TestingArgument(value, false);
    }

    /** @return a MISSING {@link TestingArgument} */
    public static TestingArgument mis() {
        return new TestingArgument(null, true);
    }

    /** @return a BOOLEAN {@link TestingArgument} that is missing */
    public static TestingArgument misBoolean() {
        return new TestingArgument(false, true);
    }

    /** @return an INTEGER {@link TestingArgument} that is missing */
    public static TestingArgument misInteger() {
        return new TestingArgument(1L, true);
    }

    /** @return a FLOAT {@link TestingArgument} that is missing */
    public static TestingArgument misFloat() {
        return new TestingArgument(1.0, true);
    }

    /** @return a STRING {@link TestingArgument} that is missing */
    public static TestingArgument misString() {
        return new TestingArgument("", true);
    }

    // ====

    private final ExpressionFunction m_function;

    private final List<DynamicTest> m_typingTests;

    private final List<DynamicTest> m_illegalArgsTests;

    private final List<DynamicTest> m_implTests;

    /** @param function the function that should be tested */
    public FunctionTestBuilder(final ExpressionFunction function) {
        m_function = function;
        m_typingTests = new ArrayList<>();
        m_illegalArgsTests = new ArrayList<>();
        m_implTests = new ArrayList<>();
    }

    /**
     * Add a test that checks that the return type is inferred correctly for the given argument types.
     *
     * @param name display name of the test
     * @param argTypes the types of the input arguments
     * @param expectedReturn the expected return type of {@link ExpressionFunction#returnType(List)}
     * @return <code>this</code> for chaining
     */
    public FunctionTestBuilder typing(final String name, final List<ValueType> argTypes,
        final ValueType expectedReturn) {
        m_typingTests.add(DynamicTest.dynamicTest(name, () -> {
            var returnType = m_function.returnType(argTypes);
            assertTrue(returnType.isPresent(), "should fit arguments");
            assertEquals(expectedReturn, returnType.get(), "should return correct type for " + argTypes);
        }));
        return this;
    }

    /**
     * Add a test that checks that the function does not accept the given argument types.
     *
     * @param name display name of the test
     * @param argTypes the types of the input arguments
     * @return <code>this</code> for chaining
     */
    public FunctionTestBuilder illegalArgs(final String name, final List<ValueType> argTypes) {
        m_illegalArgsTests.add(DynamicTest.dynamicTest(name, () -> {
            var returnType = m_function.returnType(argTypes);
            assertTrue(returnType.isEmpty(), "should not fit arguments");
        }));
        return this;
    }

    /**
     * Add a test that checks that the function evaluates to "MISSING".
     *
     * @param name display name of the test
     * @param args function arguments
     * @return <code>this</code> for chaining
     */
    public FunctionTestBuilder impl(final String name, final List<TestingArgument> args) {
        return impl(name, args, TestUtils.computerResultChecker(m_function.name()));
    }

    /**
     * Add a test that checks that the function evaluates to the expected BOOLEAN value;
     *
     * @param name display name of the test
     * @param args function arguments
     * @param expected
     * @return <code>this</code> for chaining
     */
    public FunctionTestBuilder impl(final String name, final List<TestingArgument> args, final boolean expected) {
        return impl(name, args, TestUtils.computerResultChecker(m_function.name(), expected));
    }

    /**
     * Add a test that checks that the function evaluates to the expected INTEGER value;
     *
     * @param name display name of the test
     * @param args function arguments
     * @param expected
     * @return <code>this</code> for chaining
     */
    public FunctionTestBuilder impl(final String name, final List<TestingArgument> args, final long expected) {
        return impl(name, args, TestUtils.computerResultChecker(m_function.name(), expected));
    }

    /**
     * Add a test that checks that the function evaluates to the expected FLOAT value;
     *
     * @param name display name of the test
     * @param args function arguments
     * @param expected
     * @return <code>this</code> for chaining
     */
    public FunctionTestBuilder impl(final String name, final List<TestingArgument> args, final double expected) {
        return impl(name, args, TestUtils.computerResultChecker(m_function.name(), expected));
    }

    /**
     * Add a test that checks that the function evaluates to the expected STRING value;
     *
     * @param name display name of the test
     * @param args function arguments
     * @param expected
     * @return <code>this</code> for chaining
     */
    public FunctionTestBuilder impl(final String name, final List<TestingArgument> args, final String expected) {
        return impl(name, args, TestUtils.computerResultChecker(m_function.name(), expected));
    }

    /**
     * Add a test that checks that the function evaluates such that the result checker succeeds.
     *
     * @param name display name of the test
     * @param args function arguments
     * @param resultChecker a consumer that checks if the returned computer evaluates to the correct value
     * @return <code>this</code> for chaining
     */
    public FunctionTestBuilder impl(final String name, final List<TestingArgument> args,
        final Consumer<Computer> resultChecker) {
        m_implTests.add(DynamicTest.dynamicTest("impl - " + name, () -> {
            var result = m_function.apply(args.stream().map(TestingArgument::computer).toList());
            // NB: we open the args after calling apply to make sure that apply does not access the computers
            args.forEach(TestingArgument::setOpen);
            resultChecker.accept(result);
        }));
        return this;
    }

    /** @return the dynamic tests */
    public List<DynamicNode> tests() {
        List<DynamicNode> tests = new ArrayList<>();
        if (!m_typingTests.isEmpty()) {
            tests.add(DynamicContainer.dynamicContainer("typing", m_typingTests));
        }
        if (!m_illegalArgsTests.isEmpty()) {
            tests.add(DynamicContainer.dynamicContainer("illegal args", m_illegalArgsTests));
        }
        if (!m_implTests.isEmpty()) {
            tests.add(DynamicContainer.dynamicContainer("impl", m_implTests));
        }
        return tests;
    }
}