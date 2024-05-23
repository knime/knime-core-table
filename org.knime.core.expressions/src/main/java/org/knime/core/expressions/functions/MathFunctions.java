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

import static org.knime.core.expressions.Computer.toFloat;
import static org.knime.core.expressions.ValueType.FLOAT;
import static org.knime.core.expressions.ValueType.INTEGER;
import static org.knime.core.expressions.ValueType.OPT_INTEGER;
import static org.knime.core.expressions.functions.ExpressionFunctionBuilder.allBaseTypesMatch;
import static org.knime.core.expressions.functions.ExpressionFunctionBuilder.anyMissing;
import static org.knime.core.expressions.functions.ExpressionFunctionBuilder.anyOptional;
import static org.knime.core.expressions.functions.ExpressionFunctionBuilder.arg;
import static org.knime.core.expressions.functions.ExpressionFunctionBuilder.functionBuilder;
import static org.knime.core.expressions.functions.ExpressionFunctionBuilder.isIntegerOrOpt;
import static org.knime.core.expressions.functions.ExpressionFunctionBuilder.isNumericOrOpt;
import static org.knime.core.expressions.functions.ExpressionFunctionBuilder.optarg;
import static org.knime.core.expressions.functions.ExpressionFunctionBuilder.vararg;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.List;
import java.util.OptionalDouble;
import java.util.function.Function;
import java.util.function.IntBinaryOperator;
import java.util.function.Predicate;
import java.util.function.ToDoubleFunction;
import java.util.function.ToLongFunction;
import java.util.stream.IntStream;

import org.knime.core.expressions.Computer;
import org.knime.core.expressions.Computer.FloatComputer;
import org.knime.core.expressions.Computer.IntegerComputer;
import org.knime.core.expressions.WarningMessageListener;

/**
 * Implementation of built-in functions that do math.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 * @author David Hickey, TNG Technology Consulting GmbH
 */
public final class MathFunctions {

    private static final String OPTIONAL_FLOAT = "FLOAT?";

    private MathFunctions() {
    }

    /** The "Math" category */
    public static final FunctionCategory CATEGORY = new FunctionCategory("Math", "Math functions");

    private static Predicate<WarningMessageListener> allArgsAreNan(final List<Computer> args) {
        return wml -> args.stream().map(c -> toFloat(c)).allMatch(c -> Double.isNaN(c.compute(wml)));
    }

    /**
     * Handy helper that implements min, max, nanmin, and nanmax, and returns a computer for them based on a couple of
     * parameters. Dramatically reduces the amount of code duplication.
     *
     * @param args the arguments to the function
     * @param max whether to compute the maximum (if true) or minimum (if false)
     * @param ignoreNan whether to ignore NaN values (if true) or not (if false)
     *
     * @return the result computer
     */
    private static Computer extremumComputerFactory(final List<Computer> args, final ExtremumType extremum,
        final boolean ignoreNan, final String functionName) {
        boolean allArgsAreIntegers = args.stream().allMatch(IntegerComputer.class::isInstance);

        if (allArgsAreIntegers) {
            return IntegerComputer.of( //
                wml -> {
                    var stream = args.stream().mapToLong(c -> ((IntegerComputer)c).compute(wml));

                    // No such thing as integer NaN, so we can safely ignore it

                    if (extremum == ExtremumType.MAXIMUM) {
                        return stream.max().getAsLong();
                    } else {
                        return stream.min().getAsLong();
                    }
                }, //
                anyMissing(args) //
            );
        } else {
            var floatArgs = args.stream().map(c -> toFloat(c)).toArray(FloatComputer[]::new);
            return FloatComputer.of( //
                wml -> {
                    @SuppressWarnings("resource") // this stream is closed, whatever Eclipse says
                    var unfilteredStream = Arrays.stream(floatArgs).mapToDouble(c -> c.compute(wml));

                    var filteredStream = ignoreNan //
                        ? unfilteredStream.filter(d -> !Double.isNaN(d)) //
                        : unfilteredStream;

                    // If these streams are empty it means that all the arguments were NaN,
                    // so we return NaN.

                    OptionalDouble extremumValue = (extremum == ExtremumType.MAXIMUM) //
                        ? filteredStream.max() //
                        : filteredStream.min();

                    return extremumValue.orElseGet(() -> {
                        wml.addWarning("invalid arguments to %s: all arguments are NaN".formatted(functionName));
                        return Float.NaN;
                    });
                }, //
                anyMissing(args) //
            );
        }
    }

    /** The maximum of multiple numbers */
    public static final ExpressionFunction MAX = functionBuilder() //
        .name("max") //
        .description("The maximum value of a list of numbers") //
        .keywords("maximum") //
        .category(CATEGORY.name()) //
        .args( //
            arg("input1", "first number", isNumericOrOpt()), //
            arg("input2", "second number", isNumericOrOpt()), //
            vararg("more", "additional numbers", isNumericOrOpt()) //
        ) //
        .returnType("the largest number of the arguments", "INTEGER? | FLOAT?",
            args -> allBaseTypesMatch(INTEGER::equals, args) ? INTEGER(anyOptional(args)) : FLOAT(anyOptional(args))) //
        .impl(MathFunctions::maxImpl) //
        .build();

    private static Computer maxImpl(final List<Computer> args) {
        return extremumComputerFactory(args, ExtremumType.MAXIMUM, false, "max");
    }

    /** The maximum of multiple numbers, ignoring NaN values */
    public static final ExpressionFunction NAN_MAX = functionBuilder() //
        .name("nanmax") //
        .description("""
                The maximum value of a list of numbers, ignoring NaN values. If all
                inputs are integers, the output will be an integer. If any of the
                inputs is a float, the output will be a float. If any argument is
                `MISSING`, the result is also `MISSING`. At least two arguments are
                required, but beyond that you can supply as many as you like.

                If _all_ arguments are NaN, the result is also NaN, and a warning
                will be emitted.

                Examples:
                * `nanmax(1, 2, 3)` returns 3
                * `nanmax(1.0, 2.0, 3.0)` returns 3.0
                * `nanmax(1, 2.0, 3)` returns 3.0
                * `nanmax(1, MISSING, 3)` returns MISSING
                * `nanmax(NaN, -1, 3)` returns 3.0
                * `nanmax(NaN, NaN, NaN)` returns NaN (with a warning)
                """) //
        .keywords("maximum", "nan") //
        .category(CATEGORY.name()) //
        .args( //
            arg("input1", "first number", isNumericOrOpt()), //
            arg("input2", "second number", isNumericOrOpt()), //
            vararg("more", "additional numbers", isNumericOrOpt()) //
        ) //
        .returnType("the largest number of the arguments, ignoring NaN values", "INTEGER? | FLOAT?",
            args -> allBaseTypesMatch(INTEGER::equals, args) ? INTEGER(anyOptional(args)) : FLOAT(anyOptional(args))) //
        .impl(MathFunctions::nanMaxImpl) //
        .build();

    private static Computer nanMaxImpl(final List<Computer> args) {
        return extremumComputerFactory(args, ExtremumType.MAXIMUM, true, "nanmax");
    }

    /** The minimum of multiple numbers */
    public static final ExpressionFunction MIN = functionBuilder() //
        .name("min") //
        .description("The minimum value of a list of numbers") //
        .keywords("minimum") //
        .category(CATEGORY.name()) //
        .args( //
            arg("input1", "first number", isNumericOrOpt()), //
            arg("input2", "second number", isNumericOrOpt()), //
            vararg("more", "additional numbers", isNumericOrOpt()) //
        ) //
        .returnType("the smallest number of the arguments", "INTEGER? | FLOAT?",
            args -> allBaseTypesMatch(INTEGER::equals, args) ? INTEGER(anyOptional(args)) : FLOAT(anyOptional(args))) //
        .impl(MathFunctions::minImpl) //
        .build();

    private static Computer minImpl(final List<Computer> args) {
        return extremumComputerFactory(args, ExtremumType.MINIMUM, false, "min");
    }

    /** The minimum of multiple numbers, ignoring NaN values */
    public static final ExpressionFunction NAN_MIN = functionBuilder() //
        .name("nanmin") //
        .description("""
                The minimum value of a list of numbers, ignoring NaN values. If all
                inputs are integers, the output will be an integer. If any of the
                inputs is a float, the output will be a float. If any argument is
                `MISSING`, the result is also `MISSING`. At least two arguments are
                required, but beyond that you can supply as many as you like.

                If _all_ arguments are NaN, the result is also NaN, and a warning
                will be emitted.

                Examples:
                * `nanmin(1, 2, 3)` returns 1
                * `nanmin(1.0, 2.0, 3.0)` returns 1.0
                * `nanmin(1, 2.0, 3)` returns 1.0
                * `nanmin(1, MISSING, 3)` returns MISSING
                * `nanmin(NaN, -1, 3)` returns -1.0
                * `nanmin(NaN, NaN, NaN)` returns NaN (with a warning)
                """) //
        .keywords("minimum", "nan") //
        .category(CATEGORY.name()) //
        .args( //
            arg("input1", "first number", isNumericOrOpt()), //
            arg("input2", "second number", isNumericOrOpt()), //
            vararg("more", "additional numbers", isNumericOrOpt()) //
        ) //
        .returnType("the smallest number of the arguments, ignoring NaN values", "INTEGER? | FLOAT?",
            args -> allBaseTypesMatch(INTEGER::equals, args) ? INTEGER(anyOptional(args)) : FLOAT(anyOptional(args))) //
        .impl(MathFunctions::nanMinImpl) //
        .build();

    private static Computer nanMinImpl(final List<Computer> args) {
        return extremumComputerFactory(args, ExtremumType.MINIMUM, true, "nanmin");
    }

    /** The normal distribution */
    public static final ExpressionFunction NORMAL = functionBuilder() //
        .name("normal") //
        .description(
            "probability of the random variable for the given normal distribution") //
        .keywords("gaussian", "distribution", "probability") //
        .category(CATEGORY.name()) //
        .args( //
            arg("value", "random variable", isNumericOrOpt()), //
            arg("mean", "mean value of the normal distribution", isNumericOrOpt()), //
            optarg("standard deviation", "standard deviation of the normal distribution", isNumericOrOpt()) //
        ) //
        .returnType("probability of the random variable for the given normal distribution.", OPTIONAL_FLOAT,
            args -> FLOAT(anyOptional(args))) //
        .impl(MathFunctions::normalImpl) //
        .build();

    private static Computer normalImpl(final List<Computer> args) {
        Predicate<WarningMessageListener> isMissing = wml -> args.stream().anyMatch(c -> c.isMissing(wml));
        return FloatComputer.of(wml -> {
            var value = toFloat(args.get(0)).compute(wml);
            var mean = toFloat(args.get(1)).compute(wml);
            var standardDeviation = args.size() > 2 ? toFloat(args.get(2)).compute(wml) : 1.0;
            return Math.exp(-Math.pow(value - mean, 2) / (2 * Math.pow(standardDeviation, 2)))
                / (standardDeviation * Math.sqrt(2 * Math.PI));
        }, isMissing);
    }

    /**
     * Approximation of the error function.
     */
    public static final ExpressionFunction ERROR_FUNCTION = functionBuilder() //
        .name("error_function") //
        .description(
            "The error function has the following interpretation: for a random variable Y "
            + "that is normally distributed with mean 0 and standard deviation 1/√2, "
            + "error_function(x) is the probability that Y falls in the range [−x, x]. Here"
            + "error_function is the scaled error function, where the mean and standard "
            + "deviation of the underlying distribution can be adjusted."
          ) //
        .keywords("gaussian", "distribution", "probability", "erf") //
        .category(CATEGORY.name()) //
        .args( //
            arg("x", "value to evaluate the error function", isNumericOrOpt()), //
            arg("mean", "Mean of the underlying normal distribution. For the standard error function "
                + "set mean=0", isNumericOrOpt()), //
            optarg("standard deviation", "Standard deviation of the underlying normal distribution. "
                + "Classical Error function is sigma=1/√2 "
                + "which is the default when no value is given.", isNumericOrOpt()) //
        ) //
        .returnType("probability of getting at least the value", OPTIONAL_FLOAT, args -> FLOAT(anyOptional(args))) //
        .impl(MathFunctions::errorFunctionImpl) //
        .build();

    private static Computer errorFunctionImpl(final List<Computer> args) {
        Predicate<WarningMessageListener> isMissing = wml -> args.stream().anyMatch(c -> c.isMissing(wml));
        return FloatComputer.of(wml -> {
            var value = toFloat(args.get(0)).compute(wml);
            var mean = toFloat(args.get(1)).compute(wml);
            var standardDeviation = args.size() > 2 ? toFloat(args.get(2)).compute(wml) : (1.0/Math.sqrt(2));

            double scaledValue = (value - mean) / (standardDeviation * Math.sqrt(2));
            return erf(scaledValue);
        }, isMissing);
    }


    /**
     * Source: Numerical Recipes in Fortran 77: The Art of Scientific Computing.
     * Cambridge University Press, 1992
     * @param x
     * @return error function result
     */
    private static double erf(final double x) {
        double t = 1 / (1 + 0.5 * Math.abs(x));
        if (x >= 0) {
            return 1 - tau(x, t);
        } else {
            return tau(-x, t) - 1;
        }
    }

    private static double tau(final double x, final double t) {
        double[] tPow = new double[10];
        tPow[0] = 1;
        for (int i = 1; i < 10; i++) {
            tPow[i] = tPow[i - 1] * t;
        }
        return t * Math.exp(-x * x - 1.26551223 + 1.00002368 * t + 0.37409196 * tPow[2] + 0.09678418 * tPow[3]
                    - 0.18628806 * tPow[4] + 0.27886807 * tPow[5] - 1.13520398 * tPow[6]
                    + 1.48851587 * tPow[7] - 0.82215223 * tPow[8] + 0.17087277 * tPow[9]);
    }

    /**
     * Helper that creates an IntBinaryOperator that can be passed to reduce to extract the index of the largest or
     * smallest value in a stream of doubles. Notably, if the stream contains a NaN, the index of the first NaN will be
     * returned. If there are multiple of the largest or smallest value, the index of the first one will be returned.
     *
     * @param largest if true, extract the index of the largest value, otherwise extract the index of the smallest value
     * @param values
     * @return
     */
    private static IntBinaryOperator nanPropagatingStreamExtremumReducer(final ExtremumType extremum,
        final Double[] values) {
        return (a, b) -> {
            if (Double.isNaN(values[a])) {
                return a;
            } else if (Double.isNaN(values[b])) {
                return b;
            } else if (extremum == ExtremumType.MAXIMUM) {
                return values[a] >= values[b] ? a : b;
            } else {
                return values[a] <= values[b] ? a : b;
            }
        };
    }

    /**
     * Helper that creates an IntBinaryOperator that can be passed to reduce to extract the index of the largest or
     * smallest value in a stream of longs. If there are multiple copies of the largest or smallest value, the index of
     * the first one will be returned.
     *
     * @param largest if true, extract the index of the largest value, otherwise extract the index of the smallest value
     * @param values
     * @return
     */
    private static IntBinaryOperator streamExtremumReducer(final ExtremumType extremum, final Long[] values) {
        return (a, b) -> {
            if (extremum == ExtremumType.MAXIMUM) {
                return values[a] >= values[b] ? a : b;
            } else {
                return values[a] <= values[b] ? a : b;
            }
        };
    }

    /**
     * Handy helper that implements argmax, argmin, nanargmax and nanargmin.
     *
     * If NaNs aren't being ignored, the function will return the argument of the first NaN to match the behaviour of
     * numpy.
     *
     * @param args the arguments to the function
     * @param max whether to compute the maximum (if true) or minimum (if false)
     * @param ignoreNan whether to ignore NaN values (if true) or not (if false)
     * @return the result supplier
     */
    private static ToLongFunction<WarningMessageListener> argExtremaHelper(final List<Computer> args,
        final ExtremumType extremum, final boolean ignoreNan) {
        boolean allArgsAreIntegers = args.stream().allMatch(IntegerComputer.class::isInstance);

        if (allArgsAreIntegers) {
            return wml -> {
                var computedArgs = args.stream().map(c -> toInteger(c).compute(wml)).toArray(Long[]::new);

                var intStream = IntStream.range(0, computedArgs.length);

                // No such thing as integer NaN, so we can safely ignore it

                // Add 1 here because we want to return 1-indexed values.
                return 1 + intStream.reduce(streamExtremumReducer(extremum, computedArgs)) //
                    .getAsInt(); //
            };
        } else {
            var floatArgs = args.stream().map(c -> toFloat(c)).toArray(FloatComputer[]::new);
            return wml -> {
                var computedArgs = Arrays.stream(floatArgs).map(c -> c.compute(wml)).toArray(Double[]::new);

                @SuppressWarnings("resource") // this stream is closed, whatever Eclipse says
                var unfilteredStream = IntStream.range(0, computedArgs.length);

                var filteredStream = ignoreNan //
                    ? unfilteredStream.filter(i -> !Double.isNaN(computedArgs[i])) //
                    : unfilteredStream;

                // Add 1 here because we want to return 1-indexed values.
                return 1 + filteredStream // Custom reducer produces first NaN if there are NaNs
                    .reduce(nanPropagatingStreamExtremumReducer(extremum, computedArgs)) //
                    .orElseThrow(() -> new IllegalStateException("Stream was empty. This is an implementation bug"));
            };
        }
    }

    /** Index of the maximum of multiple numbers */
    public static final ExpressionFunction ARGMAX = functionBuilder() //
        .name("argmax") //
        .description("""
                The index of the maximum of a list of numbers.
                If there are multiple copies of the max value, return the index of the first one.
                """) //
        .keywords() //
        .category(CATEGORY.name()) //
        .args( //
            arg("input1", "first number", isNumericOrOpt()), //
            arg("input2", "second number", isNumericOrOpt()), //
            vararg("more", "additional numbers", isNumericOrOpt()) //
        ) //
        .returnType("the index of the largest number in the arguments", //
            "INTEGER?", args -> INTEGER(anyOptional(args))) //
        .impl(MathFunctions::argmaxImpl) //
        .build();

    private static Computer argmaxImpl(final List<Computer> args) {
        return IntegerComputer.of( //
            argExtremaHelper(args, ExtremumType.MAXIMUM, false), //
            anyMissing(args) //
        );
    }

    /** Index of the minimum of multiple numbers */
    public static final ExpressionFunction ARGMIN = functionBuilder() //
        .name("argmin") //
        .description("""
                The index of the minimum of a list of numbers.
                If there are multiple copies of the min value, return the index of the first one.
                """) //
        .keywords() //
        .category(CATEGORY.name()) //
        .args( //
            arg("input1", "first number", isNumericOrOpt()), //
            arg("input2", "second number", isNumericOrOpt()), //
            vararg("more", "additional numbers", isNumericOrOpt()) //
        ) //
        .returnType("the index of the smallest number in the arguments", //
            "INTEGER?", args -> INTEGER(anyOptional(args))) //
        .impl(MathFunctions::argminImpl) //
        .build();

    private static Computer argminImpl(final List<Computer> args) {
        return IntegerComputer.of( //
            argExtremaHelper(args, ExtremumType.MINIMUM, false), //
            anyMissing(args) //
        );
    }

    /** Index of the maximum of multiple numbers, ignoring NaN values */
    public static final ExpressionFunction NAN_ARGMAX = functionBuilder() //
        .name("nanargmax") //
        .description("""
                The index of the maximum value of a list of numbers, starting at 1,
                and ignoring NaN values.

                If any argument is `MISSING`, the result is also `MISSING`. At least
                two arguments are required, but beyond that you can supply as many
                as you like.

                If there are multiple copies of the max value, returns the index of
                the first one.

                If _all_ arguments are NaN, the result is `MISSING`, and a warning
                will be emitted.

                Examples:
                * `nanargmax(2, 4, 6)` returns 3
                * `nanargmax(2.0, 4.0, 6.0)` returns 3
                * `nanargmax(2, 4.0, 6)` returns 3
                * `nanargmax(1, 2, 2)` returns 2
                * `nanargmax(1, MISSING, 3)` returns MISSING
                * `nanargmax(NaN, -1, 42)` returns 3
                * `nanargmax(NaN, NaN, NaN)` returns MISSING (with a warning)
                """) //
        .keywords() //
        .category(CATEGORY.name()) //
        .args( //
            arg("input1", "first number", isNumericOrOpt()), //
            arg("input2", "second number", isNumericOrOpt()), //
            vararg("more", "additional numbers", isNumericOrOpt()) //
        ) //
        .returnType("the index of the largest number in the arguments, ignoring NaN values", //
            "INTEGER?", args -> OPT_INTEGER) //
        .impl(MathFunctions::nanArgmaxImpl) //
        .build();

    private static Computer nanArgmaxImpl(final List<Computer> args) {
        return IntegerComputer.of( //
            argExtremaHelper(args, ExtremumType.MAXIMUM, true), //
            wml -> {
                if (anyMissing(args).test(wml)) {
                    return true;
                } else if (allArgsAreNan(args).test(wml)) {
                    wml.addWarning("Invalid arguments to nanargmax: all arguments are NaN");
                    return true;
                } else {
                    return false;
                }
            } //
        );
    }

    /** Index of the minimum of multiple numbers, ignoring NaN values */
    public static final ExpressionFunction NAN_ARGMIN = functionBuilder() //
        .name("nanargmin") //
        .description("""
                The index of the minimum value of a list of numbers, starting at 1,
                and ignoring NaN values.

                If any argument is `MISSING`, the result is also `MISSING`. At least
                two arguments are required, but beyond that you can supply as many
                as you like.

                If there are multiple copies of the max value, returns the index of
                the first one.

                If _all_ arguments are NaN, the result is `MISSING`, and a warning
                will be emitted.

                Examples:
                * `nanargmin(2, 4, 6)` returns 1
                * `nanargmin(2.0, 4.0, 6.0)` returns 1
                * `nanargmin(2, 4.0, 6)` returns 1
                * `nanargmin(1, 1, 2)` returns 1
                * `nanargmin(1, MISSING, 3)` returns MISSING
                * `nanargmin(NaN, -1, 42)` returns 2
                * `nanargmin(NaN, NaN, NaN)` returns MISSING (with warning)
                """) //
        .keywords() //
        .category(CATEGORY.name()) //
        .args( //
            arg("input1", "first number", isNumericOrOpt()), //
            arg("input2", "second number", isNumericOrOpt()), //
            vararg("more", "additional numbers", isNumericOrOpt()) //
        ) //
        .returnType("the index of the smallest number in the arguments, ignoring NaN values", //
            "INTEGER?", args -> OPT_INTEGER) //
        .impl(MathFunctions::nanArgminImpl) //
        .build();

    private static Computer nanArgminImpl(final List<Computer> args) {
        return IntegerComputer.of( //
            argExtremaHelper(args, ExtremumType.MINIMUM, true), //
            wml -> {
                if (anyMissing(args).test(wml)) {
                    return true;
                } else if (allArgsAreNan(args).test(wml)) {
                    wml.addWarning("Invalid arguments to nanargmin: all arguments are NaN");
                    return true;
                } else {
                    return false;
                }
            } //
        );
    }

    /** The absolute value of a number */
    public static final ExpressionFunction ABS = functionBuilder() //
        .name("abs") //
        .description("The absolute value of a number") //
        .keywords("absolute") //
        .category(CATEGORY.name()) //
        .args(arg("x", "the number", isNumericOrOpt())) //
        .returnType("the absolute value of x", "INTEGER? | FLOAT?", args -> args[0]) //
        .impl(MathFunctions::absImpl) //
        .build();

    private static Computer absImpl(final List<Computer> args) {
        if (args.get(0) instanceof IntegerComputer c) {
            return IntegerComputer.of(wml -> Math.abs(c.compute(wml)), c::isMissing);
        } else if (args.get(0) instanceof FloatComputer c) {
            return FloatComputer.of(wml -> Math.abs(c.compute(wml)), c::isMissing);
        }
        throw FunctionUtils.calledWithIllegalArgs();
    }

    /** The sine of one number */
    public static final ExpressionFunction SIN = functionBuilder() //
        .name("sin") //
        .description("The sine of a number") //
        .keywords() //
        .category(CATEGORY.name()) //
        .args(arg("x", "the number", isNumericOrOpt())) //
        .returnType("the sine of x", OPTIONAL_FLOAT, args -> FLOAT(anyOptional(args))) //
        .impl(MathFunctions::sinImpl) //
        .build();

    private static Computer sinImpl(final List<Computer> args) {
        var c = toFloat(args.get(0));
        return FloatComputer.of(wml -> Math.sin(c.compute(wml)), c::isMissing);
    }

    /** The cosine of one number */
    public static final ExpressionFunction COS = functionBuilder() //
        .name("cos") //
        .description("The cosine of a number") //
        .keywords() //
        .category(CATEGORY.name()) //
        .args(arg("x", "the number", isNumericOrOpt())) //
        .returnType("the cosine of x", OPTIONAL_FLOAT, args -> FLOAT(anyOptional(args))) //
        .impl(MathFunctions::cosImpl) //
        .build();

    private static Computer cosImpl(final List<Computer> args) {
        var c = toFloat(args.get(0));
        return FloatComputer.of(wml -> Math.cos(c.compute(wml)), c::isMissing);
    }

    /** The tangent of one number */
    public static final ExpressionFunction TAN = functionBuilder() //
        .name("tan") //
        .description("The tangent of a number") //
        .keywords() //
        .category(CATEGORY.name()) //
        .args(arg("x", "the number", isNumericOrOpt())) //
        .returnType("the tangent of x", OPTIONAL_FLOAT, args -> FLOAT(anyOptional(args))) //
        .impl(MathFunctions::tanImpl) //
        .build();

    private static Computer tanImpl(final List<Computer> args) {
        var c = toFloat(args.get(0));
        return FloatComputer.of(wml -> Math.tan(c.compute(wml)), c::isMissing);
    }

    /** The arcsine of one number */
    public static final ExpressionFunction ASIN = functionBuilder() //
        .name("asin") //
        .description("The arcsine of a number") //
        .keywords() //
        .category(CATEGORY.name()) //
        .args(arg("x", "the number", isNumericOrOpt())) //
        .returnType("the arcsine of x", OPTIONAL_FLOAT, args -> FLOAT(anyOptional(args))) //
        .impl(MathFunctions::asinImpl) //
        .build();

    private static Computer asinImpl(final List<Computer> args) {
        var c = toFloat(args.get(0));
        return FloatComputer.of(wml -> {
            var cC = c.compute(wml);

            if (Math.abs(cC) > 1) {
                wml.addWarning("invalid argument to asin (|arg| > 1)");
            }

            return Math.asin(cC);
        }, c::isMissing);
    }

    /** The arccosine of one number */
    public static final ExpressionFunction ACOS = functionBuilder() //
        .name("acos") //
        .description("The arccosine of a number") //
        .keywords() //
        .category(CATEGORY.name()) //
        .args(arg("x", "the number", isNumericOrOpt())) //
        .returnType("the arccosine of x", OPTIONAL_FLOAT, args -> FLOAT(anyOptional(args))) //
        .impl(MathFunctions::acosImpl) //
        .build();

    private static Computer acosImpl(final List<Computer> args) {
        var c = toFloat(args.get(0));
        return FloatComputer.of(wml -> {
            var cC = c.compute(wml);

            if (Math.abs(cC) > 1) {
                wml.addWarning("invalid argument to acos (|arg| > 1)");
            }

            return Math.acos(cC);
        }, c::isMissing);
    }

    /** The arctan of one number */
    public static final ExpressionFunction ATAN = functionBuilder() //
        .name("atan") //
        .description("The arctangent of a number") //
        .keywords() //
        .category(CATEGORY.name()) //
        .args(arg("x", "the number", isNumericOrOpt())) //
        .returnType("the arctangent of x", OPTIONAL_FLOAT, args -> FLOAT(anyOptional(args))) //
        .impl(MathFunctions::atanImpl) //
        .build();

    private static Computer atanImpl(final List<Computer> args) {
        var c = toFloat(args.get(0));
        return FloatComputer.of(wml -> Math.atan(c.compute(wml)), c::isMissing);
    }

    /** The arctan of two numbers */
    public static final ExpressionFunction ATAN2 = functionBuilder() //
        .name("atan2") //
        .description("The arctangent of two numbers") //
        .keywords() //
        .category(CATEGORY.name()) //
        .args( //
            arg("y", "the number", isNumericOrOpt()), //
            arg("x", "the number", isNumericOrOpt()) //
        ) //
        .returnType("the arctangent of y and x", OPTIONAL_FLOAT, args -> FLOAT(anyOptional(args))) //
        .impl(MathFunctions::atan2Impl) //
        .build();

    private static Computer atan2Impl(final List<Computer> args) {
        var y = toFloat(args.get(0));
        var x = toFloat(args.get(1));
        return FloatComputer.of( //
            wml -> Math.atan2(y.compute(wml), x.compute(wml)), //
            anyMissing(args) //
        );
    }

    /** Hyperbolic sine of one number */
    public static final ExpressionFunction SINH = functionBuilder() //
        .name("sinh") //
        .description("The hyperbolic sine of a number") //
        .keywords() //
        .category(CATEGORY.name()) //
        .args(arg("x", "the number", isNumericOrOpt())) //
        .returnType("the hyerbolic sine of x", OPTIONAL_FLOAT, args -> FLOAT(anyOptional(args))) //
        .impl(MathFunctions::sinhImpl) //
        .build();

    private static Computer sinhImpl(final List<Computer> args) {
        var c = toFloat(args.get(0));
        return FloatComputer.of(wml -> Math.sinh(c.compute(wml)), c::isMissing);
    }

    /** Hyperbolic cosine of one number */
    public static final ExpressionFunction COSH = functionBuilder() //
        .name("cosh") //
        .description("The hyperbolic cosine of a number") //
        .keywords() //
        .category(CATEGORY.name()) //
        .args(arg("x", "the number", isNumericOrOpt())) //
        .returnType("the hyerbolic cosine of x", OPTIONAL_FLOAT, args -> FLOAT(anyOptional(args))) //
        .impl(MathFunctions::coshImpl) //
        .build();

    private static Computer coshImpl(final List<Computer> args) {
        var c = toFloat(args.get(0));
        return FloatComputer.of(wml -> {
            var cC = c.compute(wml);

            if (cC < 1) {
                wml.addWarning("invalid argument to cosh (arg < 1)");
            }

            return Math.cosh(cC);
        }, c::isMissing);
    }

    /** Hyperbolic tangent of one number */
    public static final ExpressionFunction TANH = functionBuilder() //
        .name("tanh") //
        .description("The hyperbolic tangent of a number") //
        .keywords() //
        .category(CATEGORY.name()) //
        .args(arg("x", "the number", isNumericOrOpt())) //
        .returnType("the hyerbolic tangent of x", OPTIONAL_FLOAT, args -> FLOAT(anyOptional(args))) //
        .impl(MathFunctions::tanhImpl) //
        .build();

    private static Computer tanhImpl(final List<Computer> args) {
        var c = toFloat(args.get(0));
        return FloatComputer.of(wml -> Math.tanh(c.compute(wml)), c::isMissing);
    }

    /** Hyperbolic arcsine of one number */
    public static final ExpressionFunction ASINH = functionBuilder() //
        .name("asinh") //
        .description("The hyperbolic arcsine of a number") //
        .keywords("arcsinh", "arsinh") //
        .category(CATEGORY.name()) //
        .args(arg("x", "the number", isNumericOrOpt())) //
        .returnType("the hyerbolic arcsine of x", OPTIONAL_FLOAT, args -> FLOAT(anyOptional(args))) //
        .impl(MathFunctions::asinhImpl) //
        .build();

    private static Computer asinhImpl(final List<Computer> args) {
        var c = toFloat(args.get(0));

        ToDoubleFunction<WarningMessageListener> value = wml -> {
            var cC = c.compute(wml);

            return Math.log(cC + Math.sqrt(cC * cC + 1));
        };

        return FloatComputer.of(value, c::isMissing);
    }

    /** Hyperbolic arccosine of one number */
    public static final ExpressionFunction ACOSH = functionBuilder() //
        .name("acosh") //
        .description("The hyperbolic arccosine of a number") //
        .keywords("arccosh", "arcosh") //
        .category(CATEGORY.name()) //
        .args(arg("x", "the number", isNumericOrOpt())) //
        .returnType("the hyerbolic arccosine of x", OPTIONAL_FLOAT, args -> FLOAT(anyOptional(args))) //
        .impl(MathFunctions::acoshImpl) //
        .build();

    private static Computer acoshImpl(final List<Computer> args) {
        var c = toFloat(args.get(0));

        ToDoubleFunction<WarningMessageListener> value = wml -> {
            var cC = c.compute(wml);

            return Math.log(cC + Math.sqrt(cC * cC - 1));
        };

        return FloatComputer.of(value, c::isMissing);
    }

    /** Hyperbolic arctangent of one number */
    public static final ExpressionFunction ATANH = functionBuilder() //
        .name("atanh") //
        .description("The hyperbolic arctangent of a number") //
        .keywords("arctanh", "artanh") //
        .category(CATEGORY.name()) //
        .args(arg("x", "the number", isNumericOrOpt())) //
        .returnType("the hyerbolic arctangent of x", OPTIONAL_FLOAT, args -> FLOAT(anyOptional(args))) //
        .impl(MathFunctions::atanhImpl) //
        .build();

    private static Computer atanhImpl(final List<Computer> args) {
        var c = toFloat(args.get(0));

        ToDoubleFunction<WarningMessageListener> value = wml -> {
            var cC = c.compute(wml);

            if (Math.abs(cC) >= 1) {
                wml.addWarning("invalid argument to atanh (|arg| >= 1)");
            }

            return 0.5 * Math.log((cC + 1.0) / (1.0 - cC));
        };

        return FloatComputer.of(value, c::isMissing);
    }

    /** The natural logarithm of one number */
    public static final ExpressionFunction LN = functionBuilder() //
        .name("ln") //
        .description("The natural logarithm of a number") //
        .keywords() //
        .category(CATEGORY.name()) //
        .args(arg("x", "the number", isNumericOrOpt())) //
        .returnType("the natural logarithm of x", OPTIONAL_FLOAT, args -> FLOAT(anyOptional(args))) //
        .impl(MathFunctions::lnImpl) //
        .build();

    private static Computer lnImpl(final List<Computer> args) {
        var c = toFloat(args.get(0));
        return FloatComputer.of(wml -> {
            var cC = c.compute(wml);

            if (cC <= 0) {
                wml.addWarning("invalid argument to ln (arg <= 0)");
            }

            return Math.log(cC);
        }, c::isMissing);
    }

    /** The base-10 logarithm of one number */
    public static final ExpressionFunction LOG10 = functionBuilder() //
        .name("log10") //
        .description("The base-10 logarithm of a number") //
        .keywords() //
        .category(CATEGORY.name()) //
        .args(arg("x", "the number", isNumericOrOpt())) //
        .returnType("the base-10 logarithm of x", OPTIONAL_FLOAT, args -> FLOAT(anyOptional(args)))
        .impl(MathFunctions::log10Impl) //
        .build();

    private static Computer log10Impl(final List<Computer> args) {
        var c = toFloat(args.get(0));
        return FloatComputer.of(wml -> {
            var cC = c.compute(wml);

            if (cC <= 0) {
                wml.addWarning("invalid argument to log10 (arg <= 0)");
            }

            return Math.log10(cC);
        }, c::isMissing);
    }

    /** The base-2 logarithm of one number */
    public static final ExpressionFunction LOG2 = functionBuilder() //
        .name("log2") //
        .description("The base-2 logarithm of a number") //
        .keywords() //
        .category(CATEGORY.name()) //
        .args(arg("x", "the number", isNumericOrOpt())) //
        .returnType("the base-2 logarithm of x", OPTIONAL_FLOAT, args -> FLOAT(anyOptional(args)))
        .impl(MathFunctions::log2Impl) //
        .build();

    private static Computer log2Impl(final List<Computer> args) {
        var c = toFloat(args.get(0));
        return FloatComputer.of(wml -> {
            var cC = c.compute(wml);

            if (cC <= 0) {
                wml.addWarning("invalid argument to log2 (arg <= 0)");
            }

            return Math.log(cC) / Math.log(2);
        }, c::isMissing);
    }

    /** The base-n logarithm of one number */
    public static final ExpressionFunction LOG_BASE = functionBuilder() //
        .name("log") //
        .description("The logarithm of a number, with the given base") //
        .keywords() //
        .category(CATEGORY.name()) //
        .args( //
            arg("x", "the number", isNumericOrOpt()), //
            arg("b", "the base", isNumericOrOpt()) //
        ) //
        .returnType("the base logarithm of x base b", OPTIONAL_FLOAT, args -> FLOAT(anyOptional(args)))
        .impl(MathFunctions::logBaseImpl) //
        .build();

    private static Computer logBaseImpl(final List<Computer> args) {
        var c = toFloat(args.get(0));
        var b = toFloat(args.get(1));

        ToDoubleFunction<WarningMessageListener> value = wml -> {
            var bC = b.compute(wml);
            var cC = c.compute(wml);

            if (bC <= 0) {
                wml.addWarning("invalid argument to log (base <= 0)");
            }

            if (cC <= 0) {
                wml.addWarning("invalid argument to log (number <= 0)");
            }

            return (Math.abs(bC) < 2 * Double.MIN_VALUE) ? Float.NaN : (Math.log(cC) / Math.log(bC));
        };

        return FloatComputer.of(value, anyMissing(args));
    }

    /** natural log of a number plus 1, i.e. ln(1+x) */
    public static final ExpressionFunction LOG1P = functionBuilder() //
        .name("log1p") //
        .description("The natural logarithm of one plus a number") //
        .keywords() //
        .category(CATEGORY.name()) //
        .args(arg("x", "the number", isNumericOrOpt())) //
        .returnType("natural log of (1+x)", OPTIONAL_FLOAT, args -> FLOAT(anyOptional(args))) //
        .impl(MathFunctions::log1pImpl) //
        .build();

    private static Computer log1pImpl(final List<Computer> args) {
        var c = toFloat(args.get(0));
        return FloatComputer.of( //
            wml -> {
                var cC = c.compute(wml);

                if (cC <= -1) {
                    wml.addWarning("invalid argument to log1p (arg <= -1)");
                }

                return Math.log1p(cC);
            }, //
            c::isMissing //
        );
    }

    /** exponent of a number */
    public static final ExpressionFunction EXP = functionBuilder() //
        .name("exp") //
        .description("e raised to the power of a number") //
        .keywords() //
        .category(CATEGORY.name()) //
        .args(arg("x", "the number", isNumericOrOpt())) //
        .returnType("exponent of x", OPTIONAL_FLOAT, args -> FLOAT(anyOptional(args))) //
        .impl(MathFunctions::expImpl) //
        .build();

    private static Computer expImpl(final List<Computer> args) {
        var c = toFloat(args.get(0));
        return FloatComputer.of( //
            wml -> Math.exp(c.compute(wml)), //
            c::isMissing //
        );
    }

    /** For two numbers, x to the power of y */
    public static final ExpressionFunction POW = functionBuilder() //
        .name("pow") //
        .description("One number raised to the power of another") //
        .keywords() //
        .category(CATEGORY.name()) //
        .args( //
            arg("x", "the base", isNumericOrOpt()), //
            arg("y", "the exponent", isNumericOrOpt()) //
        ) //
        .returnType("x to the power of y", "INTEGER? | FLOAT?",
            args -> allBaseTypesMatch(INTEGER::equals, args) ? INTEGER(anyOptional(args)) : FLOAT(anyOptional(args))) //
        .impl(MathFunctions::powImpl) //
        .build();

    private static Computer powImpl(final List<Computer> args) {
        if (args.stream().allMatch(IntegerComputer.class::isInstance)) {
            var x = toInteger(args.get(0));
            var y = toInteger(args.get(1));
            return IntegerComputer.of( //
                wml -> {
                    var xC = x.compute(wml);
                    var yC = y.compute(wml);

                    if (xC == 0 && yC == 0) {
                        wml.addWarning("invalid arguments to pow (pow(0,0) is undefined)");
                        return 0;
                    }

                    return (long)Math.pow(xC, yC);
                }, anyMissing(args) //
            );
        } else {
            var x = toFloat(args.get(0));
            var y = toFloat(args.get(1));
            return FloatComputer.of( //
                wml -> {
                    var xC = x.compute(wml);
                    var yC = y.compute(wml);

                    if (isNearZero(xC) && isNearZero(yC)) {
                        wml.addWarning("invalid arguments to pow (pow(0,0) is undefined)");
                        return Float.NaN;
                    }

                    return Math.pow(xC, yC);
                }, anyMissing(args) //
            );
        }
    }

    /** square root of a number */
    public static final ExpressionFunction SQRT = functionBuilder() //
        .name("sqrt") //
        .description("square root of a number") //
        .keywords() //
        .category(CATEGORY.name()) //
        .args(arg("x", "the number", isNumericOrOpt())) //
        .returnType("square root of x", OPTIONAL_FLOAT, args -> FLOAT(anyOptional(args))) //
        .impl(MathFunctions::sqrtImpl) //
        .build();

    private static Computer sqrtImpl(final List<Computer> args) {
        var c = toFloat(args.get(0));
        return FloatComputer.of( //
            wml -> {
                var cC = c.compute(wml);

                if (cC < 0) {
                    wml.addWarning("invalid argument to sqrt (x < 0)");
                }

                return Math.sqrt(cC); //
            }, c::isMissing //
        );
    }

    /** One number modulo another */
    public static final ExpressionFunction MOD = functionBuilder() //
        .name("mod") //
        .description("One number modulo another") //
        .keywords() //
        .category(CATEGORY.name()) //
        .args( //
            arg("x", "the numerator", isNumericOrOpt()), //
            arg("y", "the divisor", isNumericOrOpt()) //
        ) //
        .returnType("x modulo y", "INTEGER? | FLOAT?",
            args -> allBaseTypesMatch(INTEGER::equals, args) ? INTEGER(anyOptional(args)) : FLOAT(anyOptional(args))) //
        .impl(MathFunctions::modImpl) //
        .build();

    private static Computer modImpl(final List<Computer> args) {
        if (args.stream().allMatch(IntegerComputer.class::isInstance)) {
            return IntegerComputer.of(wml -> {
                var x = toInteger(args.get(0)).compute(wml);
                var y = toInteger(args.get(1)).compute(wml);

                if (y == 0) {
                    wml.addWarning("invalid arguments to mod (y = 0)");
                    return 0;
                }

                return x % y;
            }, anyMissing(args));
        } else {
            return FloatComputer.of( //
                wml -> {
                    var x = toFloat(args.get(0)).compute(wml);
                    var y = toFloat(args.get(1)).compute(wml);

                    if (isNearZero(y)) {
                        wml.addWarning("invalid arguments to mod (y == 0)");
                        return Float.NaN;
                    }

                    return x % y;
                }, anyMissing(args) //
            );
        }
    }

    /** number from radians to degrees */
    public static final ExpressionFunction DEGREES = functionBuilder() //
        .name("degrees") //
        .description("Convert a number from radians to degrees") //
        .keywords() //
        .category(CATEGORY.name()) //
        .args(arg("x", "the number", isNumericOrOpt())) //
        .returnType("x in degrees", OPTIONAL_FLOAT, args -> FLOAT(anyOptional(args))) //
        .impl(MathFunctions::degreesImpl) //
        .build();

    private static Computer degreesImpl(final List<Computer> args) {
        var c = toFloat(args.get(0));
        return FloatComputer.of( //
            wml -> Math.toDegrees(c.compute(wml)), //
            c::isMissing //
        );
    }

    /** number from degrees to radians */
    public static final ExpressionFunction RADIANS = functionBuilder() //
        .name("radians") //
        .description("Convert a number from degrees to radians") //
        .keywords() //
        .category(CATEGORY.name()) //
        .args(arg("x", "the number", isNumericOrOpt())) //
        .returnType("x in radians", OPTIONAL_FLOAT, args -> FLOAT(anyOptional(args))) //
        .impl(MathFunctions::radiansImpl) //
        .build();

    private static Computer radiansImpl(final List<Computer> args) {
        var c = toFloat(args.get(0));
        return FloatComputer.of( //
            wml -> Math.toRadians(c.compute(wml)), //
            c::isMissing //
        );
    }

    /** floor of number */
    public static final ExpressionFunction FLOOR = functionBuilder() //
        .name("floor") //
        .description("Round x to nearest smaller integer") //
        .keywords() //
        .category(CATEGORY.name()) //
        .args(arg("x", "the number", isNumericOrOpt())) //
        .returnType("nearest integer less than x", "INTEGER?", args -> INTEGER(anyOptional(args))) //
        .impl(MathFunctions::floorImpl) //
        .build();

    private static Computer floorImpl(final List<Computer> args) {
        var c = toFloat(args.get(0));
        return IntegerComputer.of( //
            wml -> (long)Math.floor(c.compute(wml)), //
            wml -> {
                if (c.isMissing(wml)) {
                    return true;
                } else if (Double.isNaN(c.compute(wml))) {
                    wml.addWarning("Invalid arguments to floor: arg is NaN");
                    return true;
                } else {
                    return false;
                }
            } //
        );
    }

    /** ceil of number */
    public static final ExpressionFunction CEIL = functionBuilder() //
        .name("ceil") //
        .description("Round x to nearest larger integer") //
        .keywords() //
        .category(CATEGORY.name()) //
        .args(arg("x", "the number", isNumericOrOpt())) //
        .returnType("nearest integer greater than x", "INTEGER?", args -> INTEGER(anyOptional(args))) //
        .impl(MathFunctions::ceilImpl) //
        .build();

    private static Computer ceilImpl(final List<Computer> args) {
        var c = toFloat(args.get(0));
        return IntegerComputer.of( //
            wml -> (int)Math.ceil(c.compute(wml)), //
            wml -> {
                if (c.isMissing(wml)) {
                    return true;
                } else if (Double.isNaN(c.compute(wml))) {
                    wml.addWarning("Invalid arguments to ceil: arg is NaN");
                    return true;
                } else {
                    return false;
                }
            } //
        );
    }

    /** truncate number, i.e. round towards zero */
    public static final ExpressionFunction TRUNC = functionBuilder() //
        .name("trunc") //
        .description("Round x towards zero") //
        .keywords("rounddown") //
        .category(CATEGORY.name()) //
        .args(arg("x", "the number", isNumericOrOpt())) //
        .returnType("nearest integer closer to zero than x", "INTEGER?", args -> INTEGER(anyOptional(args))) //
        .impl(MathFunctions::truncImpl) //
        .build();

    private static Computer truncImpl(final List<Computer> args) {
        var c = toFloat(args.get(0));
        return IntegerComputer.of( //
            wml -> BigDecimal.valueOf(c.compute(wml)).setScale(0, RoundingMode.DOWN).longValue(), //
            wml -> {
                if (c.isMissing(wml)) {
                    return true;
                } else if (Double.isNaN(c.compute(wml))) {
                    wml.addWarning("Invalid arguments to trunc: arg is NaN");
                    return true;
                } else {
                    return false;
                }
            } //
        );
    }

    /** round towards nearest integer (n + 0.5 rounds to nearest closer to zero) */
    public static final ExpressionFunction ROUNDHALFDOWN = functionBuilder() //
        .name("roundhalfdown") //
        .description("Round x to n decimal places. In case of ambiguity, round to the one closer to zero.") //
        .keywords() //
        .category(CATEGORY.name()) //
        .args( //
            arg("x", "the number", isNumericOrOpt()), //
            optarg("n", "precision", isIntegerOrOpt()) //
        ) //
        .returnType("nearest integer to x", "INTEGER? | FLOAT?",
            args -> (args.length == 1) ? INTEGER(anyOptional(args)) : FLOAT(anyOptional(args))) //
        .impl(roundImplFactory(RoundingMode.HALF_DOWN, "roundhalfdown")) //
        .build();

    /** round towards nearest integer (n + 0.5 rounds to nearest further from zero) */
    public static final ExpressionFunction ROUNDHALFUP = functionBuilder() //
        .name("roundhalfup") //
        .description("Round x to n decimal places. In case of ambiguity, round to the one further from zero.") //
        .keywords() //
        .category(CATEGORY.name()) //
        .args( //
            arg("x", "the number", isNumericOrOpt()), //
            optarg("n", "precision", isIntegerOrOpt()) //
        ) //
        .returnType("nearest integer to x", "INTEGER? | FLOAT?",
            args -> (args.length == 1) ? INTEGER(anyOptional(args)) : FLOAT(anyOptional(args))) //
        .impl(roundImplFactory(RoundingMode.HALF_UP, "roundhalfup")) //
        .build();

    /** round towards nearest integer (n + 0.5 rounds to nearest even) */
    public static final ExpressionFunction ROUNDHALFEVEN = functionBuilder() //
        .name("round") //
        .description("Round x to n decimal places. In case of ambiguity, round to the one that is even.") //
        .keywords("roundhalfeven") //
        .category(CATEGORY.name()) //
        .args( //
            arg("x", "the number", isNumericOrOpt()), //
            optarg("n", "precision", isIntegerOrOpt()) //
        ) //
        .returnType("nearest integer to x", "INTEGER? | FLOAT?",
            args -> (args.length == 1) ? INTEGER(anyOptional(args)) : FLOAT(anyOptional(args))) //
        .impl(roundImplFactory(RoundingMode.HALF_EVEN, "round")) //
        .build();

    /**
     * Factory for round function implementations.
     *
     * @param mode the rounding mode
     * @return the function implementation
     */
    private static Function<List<Computer>, Computer> roundImplFactory(final RoundingMode mode,
        final String functionName) {
        return args -> {
            var c = toFloat(args.get(0));

            if (args.size() == 1) {
                // Return integer
                return IntegerComputer.of( //
                    wml -> BigDecimal.valueOf(c.compute(wml)).setScale(0, mode).longValue(), //
                    wml -> {
                        if (anyMissing(args).test(wml)) {
                            return true;
                        } else if (Double.isNaN(c.compute(wml))) {
                            wml.addWarning("Invalid arguments to %s: arg is NaN".formatted(functionName));
                            return true;
                        } else {
                            return false;
                        }
                    });
            } else {
                return FloatComputer.of(wml -> {
                    int scale = (int)toInteger(args.get(1)).compute(wml);
                    double value = c.compute(wml);

                    if (Double.isNaN(value)) {
                        return Double.NaN;
                    } else {
                        return BigDecimal.valueOf(value).setScale(scale, mode).doubleValue();
                    }
                }, anyMissing(args));
            }
        };
    }

    /** The sign of one number */
    public static final ExpressionFunction SIGN = functionBuilder() //
        .name("sign") //
        .description("The sign of the number") //
        .keywords() //
        .category(CATEGORY.name()) //
        .args(arg("x", "the number", isNumericOrOpt())) //
        .returnType("the sign of x", "INTEGER?", args -> INTEGER(anyOptional(args))) //
        .impl(MathFunctions::signImpl) //
        .build();

    private static Computer signImpl(final List<Computer> args) {
        var c = toFloat(args.get(0));
        return IntegerComputer.of( //
            wml -> (int)Math.signum(c.compute(wml)), //
            wml -> {
                if (c.isMissing(wml)) {
                    return true;
                } else if (Double.isNaN(c.compute(wml))) {
                    wml.addWarning("Invalid arguments to sign: arg is NaN");
                    return true;
                } else {
                    return false;
                }
            } //
        );
    }

    /** The mean of multiple numbers */
    public static final ExpressionFunction AVERAGE = functionBuilder() //
        .name("average") //
        .description("The mean value of a list of numbers") //
        .keywords("mean") //
        .category(CATEGORY.name()) //
        .args( //
            arg("input1", "first number", isNumericOrOpt()), //
            arg("input2", "second number", isNumericOrOpt()), //
            vararg("more", "additional numbers", isNumericOrOpt()) //
        ) //
        .returnType("the mean of the arguments", OPTIONAL_FLOAT, args -> FLOAT(anyOptional(args))) //
        .impl(averageImplFactory(false)) //
        .build();

    /** The mean of multiple numbers */
    public static final ExpressionFunction NAN_AVERAGE = functionBuilder() //
        .name("nanaverage") //
        .description("""
                The mean value of a list of numbers, ignoring NaNs. If any argument
                is `MISSING`, the result is also `MISSING`. At least two arguments
                are required, but beyond that you can supply as many as you like.

                If _all_ arguments are NaN, the result is also NaN, and a warning
                will be emitted.

                Examples:
                * `nanaverage(2, 4, 6)` returns 4.0
                * `nanaverage(1, 2, 3, 4, 5)` returns 3.0
                * `nanaverage(1, 2, MISSING, 4, 5)` returns `MISSING`
                * `nanaverage(1, 2, NaN, 4, 5)` returns 3.0
                * `nanaverage(NaN, NaN, NaN, NaN)` returns NaN (with a warning)
                """) //
        .keywords("mean") //
        .category(CATEGORY.name()) //
        .args( //
            arg("input1", "first number", isNumericOrOpt()), //
            arg("input2", "second number", isNumericOrOpt()), //
            vararg("more", "additional numbers", isNumericOrOpt()) //
        ) //
        .returnType("the mean of the arguments", OPTIONAL_FLOAT, args -> FLOAT(anyOptional(args))) //
        .impl(averageImplFactory(true)) //
        .build();

    private static Function<List<Computer>, Computer> averageImplFactory(final boolean ignoreNan) {
        return args -> {
            ToDoubleFunction<WarningMessageListener> value = wml -> {
                // Because we use ::compute we need to do this inside the DoubleSupplier
                @SuppressWarnings("resource")
                var unfilteredFloatStream = args.stream().map(c -> toFloat(c).compute(wml)); //
                var filteredFloatStream = ignoreNan //
                    ? unfilteredFloatStream.filter(d -> !Double.isNaN(d)) //
                    : unfilteredFloatStream;

                return filteredFloatStream.mapToDouble(Double::valueOf).average().orElseGet(() -> {
                    // This can only run if we have an empty stream, i.e. if all values were NaN
                    // and got filtered out.
                    wml.addWarning("All arguments to nanaverage are NaN");

                    return Double.NaN;
                });
            };

            return FloatComputer.of(value, anyMissing(args));
        };
    }

    /** The median of multiple numbers */
    public static final ExpressionFunction MEDIAN = functionBuilder() //
        .name("median") //
        .description("The median value of a list of numbers") //
        .keywords() //
        .category(CATEGORY.name()) //
        .args( //
            arg("input1", "first number", isNumericOrOpt()), //
            arg("input2", "second number", isNumericOrOpt()), //
            vararg("more", "additional numbers", isNumericOrOpt()) //
        ) //
        .returnType("the median of the arguments", OPTIONAL_FLOAT, args -> FLOAT(anyOptional(args))) //
        .impl(medianImplFactory(false)) //
        .build();

    /** The median of multiple numbers, ignoring NaNs */
    public static final ExpressionFunction NAN_MEDIAN = functionBuilder() //
        .name("nanmedian") //
        .description("""
                The median value of a list of numbers, ignoring NaN values.
                If any argument is `MISSING`, the result is also `MISSING`.
                At least two arguments are required, but beyond that you can
                supply as many as you like.

                If _all_ arguments are NaN, the result is also NaN, and a warning
                will be emitted.

                Examples:
                * `nanmedian(2, 4, 6)` returns 4.0
                * `nanmedian(2, 4, 6, 1000)` returns 5.0
                * `nanmedian(1, 2, MISSING, 4, 5)` returns `MISSING`
                * `nanmedian(1, 2, NaN, 4, 5)` returns 3.0
                * `nanmedian(NaN, NaN, NaN, NaN)` returns NaN (with a warning)
                """) //
        .keywords() //
        .category(CATEGORY.name()) //
        .args( //
            arg("input1", "first number", isNumericOrOpt()), //
            arg("input2", "second number", isNumericOrOpt()), //
            vararg("more", "additional numbers", isNumericOrOpt()) //
        ) //
        .returnType("the median of the arguments", OPTIONAL_FLOAT, args -> FLOAT(anyOptional(args))) //
        .impl(medianImplFactory(true)) //
        .build();

    private static Function<List<Computer>, Computer> medianImplFactory(final boolean ignoreNan) {
        return args -> {
            ToDoubleFunction<WarningMessageListener> value = wml -> {
                // Because we use ::compute we need to do this inside the DoubleSupplier
                @SuppressWarnings("resource")
                var unfilteredFloatStream = args.stream().map(c -> toFloat(c).compute(wml)); //
                var filteredFloatStream = ignoreNan //
                    ? unfilteredFloatStream.filter(d -> !Double.isNaN(d)) //
                    : unfilteredFloatStream;
                var sortedFloatArgs = filteredFloatStream.sorted().toArray(Double[]::new);

                if (sortedFloatArgs.length == 0) {
                    // This can only run if we have an empty array, i.e. if all values were NaN
                    // and got filtered out.

                    wml.addWarning("All arguments to nanmedian are NaN");
                    return Double.NaN;
                } else if (Double.isNaN(sortedFloatArgs[sortedFloatArgs.length - 1])) {
                    // This can only run if we have an array with at least one NaN value and
                    // we are not ignoring NaNs.
                    //
                    // (works because 'sorted' considers NaN to be > any other element so it
                    // will always be last in the array)
                    return Double.NaN;
                } else if (sortedFloatArgs.length % 2 == 0) {
                    // Median is average of two middle elements if we have an even number of them
                    return 0.5 * (sortedFloatArgs[sortedFloatArgs.length / 2]
                        + sortedFloatArgs[sortedFloatArgs.length / 2 - 1]);
                } else {
                    // Median is middle element if we have an odd number of them
                    return sortedFloatArgs[sortedFloatArgs.length / 2];
                }
            };

            return FloatComputer.of(value, anyMissing(args));
        };
    }

    /** Binomial coefficient nCr of two numbers */
    public static final ExpressionFunction BINOMIAL = functionBuilder() //
        .name("binomial") //
        .description("Binomial coefficient nCr = n!/r!(n-r)!") //
        .keywords() //
        .category(CATEGORY.name()) //
        .args( //
            arg("n", "n", isIntegerOrOpt()), //
            arg("r", "r", isIntegerOrOpt()) //
        ) //
        .returnType("the binomial coefficient", "INTEGER?", args -> INTEGER(anyOptional(args))) //
        .impl(MathFunctions::binomialImpl) //
        .build();

    private static Computer binomialImpl(final List<Computer> args) {
        ToLongFunction<WarningMessageListener> value = wml -> {
            long n = toInteger(args.get(0)).compute(wml);
            long r = toInteger(args.get(1)).compute(wml);

            // 0c0 needs special handling
            if (n == 0 && r == 0) {
                return 1;
            }

            if (r > n) {
                wml.addWarning("invalid arguments to binomial (r > n)");
                return 0;
            }

            if (r < 0) {
                wml.addWarning("invalid arguments to binomial (r < 0)");
                return 0;
            }

            if (n < 0) {
                wml.addWarning("invalid arguments to binomial (n < 0)");
                return 0;
            }

            // nCr == nC(n-r), and our iterative algorithm is linear in r, so
            // if n-r < r, use that instead.
            r = (n - r) < r ? (n - r) : r;

            // Take advantage of the fact that (n+1)C(r+1) = (n/r)nCr, which
            // means we can start with (n-r)C0=1 and just multiply it by
            //
            // ([n]/[r])([n-1]/[k-1])([n-2]/[k-2])...([n-k+1]/[1])
            //
            // Except! If we switch the multiplication order of the denominator
            // (start with 1 instead of r) we can ensure that everything is an
            // integer the whole way, so no floating point. Nice!
            long ret = 1;

            for (int d = 1; d <= r; d++) {
                ret *= n;
                ret /= d;

                --n;
            }

            return ret;
        };

        return IntegerComputer.of(value, anyMissing(args));
    }

    // ======================= UTILITIES ==============================

    private static IntegerComputer toInteger(final Computer computer) {
        if (computer instanceof IntegerComputer i) {
            return i;
        }
        throw FunctionUtils.calledWithIllegalArgs();
    }

    private static boolean isNearZero(final double d) {
        return Math.abs(d) < 2 * Double.MIN_VALUE;
    }

    private enum ExtremumType {
            MAXIMUM, MINIMUM
    }
}
