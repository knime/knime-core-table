package org.knime.core.expressions.functions;

import static org.knime.core.expressions.ReturnTypeDescriptions.RETURN_LOCAL_TIME;
import static org.knime.core.expressions.SignatureUtils.arg;
import static org.knime.core.expressions.SignatureUtils.isStringOrOpt;
import static org.knime.core.expressions.SignatureUtils.optarg;
import static org.knime.core.expressions.ValueType.LOCAL_TIME;
import static org.knime.core.expressions.functions.ExpressionFunctionBuilder.anyMissing;
import static org.knime.core.expressions.functions.ExpressionFunctionBuilder.anyOptional;
import static org.knime.core.expressions.functions.ExpressionFunctionBuilder.functionBuilder;

import java.time.LocalTime;
import java.time.chrono.Chronology;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Locale;
import java.util.function.Function;

import org.knime.core.expressions.Arguments;
import org.knime.core.expressions.Computer;
import org.knime.core.expressions.Computer.LocalTimeComputer;
import org.knime.core.expressions.Computer.StringComputer;
import org.knime.core.expressions.EvaluationContext;
import org.knime.core.expressions.OperatorCategory;

/**
 * Implementation of built-in functions that manipulate date and times.
 *
 * @author Tobias Kampmann, TNG
 */
@SuppressWarnings("javadoc")
public final class DateTimeFunctions {

    private DateTimeFunctions() {
    }

    private static final String DATE_TIME_META_CATEGORY_NAME = "Date&Time";

    /** The "DateTime – General" category */
    public static final OperatorCategory CATEGORY_GENERAL =
        new OperatorCategory(DATE_TIME_META_CATEGORY_NAME, "General", """
                The "Date&Time – General" category in KNIME Expression language includes functions for manipulating and
                converting Date&Time data.
                """);

    public static final ExpressionFunction TIME = functionBuilder() //
        .name("time") //
        .description("""
                The node parses String values into Time values.
                """) //
        .examples("""
                * `time("12:12")` returns LocalTime(12, 12)
                """) //
        .keywords("match", "equals") //
        .category(CATEGORY_GENERAL) //
        .args( //
            arg("string", "String to parse to a time", isStringOrOpt()), //
            arg("format", "Format of the time string", isStringOrOpt()), //
            optarg("locale", "Locale to use for parsing the time string", isStringOrOpt()) //
        ) //
        .returnType("A local time", RETURN_LOCAL_TIME, args -> LOCAL_TIME(anyOptional(args)))//
        .impl(DateTimeFunctions::parseTime) //
        .build();

    private static Computer parseTime(final Arguments<Computer> args) {
        var string = toString(args.get("string"));

        return LocalTimeComputer.of( //
            ctx -> {
                var formatter = createFormatter(args).apply(ctx);
                return formatter.parse(string.compute(ctx), LocalTime::from);
            }, //
            ctx -> {
                if (anyMissing(args).applyAsBoolean(ctx)) {
                    return true;
                }
                var formatter = createFormatter(args).apply(ctx);
                try {
                    formatter.parse(string.compute(ctx), LocalTime::from);
                    return false;
                } catch (DateTimeParseException e) {
                    return true;
                }
            });
    }

    // ======================= UTILITIES ==============================

    private static Function<EvaluationContext, DateTimeFormatter> createFormatter(final Arguments<Computer> args) {
        return ctx -> {
            var locale = args.has("locale") //
                ? Locale.forLanguageTag(toString(args.get("locale")).compute(ctx)) //
                : Locale.ENGLISH;
            System.out.println("Locale: " + locale);
            System.out.println("Format: " + toString(args.get("format")).compute(ctx));
            return DateTimeFormatter.ofPattern(toString(args.get("format")).compute(ctx), locale)
                .withChronology(Chronology.ofLocale(locale));
        };
    }

    private static StringComputer toString(final Computer c) {
        if (c instanceof StringComputer sc) {
            return sc;
        }

        throw FunctionUtils.calledWithIllegalArgs();
    }
}