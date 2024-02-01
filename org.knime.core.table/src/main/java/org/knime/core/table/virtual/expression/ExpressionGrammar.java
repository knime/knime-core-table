package org.knime.core.table.virtual.expression;

import static org.knime.core.table.virtual.expression.Ast.BinaryOp.Operator.CONDITIONAL_AND;
import static org.knime.core.table.virtual.expression.Ast.BinaryOp.Operator.CONDITIONAL_OR;
import static org.knime.core.table.virtual.expression.Ast.BinaryOp.Operator.DIVIDE;
import static org.knime.core.table.virtual.expression.Ast.BinaryOp.Operator.EQUAL_TO;
import static org.knime.core.table.virtual.expression.Ast.BinaryOp.Operator.GREATER_THAN;
import static org.knime.core.table.virtual.expression.Ast.BinaryOp.Operator.GREATER_THAN_EQUAL;
import static org.knime.core.table.virtual.expression.Ast.BinaryOp.Operator.LESS_THAN;
import static org.knime.core.table.virtual.expression.Ast.BinaryOp.Operator.LESS_THAN_EQUAL;
import static org.knime.core.table.virtual.expression.Ast.BinaryOp.Operator.MINUS;
import static org.knime.core.table.virtual.expression.Ast.BinaryOp.Operator.MULTIPLY;
import static org.knime.core.table.virtual.expression.Ast.BinaryOp.Operator.NOT_EQUAL_TO;
import static org.knime.core.table.virtual.expression.Ast.BinaryOp.Operator.PLUS;
import static org.knime.core.table.virtual.expression.Ast.BinaryOp.Operator.REMAINDER;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Paths;
import java.util.Scanner;

import org.rekex.annomacro.AnnoMacro;
import org.rekex.helper.anno.Ch;
import org.rekex.helper.anno.Str;
import org.rekex.helper.anno.StrWs;
import org.rekex.helper.datatype.SepBy;
import org.rekex.helper.datatype.SepBy1;
import org.rekex.parser.ParseResult;
import org.rekex.parser.PegParser;
import org.rekex.parser.PegParserBuilder;
import org.rekex.spec.Regex;

public interface ExpressionGrammar {

    String genPackageName = "org.knime.core.table.virtual.expression";

    String genClassName = "ExpressionParser";

    static PegParser<Expr> parser() {
        try {
            var klass = Class.forName(genPackageName + "." + genClassName);
            var constructor = klass.getConstructor(CtorCatalog.class);
            return (PegParser)constructor.newInstance(new CtorCatalog());
        } catch (ClassNotFoundException e) {
            return parser(Expr.class);
        } catch (NoSuchMethodException | InvocationTargetException | InstantiationException
                | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    record Expr(Ast.Node ast) {
    }

    record Disjunction(Ast.Node ast) {
    }

    record Conjunction(Ast.Node ast) {
    }

    record Inversion(Ast.Node ast) {
    }

    record Comparison(Ast.Node ast) {
    }

    record Sum(Ast.Node ast) {
    }

    record Term(Ast.Node ast) {
    }

    record Factor(Ast.Node ast) {
    }

    record Atom(Ast.Node ast) {
    }

    record StrConst(String value) {
    }

    record Digits(String value) {
    }

    record OptWs(String value) {
    }

    class CtorCatalog {
        private static Ast.BinaryOp.Operator binaryOperator(final String symbol) {
            return switch (symbol) {
                case "+" -> PLUS;
                case "-" -> MINUS;
                case "*" -> MULTIPLY;
                case "/" -> DIVIDE;
                case "%" -> REMAINDER;
                case "==", "=" -> EQUAL_TO;
                case "!=" -> NOT_EQUAL_TO;
                case "<" -> LESS_THAN;
                case "<=" -> LESS_THAN_EQUAL;
                case ">" -> GREATER_THAN;
                case ">=" -> GREATER_THAN_EQUAL;
                case "and" -> CONDITIONAL_AND;
                case "or" -> CONDITIONAL_OR;
                default -> throw new IllegalArgumentException("unknown binary operator: \"" + symbol + "\"");
            };
        }

        //  expression:
        //      | disjunction
        public Expr expr(final Disjunction d) {
            return new Expr(d.ast);
        }

        //  disjunction:
        //      | a=conjunction 'or' b=disjunction { "or", a, b }
        //      | conjunction
        public Disjunction disjunction(final SepBy1<Conjunction, @Str("or") String> conjunctions) {
            return new Disjunction(conjunctions
                .reduce(c1 -> op -> c2 -> new Conjunction(new Ast.BinaryOp(c1.ast, c2.ast, CONDITIONAL_OR))).ast);
        }

        //  conjunction:
        //      | a=inversion 'and' b=conjunction { "and", a, b }
        //      | inversion
        public Conjunction conjunction(final SepBy1<Inversion, @Str("and") String> inversions) {
            return new Conjunction(inversions
                .reduce(i1 -> op -> i2 -> new Inversion(new Ast.BinaryOp(i1.ast, i2.ast, CONDITIONAL_AND))).ast);
        }

        //  inversion:
        //      | 'not' a=inversion { "not", a }
        //      | comparison
        public Inversion inversion(final OptWs ws, @Str("not") final String op, final Comparison c) {
            return new Inversion(new Ast.UnaryOp(c.ast, Ast.UnaryOp.Operator.NOT));
        }

        public Inversion inversion(final Comparison c) {
            return new Inversion(c.ast);
        }

        //  comparison:
        //      | a=sum '==' b=sum { "==", a, b }
        //      | a=sum '=' b=sum { "=", a, b }
        //      | a=sum '!=' b=sum { "!=", a, b }
        //      | a=sum '<=' b=sum { "<=", a, b }
        //      | a=sum '<' b=sum { "<", a, b }
        //      | a=sum '>=' b=sum { ">=", a, b }
        //      | a=sum '>' b=sum { ">", a, b }
        //      | sum
        public Comparison comparison(final Sum s1, @Str({"==", "=", "!=", "<=", "<", ">=", ">"}) final String op,
            final Sum s2) {
            return new Comparison(new Ast.BinaryOp(s1.ast, s2.ast, binaryOperator(op)));
        }

        public Comparison comparison(final Sum s) {
            return new Comparison(s.ast);
        }

        //  sum:
        //      | sum '+' term
        //      | sum '-' term
        //      | term
        public Sum sum(final SepBy1<Term, @Ch("+-") String> terms) {
            return new Sum(
                terms.reduce(f1 -> op -> f2 -> new Term(new Ast.BinaryOp(f1.ast, f2.ast, binaryOperator(op)))).ast);
        }

        //  term:
        //      | term '*' factor
        //      | term '/' factor
        //      | term '%' factor
        //      | factor
        public Term term(final SepBy1<Factor, @Ch("*/%") String> factors) {
            return new Term(
                factors.reduce(f1 -> op -> f2 -> new Factor(new Ast.BinaryOp(f1.ast, f2.ast, binaryOperator(op)))).ast);
        }

        //  factor (memo):
        //      | '+' factor
        //      | '-' factor
        //      | atom
        public Factor factor(final OptWs ws, @Ch("+-") final String op, final Factor f) {
            if (op.equals("-")) {
                return new Factor(new Ast.UnaryOp(f.ast, Ast.UnaryOp.Operator.MINUS));
            } else {
                return new Factor(f.ast);
            }
        }

        public Factor factor(final OptWs ws, final Atom a) {
            return new Factor(a.ast);
        }

        //  atom:
        //      | column
        //      | call
        //      | group
        //      | float_literal
        //      | int_literal
        //      | string_literal
        //
        //  column:
        //      | '$' + NAME
        //      | '$' + '[' + STRING + ']'
        //      | '$' + '[' + INTEGER + ']'
        public Atom column(final OptWs ws, @Ch("$") final Void h, @Ch("[") final Void ob, final StrConst columnName,
            @Ch("]") final Void cb, final OptWs trailingWs) {
            return new Atom(new Ast.ColumnRef(columnName.value));
        }

        public Atom column(final OptWs ws, @Ch("$") final Void h, @Ch("[") final Void ob,
            @Regex("[0-9]+") final String columnIndex, @Ch("]") final Void cb, final OptWs trailingWs) {
            return new Atom(new Ast.ColumnIndex(Integer.parseInt(columnIndex)));
        }

        public Atom column(final OptWs ws, @Ch("$") final Void h,
            @Ch(range = {0x20, 0x10FFFF}, except = BS + QT + wsChars + "$()+-*/%") final int[] chars,
            final OptWs trailingWs) {
            return new Atom(new Ast.ColumnRef(new String(chars, 0, chars.length)));
        }

        //  call:
        //      | a=NAME + '(' + b=arguments + ')' { "call", a[0][1], *b }
        //
        //  arguments:
        //      | a=expression ',' b=arguments { a, b }
        //      | a=expression
        public Atom call(final OptWs ws, @Regex("[a-zA-Z_]\\w*") final String func, final OptWs wsob,
            @Ch("(") final Void ob, final SepBy<Expr, @Ch(",") String> arguments, @Ch(")") final Void cb,
            final OptWs trailingWs) {
            // TODO: What is the definition for legal identifiers in KNIME Expression Language?
            //       The above RegEx is too simplistic, probably.
            return new Atom(new Ast.Call(func, arguments.values().stream().map(Expr::ast).toList()));
        }

        //  group:
        //      | '(' a=expression ')'
        public Atom group(final OptWs ws, @Ch("(") final Void ob, final Expr expr, @Ch(")") final Void cb,
            final OptWs trailingWs) {
            return new Atom(expr.ast);
        }

        //  float_literal:
        //      | FLOAT
        public Atom float_literal(final OptWs ws, final Digits digits,
            @Regex("[.][0-9]*([e][+-]?[0-9]+)?[fFdD]?") final String str, final OptWs trailingWs) {
            return float_literal(digits.value + str);
        }

        public Atom float_literal(final OptWs ws, @Regex("[.][0-9]+([e][+-]?[0-9]+)?[fFdD]?") final String str,
            final OptWs trailingWs) {
            return float_literal(str);
        }

        private static Atom float_literal(final String str) {
            final Ast.FloatConstant ast = new Ast.FloatConstant(Double.parseDouble(str));
            ast.setInferredType(str.substring(str.length() - 1).equalsIgnoreCase("f") ? AstType.FLOAT : AstType.DOUBLE);
            return new Atom(ast);
        }

        //  int_literal:
        //      | INTEGER
        public Atom int_literal(final OptWs ws, final Digits digits, final OptWs trailingWs) {
            return new Atom(new Ast.IntConstant(Long.parseLong(digits.value)));
        }

        public Digits digits(@Regex("[0-9]+") final String str) {
            return new Digits(str);
        }

        //  string_literal:
        //      | STRING
        public Atom string_literal(final StrConst str) {
            return new Atom(new Ast.StringConstant(str.value));
        }

        // (copied from ExampleParser_Json3)
        // string ........................

        static final String QT = "\"";

        static final String BS = "\\";

        // un-annotated primitives as datatypes that mirror grammar symbols
        //    `int` represents a logical json character in strings.
        //   `byte` represents a hex char.
        // this can be quite confusing to casual observers.

        public StrConst string(@Ch(QT) final Void QL, final int[] chars, @Ch(QT) final Void QR,
            final OptWs trailingWs) {
            return new StrConst(new String(chars, 0, chars.length));
        }

        // unescape char
        public int char1(@Ch(range = {0x20, 0x10FFFF}, except = BS + QT) final int c) {
            return c;
        }

        final static String escN = BS + QT + "/bfnrt";

        final static String escV = BS + QT + "/\b\f\n\r\t";

        // escaped char: \b etc.
        public int escC(@Ch(BS) final Void BSL, @Ch(escN) final char c) {
            int i = escN.indexOf(c);
            assert i != -1;
            return escV.charAt(i);
        }

        // escaped char: \u1234
        public int escU(@Ch(BS) final Void BSL, @Ch("u") final Void U, final byte h1, final byte h2, final byte h3,
            final byte h4) {
            return (h1 << 12) | (h2 << 8) | (h3 << 4) | (h4);
        }

        public byte hex(@Regex("[0-9A-Fa-f]") final char h) {
            // ascii order:  0..9..A..Z..a..z
            if (h <= '9') {
                return (byte)(h - '0');
            }
            if (h >= 'a') {
                return (byte)(h - 'a' + 10);
            }
            return (byte)(h - 'A' + 10); // 0 - 0xF
        }

        // zero or more whitespaces
        public OptWs optWs(@Word("") final String ws) {
            return new OptWs(ws);
        }

    }

    // whitespaces ----------------------------------------------------

    String wsChars = " \t\n\r";

    // equivalent to @StrWs, with default whitespace chars
    @Target(ElementType.TYPE_USE)
    @Retention(RetentionPolicy.RUNTIME)
    @interface Word {
        String[] value();

        AnnoMacro<Word, StrWs> toStrWs = StrWs.Macro.of(Word::value, wsChars);
    }

    // --- testing ------------------------------------------------------------

    static void genJava(final String srcDir) throws Exception {
        new PegParserBuilder().rootType(Expr.class).catalogClass(CtorCatalog.class).packageName(genPackageName)
            .className(genClassName).outDirForJava(Paths.get(srcDir)).logger(System.out::println).generateJavaFile();
    }

    static void main(final String[] args) throws Exception {
        // this works if the main method is invoked from IntelliJ IDE
        final String srcDir = "src/main/java";
        //        System.out.println(Paths.get(srcDir).toAbsolutePath());
        genJava(srcDir);
        testInputs("Expr", parser(Expr.class));
    }

    static <T> PegParser<T> parser(final Class<T> klass) {
        return new PegParserBuilder().rootType(klass).catalogClass(CtorCatalog.class).logGrammar(System.out::println)
            .build(new CtorCatalog());
    }

    static <T> void testInputs(final String desc, final PegParser<T> parser) {
        System.out.println("## testing parser for " + desc);
        var scanner = new Scanner(System.in);
        while (true) {
            System.out.println("> input a test string; or 'exit' to exit.");
            if (!scanner.hasNextLine()) {
                break;
            }
            String line = scanner.nextLine();
            if (line.equals("exit")) {
                break;
            }

            var result = parser.parse(line);
            if (result instanceof ParseResult.Full<T> full) {
                var value = full.value();
                System.out.println("## parsing succeeded; value: " + value);
            } else {
                System.out.println("## parsing failed; result: " + result);
                if (result instanceof ParseResult.Fatal<T> fatal) {
                    fatal.cause().printStackTrace();
                }
            }
        }
    }
}
