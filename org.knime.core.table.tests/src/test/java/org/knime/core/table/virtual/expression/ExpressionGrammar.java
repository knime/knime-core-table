package org.knime.core.table.virtual.expression;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.nio.file.Paths;
import java.util.Scanner;

import org.knime.core.table.virtual.expression.AstNode.AstBinaryOp;
import org.knime.core.table.virtual.expression.AstNode.AstColumnRef;
import org.knime.core.table.virtual.expression.AstNode.AstIntConst;
import org.knime.core.table.virtual.expression.AstNode.AstUnaryOp;
import org.rekex.annomacro.AnnoMacro;
import org.rekex.helper.anno.Ch;
import org.rekex.helper.anno.StrWs;
import org.rekex.helper.datatype.SepBy1;
import org.rekex.parser.ParseResult;
import org.rekex.parser.PegParser;
import org.rekex.parser.PegParserBuilder;
import org.rekex.spec.Regex;

public interface ExpressionGrammar {

	record Expr(AstNode astNode) {}
	record Sum(AstNode astNode) {}
	record Term(AstNode astNode) {}
	record Factor(AstNode astNode) {}
	record Atom(AstNode astNode) {}

	static AstNode.BinaryOp binaryOp(String op) {
		return switch (op) {
			case "+" -> AstNode.BinaryOp.ADD;
			case "-" -> AstNode.BinaryOp.SUB;
			case "*" -> AstNode.BinaryOp.MUL;
			case "/" -> AstNode.BinaryOp.DIV;
			case "%" -> AstNode.BinaryOp.MOD;
			default -> throw new IllegalArgumentException();
		};
	}

    class CtorCatalog
    {
//		expression:
//			| sum
		public Expr expr(Sum s) {
			return new Expr(s.astNode);
		}

//		sum:
//			| a=sum '+' b=term { "add", a, b }
//			| a=sum '-' b=term { "sub", a, b }
//			| term
        public Sum sum(SepBy1<Term, @Ch("+-")String> terms) {
            return new Sum(
                    terms.reduce(
                            f1 -> op -> f2 -> new Term(
                                    new AstBinaryOp(f1.astNode, f2.astNode, binaryOp(op)))
                    ).astNode);
        }

//		term:
//			| a=term '*' b=factor { "mul", a, b }
//			| a=term '/' b=factor { "div", a, b }
//			| factor
		public Term term(SepBy1<Factor, @Ch("*/%")String> factors) {
			return new Term(
					factors.reduce(
							f1 -> op -> f2 -> new Factor(
									new AstBinaryOp(f1.astNode, f2.astNode, binaryOp(op)))
					).astNode);
		}

//		factor (memo):
//			| '+' a=factor { a }
//			| '-' a=factor { "neg", a }
//			| atom
		public Factor factor(OptWs ws, @Ch("+-")String op, Factor f) {
			if ( op.equals("-"))
				return new Factor(new AstUnaryOp(f.astNode, AstNode.UnaryOp.NEG));
			else
				return new Factor(f.astNode);
		}

		public Factor factor(OptWs ws, Atom a) {
			return new Factor(a.astNode);
		}

//		atom:
//			| a=NUMBER { helpers.num(a) }
//			| column
//			| group
//
//		column:
//			| '$' + a=NAME { "col", a[1] }
//			| '$' + '[' + a=STRING + ']' { "col", a[0][1][1:-1] }
//
//		group:
//			| '(' a=expression ')' { a }

		// TODO: float numbers

        public Atom intconst(OptWs ws, @Regex("[0-9]+")String str, OptWs trailingWs) {
            return new Atom(new AstIntConst(Integer.parseInt(str)));
        }

        public Atom column(OptWs ws, @Ch("$")Void h, @Ch("[")Void ob, String columnName, @Ch("]")Void cb, OptWs trailingWs) {
            return new Atom(new AstColumnRef(columnName));
        }

        public Atom column(OptWs ws, @Ch("$")Void h, @Ch(range={0x20, 0x10FFFF}, except=BS+QT+wsChars+"+-*/%") int[] chars, OptWs trailingWs)
        {
            return new Atom(new AstColumnRef(new String(chars, 0, chars.length)));
        }

		public Atom group(OptWs ws, @Ch("(")Void ob, Expr expr, @Ch(")")Void cb, OptWs trailingWs) {
			return new Atom(expr.astNode);
		}


		// (copied from ExampleParser_Json3)
        // string ........................

        static final String QT = "\"";
        static final String BS = "\\";

        // un-annotated primitives as datatypes that mirror grammar symbols
        //    `int` represents a logical json character in strings.
        //   `byte` represents a hex char.
        // this can be quite confusing to casual observers.

        public String string(@Ch(QT)Void QL, int[] chars, @Ch(QT)Void QR, OptWs trailingWs)
        {
            return new String(chars, 0, chars.length);
        }

        // unescape char
        public int char1(@Ch(range={0x20, 0x10FFFF}, except=BS+QT) int c)
        {
            return c;
        }

        final static String escN = BS+QT+"/bfnrt";
        final static String escV = BS+QT+"/\b\f\n\r\t";

        // escaped char: \b etc.
        public int escC(@Ch(BS)Void BSL, @Ch(escN) char c)
        {
            int i = escN.indexOf(c);
            assert i!=-1;
            return escV.charAt(i);
        }

        // escaped char: \u1234
        public int escU(@Ch(BS)Void BSL, @Ch("u")Void U, byte h1, byte h2, byte h3, byte h4)
        {
            return (h1<<12) | (h2<<8) | (h3<<4) | (h4) ;
        }

        public byte hex(@Regex("[0-9A-Fa-f]")char h)
        {
            // ascii order:  0..9..A..Z..a..z
            if(h<='9') return (byte)(h-'0');
            if(h>='a') return (byte)(h-'a'+10);
            return (byte)(h-'A'+10); // 0 - 0xF
        }
    }

    // whitespaces ----------------------------------------------------

    String wsChars = " \t\n\r";

    // equivalent to @StrWs, with default whitespace chars
    @Target(ElementType.TYPE_USE) @Retention(RetentionPolicy.RUNTIME) @interface Word {
        String[] value();

        AnnoMacro<Word, StrWs> toStrWs = StrWs.Macro.of(Word::value, wsChars);
    }

    // zero or more whitespaces
    enum OptWs {@Word("") I}





    // --- testing ------------------------------------------------------------


    static void genJava(String srcDir) throws Exception {
                new PegParserBuilder()
                        .rootType(Expr.class)
                        .catalogClass(CtorCatalog.class)
                        .packageName("org.knime.core.table.virtual.expression")
                        .className("ExpressionParser")
                        .outDirForJava(Paths.get(srcDir))
                        .logger(System.out::println)
                        .generateJavaFile();
    }

    static void main(String[] args) throws Exception {
        // this works if the main method is invoked from IntelliJ IDE
        final String srcDir = "src/test/java";
//        System.out.println(Paths.get(srcDir).toAbsolutePath());
        genJava(srcDir);
        testInputs("Expr", parser(Expr.class));
    }

    static <T> PegParser<T> parser(Class<T> klass) {
        return new PegParserBuilder()
                .rootType(klass)
                .catalogClass(CtorCatalog.class)
//                .logGrammar(System.out::println)
                .build(new CtorCatalog());
    }

    static <T> void testInputs(String desc, PegParser<T> parser)
    {
        System.out.println("## testing parser for "+desc);
        var scanner = new Scanner(System.in);
        while(true)
        {
            System.out.println("> input a test string; or 'exit' to exit.");
            if(!scanner.hasNextLine())
                break;
            String line=scanner.nextLine();
            if(line.equals("exit"))
                break;

            var result = parser.parse(line);
            if(result instanceof ParseResult.Full<T> full)
            {
                var value = full.value();
                System.out.println("## parsing succeeded; value: " + value);
            }
            else
            {
                System.out.println("## parsing failed; result: "+result);
                if(result instanceof ParseResult.Fatal<T> fatal)
                    fatal.cause().printStackTrace();
            }
        }
    }
}
