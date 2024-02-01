package org.knime.core.table.virtual.expression;

import org.knime.core.table.virtual.expression.ExpressionGrammar.Expr;
import org.rekex.parser.PegParser;

public class ExpressionsPlayground {
    public static void main(final String[] args) throws Exception {
        final String input = "$col1 * ($[\"my awkwardly named \\\"column\\\"\"] + 123        ) % 2";
        // final String input = "3 * $col1";
        System.out.println("input = " + input);

        final PegParser<Expr> parser = ExpressionGrammar.parser();
        final Ast.Node ast = parser.matchFull(input).ast();
        System.out.println("ast = " + ast);
        // ParseResult<Expr> result = parser.parse(input);
        // System.out.println("result = " + result);
        System.out.println(toString(ast));
    }

    static String toString(final Ast.Node ast) {
        var sb = new StringBuilder();
        toString(sb, 0, ast);
        return sb.toString();
    }

    private static void toString(final StringBuilder sb, final int level, final Ast.Node astNode) {
        if (astNode instanceof Ast.IntConstant n) {
            indent(sb, level).append(n.value()).append("\n");
        } else if (astNode instanceof Ast.ColumnRef n) {
            indent(sb, level).append("$["+n.name()+"]").append("\n");
        } else if (astNode instanceof Ast.BinaryOp n) {
            indent(sb, level).append(n.op()).append("\n");
            toString(sb, level + 1, n.arg1());
            toString(sb, level + 1, n.arg2());
        } else if (astNode instanceof Ast.UnaryOp n) {
            indent(sb, level).append(n.op()).append("\n");
            toString(sb, level + 1, n.arg());
        }
    }

    private static StringBuilder indent(final StringBuilder sb, final int level) {
        for (int i = 0; i < level; i++) {
            sb.append("  ");
        }
        return sb;
    }
}
