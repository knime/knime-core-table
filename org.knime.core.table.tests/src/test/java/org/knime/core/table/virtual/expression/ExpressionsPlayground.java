package org.knime.core.table.virtual.expression;

import org.knime.core.table.virtual.expression.AstNode.AstBinaryOp;
import org.knime.core.table.virtual.expression.AstNode.AstColumnRef;
import org.knime.core.table.virtual.expression.AstNode.AstIntConst;
import org.knime.core.table.virtual.expression.AstNode.AstUnaryOp;

public class ExpressionsPlayground {
    public static void main(String[] args) throws Exception {
        final String input = "$col1 * ($[\"my awkwardly named \\\"column\\\"\"] + 123        ) % 2";
//        final String input = "3 * $col1";
        System.out.println("input = " + input);

        final ExpressionParser parser = new ExpressionParser(new ExpressionGrammar.CtorCatalog());
        final AstNode ast = parser.matchFull(input).astNode();
        System.out.println("ast = " + ast);
//        ParseResult<Expr> result = parser.parse(input);
//        System.out.println("result = " + result);
        System.out.println(toString(ast));
    }

    static String toString(AstNode ast) {
        var sb = new StringBuilder();
        toString(sb, 0, ast);
        return sb.toString();
    }

    private static void toString(StringBuilder sb, int level, AstNode astNode) {
        if (astNode instanceof AstIntConst n) {
            indent(sb, level).append(n.value()).append("\n");
        } else if (astNode instanceof AstColumnRef n) {
            indent(sb, level).append("$["+n.name()+"]").append("\n");
        } else if (astNode instanceof AstBinaryOp n) {
            indent(sb, level).append(n.op()).append("\n");
            toString(sb, level + 1, n.arg1());
            toString(sb, level + 1, n.arg2());
        } else if (astNode instanceof AstUnaryOp n) {
            indent(sb, level).append(n.op()).append("\n");
            toString(sb, level + 1, n.arg());
        }
    }

    private static StringBuilder indent(StringBuilder sb, int level) {
        for (int i = 0; i < level; i++)
            sb.append("  ");
        return sb;
    }
}
