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
