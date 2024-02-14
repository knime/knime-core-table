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
 *   Feb 13, 2024 (benjamin): created
 */
package org.knime.core.expressions;

import java.util.List;
import java.util.Map;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.knime.core.expressions.Ast.BinaryOperator;
import org.knime.core.expressions.Expressions.SyntaxError;
import org.knime.core.expressions.antlr.KnimeExpressionBaseVisitor;
import org.knime.core.expressions.antlr.KnimeExpressionLexer;
import org.knime.core.expressions.antlr.KnimeExpressionParser;
import org.knime.core.expressions.antlr.KnimeExpressionParser.BinaryOpContext;
import org.knime.core.expressions.antlr.KnimeExpressionParser.FunctionCallContext;
import org.knime.core.expressions.antlr.KnimeExpressionParser.ParenthesisedExprContext;

/**
 * Expression language parser that makes use of a ANTLR4 generated parser.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
final class Parser {

    private Parser() {
    }

    private static final ThrowingErrorListener ERROR_LISTENER = new ThrowingErrorListener();

    private static final ExpressionToAstVisitor EXPRESSION_TO_AST_VISITOR = new ExpressionToAstVisitor();

    static Ast parse(final String expression) throws SyntaxError {
        try {
            var lexer = new KnimeExpressionLexer(CharStreams.fromString(expression));
            var parser = new KnimeExpressionParser(new CommonTokenStream(lexer));
            parser.removeErrorListeners();
            parser.addErrorListener(ERROR_LISTENER);
            return parser.fullExpr().accept(EXPRESSION_TO_AST_VISITOR);
        } catch (RuntimeSyntaxError ex) { // NOSONAR: RuntimeSyntaxError is just a wrapper
            throw ex.m_cause;
        }
    }

    /** Can be set for the parser to handle errors */
    private static final class ThrowingErrorListener extends BaseErrorListener {
        @Override
        public void syntaxError(final Recognizer<?, ?> recognizer, final Object offendingSymbol, final int line,
            final int charPositionInLine, final String msg, final RecognitionException e) {
            throw new RuntimeSyntaxError(new Expressions.SyntaxError(msg));
        }
    }

    /** Wrapper for a {@link SyntaxError} that is */
    private static final class RuntimeSyntaxError extends RuntimeException {
        private static final long serialVersionUID = 1L;

        final SyntaxError m_cause;

        RuntimeSyntaxError(final SyntaxError cause) {
            m_cause = cause;
        }
    }

    private static final class ExpressionToAstVisitor extends KnimeExpressionBaseVisitor<Ast> {

        @Override
        public Ast visitBinaryOp(final BinaryOpContext ctx) {
            var arg1 = ctx.getChild(0).accept(this);
            var arg2 = ctx.getChild(2).accept(this);
            var op = mapBinaryOperator(ctx.op);
            return new Ast.BinaryOp(op, arg1, arg2, Map.of());
        }

        @Override
        public Ast visitFunctionCall(final FunctionCallContext ctx) {
            var name = ctx.name.getText();
            var argsContext = ctx.functionArgs();
            var args = //
                argsContext == null //
                    ? List.<Ast> of() //
                    : argsContext.expr().stream().map(a -> a.accept(this)).toList();
            return new Ast.FunctionCall(name, args, Map.of());
        }

        @Override
        public Ast visitParenthesisedExpr(final ParenthesisedExprContext ctx) {
            // Ignore the parentheses terminal symbols
            return ctx.inner.accept(this);
        }

        @Override
        public Ast visitErrorNode(final ErrorNode node) {
            throw new RuntimeSyntaxError(new SyntaxError(node.getText()));
        }

        @Override
        public Ast visitTerminal(final TerminalNode node) {
            var symbol = node.getSymbol();
            return switch (symbol.getType()) {
                case KnimeExpressionParser.BOOLEAN: {
                    var value = Boolean.parseBoolean(symbol.getText());
                    yield new Ast.BooleanConstant(value, Map.of());
                }
                case KnimeExpressionParser.INTEGER: {
                    var value = Long.parseLong(symbol.getText().replace("_", ""));
                    yield new Ast.IntegerConstant(value, Map.of());
                }
                case KnimeExpressionParser.FLOAT: {
                    var value = Double.parseDouble(symbol.getText().replace("_", ""));
                    yield new Ast.FloatConstant(value, Map.of());
                }
                case Recognizer.EOF: {
                    yield null;
                }
                default:
                    // TODO how to handle?
                    throw new IllegalArgumentException("Unexpected value: " + symbol);
            };
        }

        @Override
        protected Ast aggregateResult(final Ast aggregate, final Ast nextResult) {
            // next result is null for EOF
            return nextResult == null ? aggregate : nextResult;
        }

        /** Maps a token for a binary operation to the Ast operator */
        private static BinaryOperator mapBinaryOperator(final Token op) {
            return switch (op.getType()) {
                case KnimeExpressionParser.PLUS -> BinaryOperator.PLUS;
                case KnimeExpressionParser.MINUS -> BinaryOperator.MINUS;
                case KnimeExpressionParser.MULTIPLY -> BinaryOperator.MULTIPLY;
                case KnimeExpressionParser.DIVIDE -> BinaryOperator.DIVIDE;
                case KnimeExpressionParser.FLOOR_DIVIDE -> BinaryOperator.FLOOR_DIVIDE;
                case KnimeExpressionParser.EXPONENTIATE -> BinaryOperator.EXPONENTIAL;
                case KnimeExpressionParser.MODULO -> BinaryOperator.REMAINDER;
                // TODO how to handle?
                default -> throw new IllegalArgumentException("Unexpected value: " + op.getType());
            };
        }
    }
}