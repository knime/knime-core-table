// Generated from KnimeExpression.g4 by ANTLR 4.13.1
package org.knime.core.expressions.antlr;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast", "CheckReturnValue"})
public class KnimeExpressionParser extends Parser {
	static { RuntimeMetaData.checkVersion("4.13.1", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__0=1, T__1=2, T__2=3, WHITESPACE=4, BOOLEAN=5, INTEGER=6, FLOAT=7, STRING=8, 
		MISSING=9, PLUS=10, MINUS=11, MULTIPLY=12, DIVIDE=13, FLOOR_DIVIDE=14, 
		EXPONENTIATE=15, MODULO=16, LESS_THAN=17, LESS_THAN_OR_EQUAL=18, GREATER_THAN=19, 
		GREATER_THAN_OR_EQUAL=20, EQUAL=21, NOT_EQUAL=22, AND=23, OR=24, NOT=25, 
		IDENTIFIER=26;
	public static final int
		RULE_fullExpr = 0, RULE_expr = 1, RULE_functionArgs = 2;
	private static String[] makeRuleNames() {
		return new String[] {
			"fullExpr", "expr", "functionArgs"
		};
	}
	public static final String[] ruleNames = makeRuleNames();

	private static String[] makeLiteralNames() {
		return new String[] {
			null, "'('", "')'", "','", null, null, null, null, null, "'MISSING'", 
			"'+'", "'-'", "'*'", "'/'", "'//'", "'**'", "'%'", "'<'", "'<='", "'>'", 
			"'>='", null, null, "'and'", "'or'", "'not'"
		};
	}
	private static final String[] _LITERAL_NAMES = makeLiteralNames();
	private static String[] makeSymbolicNames() {
		return new String[] {
			null, null, null, null, "WHITESPACE", "BOOLEAN", "INTEGER", "FLOAT", 
			"STRING", "MISSING", "PLUS", "MINUS", "MULTIPLY", "DIVIDE", "FLOOR_DIVIDE", 
			"EXPONENTIATE", "MODULO", "LESS_THAN", "LESS_THAN_OR_EQUAL", "GREATER_THAN", 
			"GREATER_THAN_OR_EQUAL", "EQUAL", "NOT_EQUAL", "AND", "OR", "NOT", "IDENTIFIER"
		};
	}
	private static final String[] _SYMBOLIC_NAMES = makeSymbolicNames();
	public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

	/**
	 * @deprecated Use {@link #VOCABULARY} instead.
	 */
	@Deprecated
	public static final String[] tokenNames;
	static {
		tokenNames = new String[_SYMBOLIC_NAMES.length];
		for (int i = 0; i < tokenNames.length; i++) {
			tokenNames[i] = VOCABULARY.getLiteralName(i);
			if (tokenNames[i] == null) {
				tokenNames[i] = VOCABULARY.getSymbolicName(i);
			}

			if (tokenNames[i] == null) {
				tokenNames[i] = "<INVALID>";
			}
		}
	}

	@Override
	@Deprecated
	public String[] getTokenNames() {
		return tokenNames;
	}

	@Override

	public Vocabulary getVocabulary() {
		return VOCABULARY;
	}

	@Override
	public String getGrammarFileName() { return "KnimeExpression.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public ATN getATN() { return _ATN; }

	public KnimeExpressionParser(TokenStream input) {
		super(input);
		_interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	@SuppressWarnings("CheckReturnValue")
	public static class FullExprContext extends ParserRuleContext {
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public TerminalNode EOF() { return getToken(KnimeExpressionParser.EOF, 0); }
		public FullExprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_fullExpr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KnimeExpressionListener ) ((KnimeExpressionListener)listener).enterFullExpr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KnimeExpressionListener ) ((KnimeExpressionListener)listener).exitFullExpr(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof KnimeExpressionVisitor ) return ((KnimeExpressionVisitor<? extends T>)visitor).visitFullExpr(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FullExprContext fullExpr() throws RecognitionException {
		FullExprContext _localctx = new FullExprContext(_ctx, getState());
		enterRule(_localctx, 0, RULE_fullExpr);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(6);
			expr(0);
			setState(7);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ExprContext extends ParserRuleContext {
		public ExprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_expr; }
	 
		public ExprContext() { }
		public void copyFrom(ExprContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ParenthesisedExprContext extends ExprContext {
		public ExprContext inner;
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public ParenthesisedExprContext(ExprContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KnimeExpressionListener ) ((KnimeExpressionListener)listener).enterParenthesisedExpr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KnimeExpressionListener ) ((KnimeExpressionListener)listener).exitParenthesisedExpr(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof KnimeExpressionVisitor ) return ((KnimeExpressionVisitor<? extends T>)visitor).visitParenthesisedExpr(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class BinaryOpContext extends ExprContext {
		public Token op;
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public TerminalNode EXPONENTIATE() { return getToken(KnimeExpressionParser.EXPONENTIATE, 0); }
		public TerminalNode MULTIPLY() { return getToken(KnimeExpressionParser.MULTIPLY, 0); }
		public TerminalNode DIVIDE() { return getToken(KnimeExpressionParser.DIVIDE, 0); }
		public TerminalNode MODULO() { return getToken(KnimeExpressionParser.MODULO, 0); }
		public TerminalNode FLOOR_DIVIDE() { return getToken(KnimeExpressionParser.FLOOR_DIVIDE, 0); }
		public TerminalNode PLUS() { return getToken(KnimeExpressionParser.PLUS, 0); }
		public TerminalNode MINUS() { return getToken(KnimeExpressionParser.MINUS, 0); }
		public BinaryOpContext(ExprContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KnimeExpressionListener ) ((KnimeExpressionListener)listener).enterBinaryOp(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KnimeExpressionListener ) ((KnimeExpressionListener)listener).exitBinaryOp(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof KnimeExpressionVisitor ) return ((KnimeExpressionVisitor<? extends T>)visitor).visitBinaryOp(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class FunctionCallContext extends ExprContext {
		public Token name;
		public TerminalNode IDENTIFIER() { return getToken(KnimeExpressionParser.IDENTIFIER, 0); }
		public FunctionArgsContext functionArgs() {
			return getRuleContext(FunctionArgsContext.class,0);
		}
		public FunctionCallContext(ExprContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KnimeExpressionListener ) ((KnimeExpressionListener)listener).enterFunctionCall(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KnimeExpressionListener ) ((KnimeExpressionListener)listener).exitFunctionCall(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof KnimeExpressionVisitor ) return ((KnimeExpressionVisitor<? extends T>)visitor).visitFunctionCall(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class AtomContext extends ExprContext {
		public TerminalNode BOOLEAN() { return getToken(KnimeExpressionParser.BOOLEAN, 0); }
		public TerminalNode INTEGER() { return getToken(KnimeExpressionParser.INTEGER, 0); }
		public TerminalNode FLOAT() { return getToken(KnimeExpressionParser.FLOAT, 0); }
		public TerminalNode STRING() { return getToken(KnimeExpressionParser.STRING, 0); }
		public TerminalNode MISSING() { return getToken(KnimeExpressionParser.MISSING, 0); }
		public AtomContext(ExprContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KnimeExpressionListener ) ((KnimeExpressionListener)listener).enterAtom(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KnimeExpressionListener ) ((KnimeExpressionListener)listener).exitAtom(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof KnimeExpressionVisitor ) return ((KnimeExpressionVisitor<? extends T>)visitor).visitAtom(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ExprContext expr() throws RecognitionException {
		return expr(0);
	}

	private ExprContext expr(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		ExprContext _localctx = new ExprContext(_ctx, _parentState);
		ExprContext _prevctx = _localctx;
		int _startState = 2;
		enterRecursionRule(_localctx, 2, RULE_expr, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(25);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case IDENTIFIER:
				{
				_localctx = new FunctionCallContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(10);
				((FunctionCallContext)_localctx).name = match(IDENTIFIER);
				setState(11);
				match(T__0);
				setState(13);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 67109858L) != 0)) {
					{
					setState(12);
					functionArgs();
					}
				}

				setState(15);
				match(T__1);
				}
				break;
			case T__0:
				{
				_localctx = new ParenthesisedExprContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(16);
				match(T__0);
				setState(17);
				((ParenthesisedExprContext)_localctx).inner = expr(0);
				setState(18);
				match(T__1);
				}
				break;
			case BOOLEAN:
				{
				_localctx = new AtomContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(20);
				match(BOOLEAN);
				}
				break;
			case INTEGER:
				{
				_localctx = new AtomContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(21);
				match(INTEGER);
				}
				break;
			case FLOAT:
				{
				_localctx = new AtomContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(22);
				match(FLOAT);
				}
				break;
			case STRING:
				{
				_localctx = new AtomContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(23);
				match(STRING);
				}
				break;
			case MISSING:
				{
				_localctx = new AtomContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(24);
				match(MISSING);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			_ctx.stop = _input.LT(-1);
			setState(38);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,3,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(36);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,2,_ctx) ) {
					case 1:
						{
						_localctx = new BinaryOpContext(new ExprContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_expr);
						setState(27);
						if (!(precpred(_ctx, 9))) throw new FailedPredicateException(this, "precpred(_ctx, 9)");
						setState(28);
						((BinaryOpContext)_localctx).op = match(EXPONENTIATE);
						setState(29);
						expr(9);
						}
						break;
					case 2:
						{
						_localctx = new BinaryOpContext(new ExprContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_expr);
						setState(30);
						if (!(precpred(_ctx, 8))) throw new FailedPredicateException(this, "precpred(_ctx, 8)");
						setState(31);
						((BinaryOpContext)_localctx).op = _input.LT(1);
						_la = _input.LA(1);
						if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & 94208L) != 0)) ) {
							((BinaryOpContext)_localctx).op = (Token)_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(32);
						expr(9);
						}
						break;
					case 3:
						{
						_localctx = new BinaryOpContext(new ExprContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_expr);
						setState(33);
						if (!(precpred(_ctx, 7))) throw new FailedPredicateException(this, "precpred(_ctx, 7)");
						setState(34);
						((BinaryOpContext)_localctx).op = _input.LT(1);
						_la = _input.LA(1);
						if ( !(_la==PLUS || _la==MINUS) ) {
							((BinaryOpContext)_localctx).op = (Token)_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(35);
						expr(8);
						}
						break;
					}
					} 
				}
				setState(40);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,3,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class FunctionArgsContext extends ParserRuleContext {
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public FunctionArgsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_functionArgs; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KnimeExpressionListener ) ((KnimeExpressionListener)listener).enterFunctionArgs(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KnimeExpressionListener ) ((KnimeExpressionListener)listener).exitFunctionArgs(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof KnimeExpressionVisitor ) return ((KnimeExpressionVisitor<? extends T>)visitor).visitFunctionArgs(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FunctionArgsContext functionArgs() throws RecognitionException {
		FunctionArgsContext _localctx = new FunctionArgsContext(_ctx, getState());
		enterRule(_localctx, 4, RULE_functionArgs);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(41);
			expr(0);
			setState(46);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,4,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(42);
					match(T__2);
					setState(43);
					expr(0);
					}
					} 
				}
				setState(48);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,4,_ctx);
			}
			setState(50);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==T__2) {
				{
				setState(49);
				match(T__2);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public boolean sempred(RuleContext _localctx, int ruleIndex, int predIndex) {
		switch (ruleIndex) {
		case 1:
			return expr_sempred((ExprContext)_localctx, predIndex);
		}
		return true;
	}
	private boolean expr_sempred(ExprContext _localctx, int predIndex) {
		switch (predIndex) {
		case 0:
			return precpred(_ctx, 9);
		case 1:
			return precpred(_ctx, 8);
		case 2:
			return precpred(_ctx, 7);
		}
		return true;
	}

	public static final String _serializedATN =
		"\u0004\u0001\u001a5\u0002\u0000\u0007\u0000\u0002\u0001\u0007\u0001\u0002"+
		"\u0002\u0007\u0002\u0001\u0000\u0001\u0000\u0001\u0000\u0001\u0001\u0001"+
		"\u0001\u0001\u0001\u0001\u0001\u0003\u0001\u000e\b\u0001\u0001\u0001\u0001"+
		"\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001"+
		"\u0001\u0001\u0001\u0001\u0001\u0003\u0001\u001a\b\u0001\u0001\u0001\u0001"+
		"\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001"+
		"\u0001\u0001\u0001\u0005\u0001%\b\u0001\n\u0001\f\u0001(\t\u0001\u0001"+
		"\u0002\u0001\u0002\u0001\u0002\u0005\u0002-\b\u0002\n\u0002\f\u00020\t"+
		"\u0002\u0001\u0002\u0003\u00023\b\u0002\u0001\u0002\u0000\u0001\u0002"+
		"\u0003\u0000\u0002\u0004\u0000\u0002\u0002\u0000\f\u000e\u0010\u0010\u0001"+
		"\u0000\n\u000b=\u0000\u0006\u0001\u0000\u0000\u0000\u0002\u0019\u0001"+
		"\u0000\u0000\u0000\u0004)\u0001\u0000\u0000\u0000\u0006\u0007\u0003\u0002"+
		"\u0001\u0000\u0007\b\u0005\u0000\u0000\u0001\b\u0001\u0001\u0000\u0000"+
		"\u0000\t\n\u0006\u0001\uffff\uffff\u0000\n\u000b\u0005\u001a\u0000\u0000"+
		"\u000b\r\u0005\u0001\u0000\u0000\f\u000e\u0003\u0004\u0002\u0000\r\f\u0001"+
		"\u0000\u0000\u0000\r\u000e\u0001\u0000\u0000\u0000\u000e\u000f\u0001\u0000"+
		"\u0000\u0000\u000f\u001a\u0005\u0002\u0000\u0000\u0010\u0011\u0005\u0001"+
		"\u0000\u0000\u0011\u0012\u0003\u0002\u0001\u0000\u0012\u0013\u0005\u0002"+
		"\u0000\u0000\u0013\u001a\u0001\u0000\u0000\u0000\u0014\u001a\u0005\u0005"+
		"\u0000\u0000\u0015\u001a\u0005\u0006\u0000\u0000\u0016\u001a\u0005\u0007"+
		"\u0000\u0000\u0017\u001a\u0005\b\u0000\u0000\u0018\u001a\u0005\t\u0000"+
		"\u0000\u0019\t\u0001\u0000\u0000\u0000\u0019\u0010\u0001\u0000\u0000\u0000"+
		"\u0019\u0014\u0001\u0000\u0000\u0000\u0019\u0015\u0001\u0000\u0000\u0000"+
		"\u0019\u0016\u0001\u0000\u0000\u0000\u0019\u0017\u0001\u0000\u0000\u0000"+
		"\u0019\u0018\u0001\u0000\u0000\u0000\u001a&\u0001\u0000\u0000\u0000\u001b"+
		"\u001c\n\t\u0000\u0000\u001c\u001d\u0005\u000f\u0000\u0000\u001d%\u0003"+
		"\u0002\u0001\t\u001e\u001f\n\b\u0000\u0000\u001f \u0007\u0000\u0000\u0000"+
		" %\u0003\u0002\u0001\t!\"\n\u0007\u0000\u0000\"#\u0007\u0001\u0000\u0000"+
		"#%\u0003\u0002\u0001\b$\u001b\u0001\u0000\u0000\u0000$\u001e\u0001\u0000"+
		"\u0000\u0000$!\u0001\u0000\u0000\u0000%(\u0001\u0000\u0000\u0000&$\u0001"+
		"\u0000\u0000\u0000&\'\u0001\u0000\u0000\u0000\'\u0003\u0001\u0000\u0000"+
		"\u0000(&\u0001\u0000\u0000\u0000).\u0003\u0002\u0001\u0000*+\u0005\u0003"+
		"\u0000\u0000+-\u0003\u0002\u0001\u0000,*\u0001\u0000\u0000\u0000-0\u0001"+
		"\u0000\u0000\u0000.,\u0001\u0000\u0000\u0000./\u0001\u0000\u0000\u0000"+
		"/2\u0001\u0000\u0000\u00000.\u0001\u0000\u0000\u000013\u0005\u0003\u0000"+
		"\u000021\u0001\u0000\u0000\u000023\u0001\u0000\u0000\u00003\u0005\u0001"+
		"\u0000\u0000\u0000\u0006\r\u0019$&.2";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}