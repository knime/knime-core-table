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
		T__0=1, T__1=2, T__2=3, T__3=4, T__4=5, T__5=6, LINE_COMMENT=7, WHITESPACE=8, 
		BOOLEAN=9, POSITIVE_INTEGER=10, POSITIVE_FLOAT=11, STRING=12, MISSING=13, 
		MATH_CONSTANT=14, ROW_INDEX=15, ROW_NUMBER=16, ROW_ID=17, PLUS=18, MINUS=19, 
		MULTIPLY=20, DIVIDE=21, FLOOR_DIVIDE=22, EXPONENTIATE=23, MODULO=24, LESS_THAN=25, 
		LESS_THAN_EQUAL=26, GREATER_THAN=27, GREATER_THAN_EQUAL=28, EQUAL=29, 
		NOT_EQUAL=30, AND=31, OR=32, NOT=33, MISSING_FALLBACK=34, AGGREGATION_IDENTIFIER=35, 
		FUNCTION_IDENTIFIER=36, COLUMN_IDENTIFIER=37, FLOW_VAR_IDENTIFIER=38, 
		NAMED_ARGUMENT_IDENTIFIER=39;
	public static final int
		RULE_fullExpr = 0, RULE_atom = 1, RULE_expr = 2, RULE_functionArgs = 3, 
		RULE_aggregationArgs = 4, RULE_positionalAggregationArgs = 5, RULE_positionalAggregationArg = 6, 
		RULE_namedAggregationArgs = 7, RULE_namedAggregationArg = 8, RULE_negativeInteger = 9, 
		RULE_negativeFloat = 10;
	private static String[] makeRuleNames() {
		return new String[] {
			"fullExpr", "atom", "expr", "functionArgs", "aggregationArgs", "positionalAggregationArgs", 
			"positionalAggregationArg", "namedAggregationArgs", "namedAggregationArg", 
			"negativeInteger", "negativeFloat"
		};
	}
	public static final String[] ruleNames = makeRuleNames();

	private static String[] makeLiteralNames() {
		return new String[] {
			null, "'$$['", "']'", "'$['", "','", "'('", "')'", null, null, null, 
			null, null, null, "'MISSING'", null, "'$[ROW_INDEX]'", "'$[ROW_NUMBER]'", 
			"'$[ROW_ID]'", "'+'", "'-'", "'*'", "'/'", "'//'", "'**'", "'%'", "'<'", 
			"'<='", "'>'", "'>='", null, null, "'and'", "'or'", "'not'", "'??'"
		};
	}
	private static final String[] _LITERAL_NAMES = makeLiteralNames();
	private static String[] makeSymbolicNames() {
		return new String[] {
			null, null, null, null, null, null, null, "LINE_COMMENT", "WHITESPACE", 
			"BOOLEAN", "POSITIVE_INTEGER", "POSITIVE_FLOAT", "STRING", "MISSING", 
			"MATH_CONSTANT", "ROW_INDEX", "ROW_NUMBER", "ROW_ID", "PLUS", "MINUS", 
			"MULTIPLY", "DIVIDE", "FLOOR_DIVIDE", "EXPONENTIATE", "MODULO", "LESS_THAN", 
			"LESS_THAN_EQUAL", "GREATER_THAN", "GREATER_THAN_EQUAL", "EQUAL", "NOT_EQUAL", 
			"AND", "OR", "NOT", "MISSING_FALLBACK", "AGGREGATION_IDENTIFIER", "FUNCTION_IDENTIFIER", 
			"COLUMN_IDENTIFIER", "FLOW_VAR_IDENTIFIER", "NAMED_ARGUMENT_IDENTIFIER"
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
			setState(22);
			expr(0);
			setState(23);
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
	public static class AtomContext extends ParserRuleContext {
		public TerminalNode BOOLEAN() { return getToken(KnimeExpressionParser.BOOLEAN, 0); }
		public TerminalNode POSITIVE_INTEGER() { return getToken(KnimeExpressionParser.POSITIVE_INTEGER, 0); }
		public TerminalNode POSITIVE_FLOAT() { return getToken(KnimeExpressionParser.POSITIVE_FLOAT, 0); }
		public TerminalNode STRING() { return getToken(KnimeExpressionParser.STRING, 0); }
		public TerminalNode MISSING() { return getToken(KnimeExpressionParser.MISSING, 0); }
		public TerminalNode MATH_CONSTANT() { return getToken(KnimeExpressionParser.MATH_CONSTANT, 0); }
		public TerminalNode ROW_INDEX() { return getToken(KnimeExpressionParser.ROW_INDEX, 0); }
		public TerminalNode ROW_NUMBER() { return getToken(KnimeExpressionParser.ROW_NUMBER, 0); }
		public TerminalNode ROW_ID() { return getToken(KnimeExpressionParser.ROW_ID, 0); }
		public AtomContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_atom; }
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

	public final AtomContext atom() throws RecognitionException {
		AtomContext _localctx = new AtomContext(_ctx, getState());
		enterRule(_localctx, 2, RULE_atom);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(25);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & 261632L) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
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
		public TerminalNode MISSING_FALLBACK() { return getToken(KnimeExpressionParser.MISSING_FALLBACK, 0); }
		public TerminalNode EXPONENTIATE() { return getToken(KnimeExpressionParser.EXPONENTIATE, 0); }
		public TerminalNode MULTIPLY() { return getToken(KnimeExpressionParser.MULTIPLY, 0); }
		public TerminalNode DIVIDE() { return getToken(KnimeExpressionParser.DIVIDE, 0); }
		public TerminalNode MODULO() { return getToken(KnimeExpressionParser.MODULO, 0); }
		public TerminalNode FLOOR_DIVIDE() { return getToken(KnimeExpressionParser.FLOOR_DIVIDE, 0); }
		public TerminalNode PLUS() { return getToken(KnimeExpressionParser.PLUS, 0); }
		public TerminalNode MINUS() { return getToken(KnimeExpressionParser.MINUS, 0); }
		public TerminalNode LESS_THAN() { return getToken(KnimeExpressionParser.LESS_THAN, 0); }
		public TerminalNode LESS_THAN_EQUAL() { return getToken(KnimeExpressionParser.LESS_THAN_EQUAL, 0); }
		public TerminalNode GREATER_THAN() { return getToken(KnimeExpressionParser.GREATER_THAN, 0); }
		public TerminalNode GREATER_THAN_EQUAL() { return getToken(KnimeExpressionParser.GREATER_THAN_EQUAL, 0); }
		public TerminalNode EQUAL() { return getToken(KnimeExpressionParser.EQUAL, 0); }
		public TerminalNode NOT_EQUAL() { return getToken(KnimeExpressionParser.NOT_EQUAL, 0); }
		public TerminalNode AND() { return getToken(KnimeExpressionParser.AND, 0); }
		public TerminalNode OR() { return getToken(KnimeExpressionParser.OR, 0); }
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
	public static class ColAccessContext extends ExprContext {
		public Token shortName;
		public Token longName;
		public Token minus;
		public Token offset;
		public TerminalNode COLUMN_IDENTIFIER() { return getToken(KnimeExpressionParser.COLUMN_IDENTIFIER, 0); }
		public TerminalNode STRING() { return getToken(KnimeExpressionParser.STRING, 0); }
		public TerminalNode POSITIVE_INTEGER() { return getToken(KnimeExpressionParser.POSITIVE_INTEGER, 0); }
		public TerminalNode MINUS() { return getToken(KnimeExpressionParser.MINUS, 0); }
		public ColAccessContext(ExprContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KnimeExpressionListener ) ((KnimeExpressionListener)listener).enterColAccess(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KnimeExpressionListener ) ((KnimeExpressionListener)listener).exitColAccess(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof KnimeExpressionVisitor ) return ((KnimeExpressionVisitor<? extends T>)visitor).visitColAccess(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class AggregationCallContext extends ExprContext {
		public Token name;
		public AggregationArgsContext aggregationArgs() {
			return getRuleContext(AggregationArgsContext.class,0);
		}
		public TerminalNode AGGREGATION_IDENTIFIER() { return getToken(KnimeExpressionParser.AGGREGATION_IDENTIFIER, 0); }
		public AggregationCallContext(ExprContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KnimeExpressionListener ) ((KnimeExpressionListener)listener).enterAggregationCall(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KnimeExpressionListener ) ((KnimeExpressionListener)listener).exitAggregationCall(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof KnimeExpressionVisitor ) return ((KnimeExpressionVisitor<? extends T>)visitor).visitAggregationCall(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class FunctionCallContext extends ExprContext {
		public Token name;
		public TerminalNode FUNCTION_IDENTIFIER() { return getToken(KnimeExpressionParser.FUNCTION_IDENTIFIER, 0); }
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
	public static class FlowVarAccessContext extends ExprContext {
		public Token shortName;
		public Token longName;
		public TerminalNode FLOW_VAR_IDENTIFIER() { return getToken(KnimeExpressionParser.FLOW_VAR_IDENTIFIER, 0); }
		public TerminalNode STRING() { return getToken(KnimeExpressionParser.STRING, 0); }
		public FlowVarAccessContext(ExprContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KnimeExpressionListener ) ((KnimeExpressionListener)listener).enterFlowVarAccess(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KnimeExpressionListener ) ((KnimeExpressionListener)listener).exitFlowVarAccess(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof KnimeExpressionVisitor ) return ((KnimeExpressionVisitor<? extends T>)visitor).visitFlowVarAccess(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class AtomExprContext extends ExprContext {
		public AtomContext atom() {
			return getRuleContext(AtomContext.class,0);
		}
		public AtomExprContext(ExprContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KnimeExpressionListener ) ((KnimeExpressionListener)listener).enterAtomExpr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KnimeExpressionListener ) ((KnimeExpressionListener)listener).exitAtomExpr(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof KnimeExpressionVisitor ) return ((KnimeExpressionVisitor<? extends T>)visitor).visitAtomExpr(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class UnaryOpContext extends ExprContext {
		public Token op;
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public TerminalNode MINUS() { return getToken(KnimeExpressionParser.MINUS, 0); }
		public TerminalNode NOT() { return getToken(KnimeExpressionParser.NOT, 0); }
		public UnaryOpContext(ExprContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KnimeExpressionListener ) ((KnimeExpressionListener)listener).enterUnaryOp(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KnimeExpressionListener ) ((KnimeExpressionListener)listener).exitUnaryOp(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof KnimeExpressionVisitor ) return ((KnimeExpressionVisitor<? extends T>)visitor).visitUnaryOp(this);
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
		int _startState = 4;
		enterRecursionRule(_localctx, 4, RULE_expr, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(79);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case T__0:
			case FLOW_VAR_IDENTIFIER:
				{
				_localctx = new FlowVarAccessContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(36);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case FLOW_VAR_IDENTIFIER:
					{
					setState(28);
					((FlowVarAccessContext)_localctx).shortName = match(FLOW_VAR_IDENTIFIER);
					}
					break;
				case T__0:
					{
					setState(30); 
					_errHandler.sync(this);
					_la = _input.LA(1);
					do {
						{
						{
						setState(29);
						match(T__0);
						}
						}
						setState(32); 
						_errHandler.sync(this);
						_la = _input.LA(1);
					} while ( _la==T__0 );
					setState(34);
					((FlowVarAccessContext)_localctx).longName = match(STRING);
					setState(35);
					match(T__1);
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				break;
			case T__2:
			case COLUMN_IDENTIFIER:
				{
				_localctx = new ColAccessContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(57);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case COLUMN_IDENTIFIER:
					{
					setState(38);
					((ColAccessContext)_localctx).shortName = match(COLUMN_IDENTIFIER);
					}
					break;
				case T__2:
					{
					setState(40); 
					_errHandler.sync(this);
					_la = _input.LA(1);
					do {
						{
						{
						setState(39);
						match(T__2);
						}
						}
						setState(42); 
						_errHandler.sync(this);
						_la = _input.LA(1);
					} while ( _la==T__2 );
					setState(44);
					((ColAccessContext)_localctx).longName = match(STRING);
					setState(54);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==T__3) {
						{
						setState(46); 
						_errHandler.sync(this);
						_la = _input.LA(1);
						do {
							{
							{
							setState(45);
							match(T__3);
							}
							}
							setState(48); 
							_errHandler.sync(this);
							_la = _input.LA(1);
						} while ( _la==T__3 );
						setState(51);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==MINUS) {
							{
							setState(50);
							((ColAccessContext)_localctx).minus = match(MINUS);
							}
						}

						setState(53);
						((ColAccessContext)_localctx).offset = match(POSITIVE_INTEGER);
						}
					}

					setState(56);
					match(T__1);
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				break;
			case FUNCTION_IDENTIFIER:
				{
				_localctx = new FunctionCallContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(59);
				((FunctionCallContext)_localctx).name = match(FUNCTION_IDENTIFIER);
				setState(60);
				match(T__4);
				setState(62);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 523986796074L) != 0)) {
					{
					setState(61);
					functionArgs();
					}
				}

				setState(64);
				match(T__5);
				}
				break;
			case AGGREGATION_IDENTIFIER:
				{
				_localctx = new AggregationCallContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(65);
				((AggregationCallContext)_localctx).name = match(AGGREGATION_IDENTIFIER);
				setState(66);
				match(T__4);
				setState(67);
				aggregationArgs();
				setState(68);
				match(T__5);
				}
				break;
			case MINUS:
				{
				_localctx = new UnaryOpContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(70);
				((UnaryOpContext)_localctx).op = match(MINUS);
				setState(71);
				expr(9);
				}
				break;
			case NOT:
				{
				_localctx = new UnaryOpContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(72);
				((UnaryOpContext)_localctx).op = match(NOT);
				setState(73);
				expr(5);
				}
				break;
			case T__4:
				{
				_localctx = new ParenthesisedExprContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(74);
				match(T__4);
				setState(75);
				((ParenthesisedExprContext)_localctx).inner = expr(0);
				setState(76);
				match(T__5);
				}
				break;
			case BOOLEAN:
			case POSITIVE_INTEGER:
			case POSITIVE_FLOAT:
			case STRING:
			case MISSING:
			case MATH_CONSTANT:
			case ROW_INDEX:
			case ROW_NUMBER:
			case ROW_ID:
				{
				_localctx = new AtomExprContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(78);
				atom();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			_ctx.stop = _input.LT(-1);
			setState(104);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,10,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(102);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,9,_ctx) ) {
					case 1:
						{
						_localctx = new BinaryOpContext(new ExprContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_expr);
						setState(81);
						if (!(precpred(_ctx, 11))) throw new FailedPredicateException(this, "precpred(_ctx, 11)");
						setState(82);
						((BinaryOpContext)_localctx).op = match(MISSING_FALLBACK);
						setState(83);
						expr(12);
						}
						break;
					case 2:
						{
						_localctx = new BinaryOpContext(new ExprContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_expr);
						setState(84);
						if (!(precpred(_ctx, 10))) throw new FailedPredicateException(this, "precpred(_ctx, 10)");
						setState(85);
						((BinaryOpContext)_localctx).op = match(EXPONENTIATE);
						setState(86);
						expr(10);
						}
						break;
					case 3:
						{
						_localctx = new BinaryOpContext(new ExprContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_expr);
						setState(87);
						if (!(precpred(_ctx, 8))) throw new FailedPredicateException(this, "precpred(_ctx, 8)");
						setState(88);
						((BinaryOpContext)_localctx).op = _input.LT(1);
						_la = _input.LA(1);
						if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & 24117248L) != 0)) ) {
							((BinaryOpContext)_localctx).op = (Token)_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(89);
						expr(9);
						}
						break;
					case 4:
						{
						_localctx = new BinaryOpContext(new ExprContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_expr);
						setState(90);
						if (!(precpred(_ctx, 7))) throw new FailedPredicateException(this, "precpred(_ctx, 7)");
						setState(91);
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
						setState(92);
						expr(8);
						}
						break;
					case 5:
						{
						_localctx = new BinaryOpContext(new ExprContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_expr);
						setState(93);
						if (!(precpred(_ctx, 6))) throw new FailedPredicateException(this, "precpred(_ctx, 6)");
						setState(94);
						((BinaryOpContext)_localctx).op = _input.LT(1);
						_la = _input.LA(1);
						if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & 2113929216L) != 0)) ) {
							((BinaryOpContext)_localctx).op = (Token)_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(95);
						expr(7);
						}
						break;
					case 6:
						{
						_localctx = new BinaryOpContext(new ExprContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_expr);
						setState(96);
						if (!(precpred(_ctx, 4))) throw new FailedPredicateException(this, "precpred(_ctx, 4)");
						setState(97);
						((BinaryOpContext)_localctx).op = match(AND);
						setState(98);
						expr(5);
						}
						break;
					case 7:
						{
						_localctx = new BinaryOpContext(new ExprContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_expr);
						setState(99);
						if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
						setState(100);
						((BinaryOpContext)_localctx).op = match(OR);
						setState(101);
						expr(4);
						}
						break;
					}
					} 
				}
				setState(106);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,10,_ctx);
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
		enterRule(_localctx, 6, RULE_functionArgs);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(107);
			expr(0);
			setState(112);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,11,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(108);
					match(T__3);
					setState(109);
					expr(0);
					}
					} 
				}
				setState(114);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,11,_ctx);
			}
			setState(116);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==T__3) {
				{
				setState(115);
				match(T__3);
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

	@SuppressWarnings("CheckReturnValue")
	public static class AggregationArgsContext extends ParserRuleContext {
		public PositionalAggregationArgsContext positionalAggregationArgs() {
			return getRuleContext(PositionalAggregationArgsContext.class,0);
		}
		public NamedAggregationArgsContext namedAggregationArgs() {
			return getRuleContext(NamedAggregationArgsContext.class,0);
		}
		public AggregationArgsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_aggregationArgs; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KnimeExpressionListener ) ((KnimeExpressionListener)listener).enterAggregationArgs(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KnimeExpressionListener ) ((KnimeExpressionListener)listener).exitAggregationArgs(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof KnimeExpressionVisitor ) return ((KnimeExpressionVisitor<? extends T>)visitor).visitAggregationArgs(this);
			else return visitor.visitChildren(this);
		}
	}

	public final AggregationArgsContext aggregationArgs() throws RecognitionException {
		AggregationArgsContext _localctx = new AggregationArgsContext(_ctx, getState());
		enterRule(_localctx, 8, RULE_aggregationArgs);
		int _la;
		try {
			setState(130);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case BOOLEAN:
			case POSITIVE_INTEGER:
			case POSITIVE_FLOAT:
			case STRING:
			case MISSING:
			case MATH_CONSTANT:
			case ROW_INDEX:
			case ROW_NUMBER:
			case ROW_ID:
			case MINUS:
				enterOuterAlt(_localctx, 1);
				{
				{
				setState(118);
				positionalAggregationArgs();
				setState(121);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,13,_ctx) ) {
				case 1:
					{
					setState(119);
					match(T__3);
					setState(120);
					namedAggregationArgs();
					}
					break;
				}
				setState(124);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__3) {
					{
					setState(123);
					match(T__3);
					}
				}

				}
				}
				break;
			case NAMED_ARGUMENT_IDENTIFIER:
				enterOuterAlt(_localctx, 2);
				{
				{
				setState(126);
				namedAggregationArgs();
				setState(128);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__3) {
					{
					setState(127);
					match(T__3);
					}
				}

				}
				}
				break;
			default:
				throw new NoViableAltException(this);
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
	public static class PositionalAggregationArgsContext extends ParserRuleContext {
		public List<PositionalAggregationArgContext> positionalAggregationArg() {
			return getRuleContexts(PositionalAggregationArgContext.class);
		}
		public PositionalAggregationArgContext positionalAggregationArg(int i) {
			return getRuleContext(PositionalAggregationArgContext.class,i);
		}
		public PositionalAggregationArgsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_positionalAggregationArgs; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KnimeExpressionListener ) ((KnimeExpressionListener)listener).enterPositionalAggregationArgs(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KnimeExpressionListener ) ((KnimeExpressionListener)listener).exitPositionalAggregationArgs(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof KnimeExpressionVisitor ) return ((KnimeExpressionVisitor<? extends T>)visitor).visitPositionalAggregationArgs(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PositionalAggregationArgsContext positionalAggregationArgs() throws RecognitionException {
		PositionalAggregationArgsContext _localctx = new PositionalAggregationArgsContext(_ctx, getState());
		enterRule(_localctx, 10, RULE_positionalAggregationArgs);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(132);
			positionalAggregationArg();
			setState(137);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,17,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(133);
					match(T__3);
					setState(134);
					positionalAggregationArg();
					}
					} 
				}
				setState(139);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,17,_ctx);
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

	@SuppressWarnings("CheckReturnValue")
	public static class PositionalAggregationArgContext extends ParserRuleContext {
		public AtomContext atom() {
			return getRuleContext(AtomContext.class,0);
		}
		public NegativeFloatContext negativeFloat() {
			return getRuleContext(NegativeFloatContext.class,0);
		}
		public NegativeIntegerContext negativeInteger() {
			return getRuleContext(NegativeIntegerContext.class,0);
		}
		public PositionalAggregationArgContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_positionalAggregationArg; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KnimeExpressionListener ) ((KnimeExpressionListener)listener).enterPositionalAggregationArg(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KnimeExpressionListener ) ((KnimeExpressionListener)listener).exitPositionalAggregationArg(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof KnimeExpressionVisitor ) return ((KnimeExpressionVisitor<? extends T>)visitor).visitPositionalAggregationArg(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PositionalAggregationArgContext positionalAggregationArg() throws RecognitionException {
		PositionalAggregationArgContext _localctx = new PositionalAggregationArgContext(_ctx, getState());
		enterRule(_localctx, 12, RULE_positionalAggregationArg);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(143);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,18,_ctx) ) {
			case 1:
				{
				setState(140);
				atom();
				}
				break;
			case 2:
				{
				setState(141);
				negativeFloat();
				}
				break;
			case 3:
				{
				setState(142);
				negativeInteger();
				}
				break;
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

	@SuppressWarnings("CheckReturnValue")
	public static class NamedAggregationArgsContext extends ParserRuleContext {
		public List<NamedAggregationArgContext> namedAggregationArg() {
			return getRuleContexts(NamedAggregationArgContext.class);
		}
		public NamedAggregationArgContext namedAggregationArg(int i) {
			return getRuleContext(NamedAggregationArgContext.class,i);
		}
		public NamedAggregationArgsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_namedAggregationArgs; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KnimeExpressionListener ) ((KnimeExpressionListener)listener).enterNamedAggregationArgs(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KnimeExpressionListener ) ((KnimeExpressionListener)listener).exitNamedAggregationArgs(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof KnimeExpressionVisitor ) return ((KnimeExpressionVisitor<? extends T>)visitor).visitNamedAggregationArgs(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NamedAggregationArgsContext namedAggregationArgs() throws RecognitionException {
		NamedAggregationArgsContext _localctx = new NamedAggregationArgsContext(_ctx, getState());
		enterRule(_localctx, 14, RULE_namedAggregationArgs);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(145);
			namedAggregationArg();
			setState(150);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,19,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(146);
					match(T__3);
					setState(147);
					namedAggregationArg();
					}
					} 
				}
				setState(152);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,19,_ctx);
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

	@SuppressWarnings("CheckReturnValue")
	public static class NamedAggregationArgContext extends ParserRuleContext {
		public Token argName;
		public TerminalNode NAMED_ARGUMENT_IDENTIFIER() { return getToken(KnimeExpressionParser.NAMED_ARGUMENT_IDENTIFIER, 0); }
		public AtomContext atom() {
			return getRuleContext(AtomContext.class,0);
		}
		public NegativeFloatContext negativeFloat() {
			return getRuleContext(NegativeFloatContext.class,0);
		}
		public NegativeIntegerContext negativeInteger() {
			return getRuleContext(NegativeIntegerContext.class,0);
		}
		public NamedAggregationArgContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_namedAggregationArg; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KnimeExpressionListener ) ((KnimeExpressionListener)listener).enterNamedAggregationArg(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KnimeExpressionListener ) ((KnimeExpressionListener)listener).exitNamedAggregationArg(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof KnimeExpressionVisitor ) return ((KnimeExpressionVisitor<? extends T>)visitor).visitNamedAggregationArg(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NamedAggregationArgContext namedAggregationArg() throws RecognitionException {
		NamedAggregationArgContext _localctx = new NamedAggregationArgContext(_ctx, getState());
		enterRule(_localctx, 16, RULE_namedAggregationArg);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(153);
			((NamedAggregationArgContext)_localctx).argName = match(NAMED_ARGUMENT_IDENTIFIER);
			setState(157);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,20,_ctx) ) {
			case 1:
				{
				setState(154);
				atom();
				}
				break;
			case 2:
				{
				setState(155);
				negativeFloat();
				}
				break;
			case 3:
				{
				setState(156);
				negativeInteger();
				}
				break;
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

	@SuppressWarnings("CheckReturnValue")
	public static class NegativeIntegerContext extends ParserRuleContext {
		public TerminalNode MINUS() { return getToken(KnimeExpressionParser.MINUS, 0); }
		public TerminalNode POSITIVE_INTEGER() { return getToken(KnimeExpressionParser.POSITIVE_INTEGER, 0); }
		public NegativeIntegerContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_negativeInteger; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KnimeExpressionListener ) ((KnimeExpressionListener)listener).enterNegativeInteger(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KnimeExpressionListener ) ((KnimeExpressionListener)listener).exitNegativeInteger(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof KnimeExpressionVisitor ) return ((KnimeExpressionVisitor<? extends T>)visitor).visitNegativeInteger(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NegativeIntegerContext negativeInteger() throws RecognitionException {
		NegativeIntegerContext _localctx = new NegativeIntegerContext(_ctx, getState());
		enterRule(_localctx, 18, RULE_negativeInteger);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(159);
			match(MINUS);
			setState(160);
			match(POSITIVE_INTEGER);
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
	public static class NegativeFloatContext extends ParserRuleContext {
		public TerminalNode MINUS() { return getToken(KnimeExpressionParser.MINUS, 0); }
		public TerminalNode POSITIVE_FLOAT() { return getToken(KnimeExpressionParser.POSITIVE_FLOAT, 0); }
		public NegativeFloatContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_negativeFloat; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KnimeExpressionListener ) ((KnimeExpressionListener)listener).enterNegativeFloat(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KnimeExpressionListener ) ((KnimeExpressionListener)listener).exitNegativeFloat(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof KnimeExpressionVisitor ) return ((KnimeExpressionVisitor<? extends T>)visitor).visitNegativeFloat(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NegativeFloatContext negativeFloat() throws RecognitionException {
		NegativeFloatContext _localctx = new NegativeFloatContext(_ctx, getState());
		enterRule(_localctx, 20, RULE_negativeFloat);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(162);
			match(MINUS);
			setState(163);
			match(POSITIVE_FLOAT);
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
		case 2:
			return expr_sempred((ExprContext)_localctx, predIndex);
		}
		return true;
	}
	private boolean expr_sempred(ExprContext _localctx, int predIndex) {
		switch (predIndex) {
		case 0:
			return precpred(_ctx, 11);
		case 1:
			return precpred(_ctx, 10);
		case 2:
			return precpred(_ctx, 8);
		case 3:
			return precpred(_ctx, 7);
		case 4:
			return precpred(_ctx, 6);
		case 5:
			return precpred(_ctx, 4);
		case 6:
			return precpred(_ctx, 3);
		}
		return true;
	}

	public static final String _serializedATN =
		"\u0004\u0001\'\u00a6\u0002\u0000\u0007\u0000\u0002\u0001\u0007\u0001\u0002"+
		"\u0002\u0007\u0002\u0002\u0003\u0007\u0003\u0002\u0004\u0007\u0004\u0002"+
		"\u0005\u0007\u0005\u0002\u0006\u0007\u0006\u0002\u0007\u0007\u0007\u0002"+
		"\b\u0007\b\u0002\t\u0007\t\u0002\n\u0007\n\u0001\u0000\u0001\u0000\u0001"+
		"\u0000\u0001\u0001\u0001\u0001\u0001\u0002\u0001\u0002\u0001\u0002\u0004"+
		"\u0002\u001f\b\u0002\u000b\u0002\f\u0002 \u0001\u0002\u0001\u0002\u0003"+
		"\u0002%\b\u0002\u0001\u0002\u0001\u0002\u0004\u0002)\b\u0002\u000b\u0002"+
		"\f\u0002*\u0001\u0002\u0001\u0002\u0004\u0002/\b\u0002\u000b\u0002\f\u0002"+
		"0\u0001\u0002\u0003\u00024\b\u0002\u0001\u0002\u0003\u00027\b\u0002\u0001"+
		"\u0002\u0003\u0002:\b\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0003"+
		"\u0002?\b\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001"+
		"\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001"+
		"\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0003\u0002P\b"+
		"\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001"+
		"\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001"+
		"\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001"+
		"\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0005\u0002g\b\u0002\n\u0002"+
		"\f\u0002j\t\u0002\u0001\u0003\u0001\u0003\u0001\u0003\u0005\u0003o\b\u0003"+
		"\n\u0003\f\u0003r\t\u0003\u0001\u0003\u0003\u0003u\b\u0003\u0001\u0004"+
		"\u0001\u0004\u0001\u0004\u0003\u0004z\b\u0004\u0001\u0004\u0003\u0004"+
		"}\b\u0004\u0001\u0004\u0001\u0004\u0003\u0004\u0081\b\u0004\u0003\u0004"+
		"\u0083\b\u0004\u0001\u0005\u0001\u0005\u0001\u0005\u0005\u0005\u0088\b"+
		"\u0005\n\u0005\f\u0005\u008b\t\u0005\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0003\u0006\u0090\b\u0006\u0001\u0007\u0001\u0007\u0001\u0007\u0005\u0007"+
		"\u0095\b\u0007\n\u0007\f\u0007\u0098\t\u0007\u0001\b\u0001\b\u0001\b\u0001"+
		"\b\u0003\b\u009e\b\b\u0001\t\u0001\t\u0001\t\u0001\n\u0001\n\u0001\n\u0001"+
		"\n\u0000\u0001\u0004\u000b\u0000\u0002\u0004\u0006\b\n\f\u000e\u0010\u0012"+
		"\u0014\u0000\u0004\u0001\u0000\t\u0011\u0002\u0000\u0014\u0016\u0018\u0018"+
		"\u0001\u0000\u0012\u0013\u0001\u0000\u0019\u001e\u00bc\u0000\u0016\u0001"+
		"\u0000\u0000\u0000\u0002\u0019\u0001\u0000\u0000\u0000\u0004O\u0001\u0000"+
		"\u0000\u0000\u0006k\u0001\u0000\u0000\u0000\b\u0082\u0001\u0000\u0000"+
		"\u0000\n\u0084\u0001\u0000\u0000\u0000\f\u008f\u0001\u0000\u0000\u0000"+
		"\u000e\u0091\u0001\u0000\u0000\u0000\u0010\u0099\u0001\u0000\u0000\u0000"+
		"\u0012\u009f\u0001\u0000\u0000\u0000\u0014\u00a2\u0001\u0000\u0000\u0000"+
		"\u0016\u0017\u0003\u0004\u0002\u0000\u0017\u0018\u0005\u0000\u0000\u0001"+
		"\u0018\u0001\u0001\u0000\u0000\u0000\u0019\u001a\u0007\u0000\u0000\u0000"+
		"\u001a\u0003\u0001\u0000\u0000\u0000\u001b$\u0006\u0002\uffff\uffff\u0000"+
		"\u001c%\u0005&\u0000\u0000\u001d\u001f\u0005\u0001\u0000\u0000\u001e\u001d"+
		"\u0001\u0000\u0000\u0000\u001f \u0001\u0000\u0000\u0000 \u001e\u0001\u0000"+
		"\u0000\u0000 !\u0001\u0000\u0000\u0000!\"\u0001\u0000\u0000\u0000\"#\u0005"+
		"\f\u0000\u0000#%\u0005\u0002\u0000\u0000$\u001c\u0001\u0000\u0000\u0000"+
		"$\u001e\u0001\u0000\u0000\u0000%P\u0001\u0000\u0000\u0000&:\u0005%\u0000"+
		"\u0000\')\u0005\u0003\u0000\u0000(\'\u0001\u0000\u0000\u0000)*\u0001\u0000"+
		"\u0000\u0000*(\u0001\u0000\u0000\u0000*+\u0001\u0000\u0000\u0000+,\u0001"+
		"\u0000\u0000\u0000,6\u0005\f\u0000\u0000-/\u0005\u0004\u0000\u0000.-\u0001"+
		"\u0000\u0000\u0000/0\u0001\u0000\u0000\u00000.\u0001\u0000\u0000\u0000"+
		"01\u0001\u0000\u0000\u000013\u0001\u0000\u0000\u000024\u0005\u0013\u0000"+
		"\u000032\u0001\u0000\u0000\u000034\u0001\u0000\u0000\u000045\u0001\u0000"+
		"\u0000\u000057\u0005\n\u0000\u00006.\u0001\u0000\u0000\u000067\u0001\u0000"+
		"\u0000\u000078\u0001\u0000\u0000\u00008:\u0005\u0002\u0000\u00009&\u0001"+
		"\u0000\u0000\u00009(\u0001\u0000\u0000\u0000:P\u0001\u0000\u0000\u0000"+
		";<\u0005$\u0000\u0000<>\u0005\u0005\u0000\u0000=?\u0003\u0006\u0003\u0000"+
		">=\u0001\u0000\u0000\u0000>?\u0001\u0000\u0000\u0000?@\u0001\u0000\u0000"+
		"\u0000@P\u0005\u0006\u0000\u0000AB\u0005#\u0000\u0000BC\u0005\u0005\u0000"+
		"\u0000CD\u0003\b\u0004\u0000DE\u0005\u0006\u0000\u0000EP\u0001\u0000\u0000"+
		"\u0000FG\u0005\u0013\u0000\u0000GP\u0003\u0004\u0002\tHI\u0005!\u0000"+
		"\u0000IP\u0003\u0004\u0002\u0005JK\u0005\u0005\u0000\u0000KL\u0003\u0004"+
		"\u0002\u0000LM\u0005\u0006\u0000\u0000MP\u0001\u0000\u0000\u0000NP\u0003"+
		"\u0002\u0001\u0000O\u001b\u0001\u0000\u0000\u0000O9\u0001\u0000\u0000"+
		"\u0000O;\u0001\u0000\u0000\u0000OA\u0001\u0000\u0000\u0000OF\u0001\u0000"+
		"\u0000\u0000OH\u0001\u0000\u0000\u0000OJ\u0001\u0000\u0000\u0000ON\u0001"+
		"\u0000\u0000\u0000Ph\u0001\u0000\u0000\u0000QR\n\u000b\u0000\u0000RS\u0005"+
		"\"\u0000\u0000Sg\u0003\u0004\u0002\fTU\n\n\u0000\u0000UV\u0005\u0017\u0000"+
		"\u0000Vg\u0003\u0004\u0002\nWX\n\b\u0000\u0000XY\u0007\u0001\u0000\u0000"+
		"Yg\u0003\u0004\u0002\tZ[\n\u0007\u0000\u0000[\\\u0007\u0002\u0000\u0000"+
		"\\g\u0003\u0004\u0002\b]^\n\u0006\u0000\u0000^_\u0007\u0003\u0000\u0000"+
		"_g\u0003\u0004\u0002\u0007`a\n\u0004\u0000\u0000ab\u0005\u001f\u0000\u0000"+
		"bg\u0003\u0004\u0002\u0005cd\n\u0003\u0000\u0000de\u0005 \u0000\u0000"+
		"eg\u0003\u0004\u0002\u0004fQ\u0001\u0000\u0000\u0000fT\u0001\u0000\u0000"+
		"\u0000fW\u0001\u0000\u0000\u0000fZ\u0001\u0000\u0000\u0000f]\u0001\u0000"+
		"\u0000\u0000f`\u0001\u0000\u0000\u0000fc\u0001\u0000\u0000\u0000gj\u0001"+
		"\u0000\u0000\u0000hf\u0001\u0000\u0000\u0000hi\u0001\u0000\u0000\u0000"+
		"i\u0005\u0001\u0000\u0000\u0000jh\u0001\u0000\u0000\u0000kp\u0003\u0004"+
		"\u0002\u0000lm\u0005\u0004\u0000\u0000mo\u0003\u0004\u0002\u0000nl\u0001"+
		"\u0000\u0000\u0000or\u0001\u0000\u0000\u0000pn\u0001\u0000\u0000\u0000"+
		"pq\u0001\u0000\u0000\u0000qt\u0001\u0000\u0000\u0000rp\u0001\u0000\u0000"+
		"\u0000su\u0005\u0004\u0000\u0000ts\u0001\u0000\u0000\u0000tu\u0001\u0000"+
		"\u0000\u0000u\u0007\u0001\u0000\u0000\u0000vy\u0003\n\u0005\u0000wx\u0005"+
		"\u0004\u0000\u0000xz\u0003\u000e\u0007\u0000yw\u0001\u0000\u0000\u0000"+
		"yz\u0001\u0000\u0000\u0000z|\u0001\u0000\u0000\u0000{}\u0005\u0004\u0000"+
		"\u0000|{\u0001\u0000\u0000\u0000|}\u0001\u0000\u0000\u0000}\u0083\u0001"+
		"\u0000\u0000\u0000~\u0080\u0003\u000e\u0007\u0000\u007f\u0081\u0005\u0004"+
		"\u0000\u0000\u0080\u007f\u0001\u0000\u0000\u0000\u0080\u0081\u0001\u0000"+
		"\u0000\u0000\u0081\u0083\u0001\u0000\u0000\u0000\u0082v\u0001\u0000\u0000"+
		"\u0000\u0082~\u0001\u0000\u0000\u0000\u0083\t\u0001\u0000\u0000\u0000"+
		"\u0084\u0089\u0003\f\u0006\u0000\u0085\u0086\u0005\u0004\u0000\u0000\u0086"+
		"\u0088\u0003\f\u0006\u0000\u0087\u0085\u0001\u0000\u0000\u0000\u0088\u008b"+
		"\u0001\u0000\u0000\u0000\u0089\u0087\u0001\u0000\u0000\u0000\u0089\u008a"+
		"\u0001\u0000\u0000\u0000\u008a\u000b\u0001\u0000\u0000\u0000\u008b\u0089"+
		"\u0001\u0000\u0000\u0000\u008c\u0090\u0003\u0002\u0001\u0000\u008d\u0090"+
		"\u0003\u0014\n\u0000\u008e\u0090\u0003\u0012\t\u0000\u008f\u008c\u0001"+
		"\u0000\u0000\u0000\u008f\u008d\u0001\u0000\u0000\u0000\u008f\u008e\u0001"+
		"\u0000\u0000\u0000\u0090\r\u0001\u0000\u0000\u0000\u0091\u0096\u0003\u0010"+
		"\b\u0000\u0092\u0093\u0005\u0004\u0000\u0000\u0093\u0095\u0003\u0010\b"+
		"\u0000\u0094\u0092\u0001\u0000\u0000\u0000\u0095\u0098\u0001\u0000\u0000"+
		"\u0000\u0096\u0094\u0001\u0000\u0000\u0000\u0096\u0097\u0001\u0000\u0000"+
		"\u0000\u0097\u000f\u0001\u0000\u0000\u0000\u0098\u0096\u0001\u0000\u0000"+
		"\u0000\u0099\u009d\u0005\'\u0000\u0000\u009a\u009e\u0003\u0002\u0001\u0000"+
		"\u009b\u009e\u0003\u0014\n\u0000\u009c\u009e\u0003\u0012\t\u0000\u009d"+
		"\u009a\u0001\u0000\u0000\u0000\u009d\u009b\u0001\u0000\u0000\u0000\u009d"+
		"\u009c\u0001\u0000\u0000\u0000\u009e\u0011\u0001\u0000\u0000\u0000\u009f"+
		"\u00a0\u0005\u0013\u0000\u0000\u00a0\u00a1\u0005\n\u0000\u0000\u00a1\u0013"+
		"\u0001\u0000\u0000\u0000\u00a2\u00a3\u0005\u0013\u0000\u0000\u00a3\u00a4"+
		"\u0005\u000b\u0000\u0000\u00a4\u0015\u0001\u0000\u0000\u0000\u0015 $*"+
		"0369>Ofhpty|\u0080\u0082\u0089\u008f\u0096\u009d";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}