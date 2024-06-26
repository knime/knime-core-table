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
		BOOLEAN=9, INTEGER=10, FLOAT=11, STRING=12, MISSING=13, MATH_CONSTANT=14, 
		ROW_INDEX=15, ROW_NUMBER=16, ROW_ID=17, PLUS=18, MINUS=19, MULTIPLY=20, 
		DIVIDE=21, FLOOR_DIVIDE=22, EXPONENTIATE=23, MODULO=24, LESS_THAN=25, 
		LESS_THAN_EQUAL=26, GREATER_THAN=27, GREATER_THAN_EQUAL=28, EQUAL=29, 
		NOT_EQUAL=30, AND=31, OR=32, NOT=33, MISSING_FALLBACK=34, AGGREGATION_IDENTIFIER=35, 
		FUNCTION_IDENTIFIER=36, COLUMN_IDENTIFIER=37, FLOW_VAR_IDENTIFIER=38, 
		NAMED_ARGUMENT_IDENTIFIER=39;
	public static final int
		RULE_fullExpr = 0, RULE_atom = 1, RULE_expr = 2, RULE_functionArgs = 3, 
		RULE_aggregationArgs = 4, RULE_positionalAggregationArgs = 5, RULE_namedAggregationArgs = 6, 
		RULE_namedAggregationArg = 7;
	private static String[] makeRuleNames() {
		return new String[] {
			"fullExpr", "atom", "expr", "functionArgs", "aggregationArgs", "positionalAggregationArgs", 
			"namedAggregationArgs", "namedAggregationArg"
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
			"BOOLEAN", "INTEGER", "FLOAT", "STRING", "MISSING", "MATH_CONSTANT", 
			"ROW_INDEX", "ROW_NUMBER", "ROW_ID", "PLUS", "MINUS", "MULTIPLY", "DIVIDE", 
			"FLOOR_DIVIDE", "EXPONENTIATE", "MODULO", "LESS_THAN", "LESS_THAN_EQUAL", 
			"GREATER_THAN", "GREATER_THAN_EQUAL", "EQUAL", "NOT_EQUAL", "AND", "OR", 
			"NOT", "MISSING_FALLBACK", "AGGREGATION_IDENTIFIER", "FUNCTION_IDENTIFIER", 
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
			setState(16);
			expr(0);
			setState(17);
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
		public TerminalNode INTEGER() { return getToken(KnimeExpressionParser.INTEGER, 0); }
		public TerminalNode FLOAT() { return getToken(KnimeExpressionParser.FLOAT, 0); }
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
			setState(19);
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
		public TerminalNode INTEGER() { return getToken(KnimeExpressionParser.INTEGER, 0); }
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
			setState(73);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case T__0:
			case FLOW_VAR_IDENTIFIER:
				{
				_localctx = new FlowVarAccessContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(30);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case FLOW_VAR_IDENTIFIER:
					{
					setState(22);
					((FlowVarAccessContext)_localctx).shortName = match(FLOW_VAR_IDENTIFIER);
					}
					break;
				case T__0:
					{
					setState(24); 
					_errHandler.sync(this);
					_la = _input.LA(1);
					do {
						{
						{
						setState(23);
						match(T__0);
						}
						}
						setState(26); 
						_errHandler.sync(this);
						_la = _input.LA(1);
					} while ( _la==T__0 );
					setState(28);
					((FlowVarAccessContext)_localctx).longName = match(STRING);
					setState(29);
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
				setState(51);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case COLUMN_IDENTIFIER:
					{
					setState(32);
					((ColAccessContext)_localctx).shortName = match(COLUMN_IDENTIFIER);
					}
					break;
				case T__2:
					{
					setState(34); 
					_errHandler.sync(this);
					_la = _input.LA(1);
					do {
						{
						{
						setState(33);
						match(T__2);
						}
						}
						setState(36); 
						_errHandler.sync(this);
						_la = _input.LA(1);
					} while ( _la==T__2 );
					setState(38);
					((ColAccessContext)_localctx).longName = match(STRING);
					setState(48);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==T__3) {
						{
						setState(40); 
						_errHandler.sync(this);
						_la = _input.LA(1);
						do {
							{
							{
							setState(39);
							match(T__3);
							}
							}
							setState(42); 
							_errHandler.sync(this);
							_la = _input.LA(1);
						} while ( _la==T__3 );
						setState(45);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==MINUS) {
							{
							setState(44);
							((ColAccessContext)_localctx).minus = match(MINUS);
							}
						}

						setState(47);
						((ColAccessContext)_localctx).offset = match(INTEGER);
						}
					}

					setState(50);
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
				setState(53);
				((FunctionCallContext)_localctx).name = match(FUNCTION_IDENTIFIER);
				setState(54);
				match(T__4);
				setState(56);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 523986796074L) != 0)) {
					{
					setState(55);
					functionArgs();
					}
				}

				setState(58);
				match(T__5);
				}
				break;
			case AGGREGATION_IDENTIFIER:
				{
				_localctx = new AggregationCallContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(59);
				((AggregationCallContext)_localctx).name = match(AGGREGATION_IDENTIFIER);
				setState(60);
				match(T__4);
				setState(61);
				aggregationArgs();
				setState(62);
				match(T__5);
				}
				break;
			case MINUS:
				{
				_localctx = new UnaryOpContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(64);
				((UnaryOpContext)_localctx).op = match(MINUS);
				setState(65);
				expr(9);
				}
				break;
			case NOT:
				{
				_localctx = new UnaryOpContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(66);
				((UnaryOpContext)_localctx).op = match(NOT);
				setState(67);
				expr(5);
				}
				break;
			case T__4:
				{
				_localctx = new ParenthesisedExprContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(68);
				match(T__4);
				setState(69);
				((ParenthesisedExprContext)_localctx).inner = expr(0);
				setState(70);
				match(T__5);
				}
				break;
			case BOOLEAN:
			case INTEGER:
			case FLOAT:
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
				setState(72);
				atom();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			_ctx.stop = _input.LT(-1);
			setState(98);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,10,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(96);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,9,_ctx) ) {
					case 1:
						{
						_localctx = new BinaryOpContext(new ExprContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_expr);
						setState(75);
						if (!(precpred(_ctx, 11))) throw new FailedPredicateException(this, "precpred(_ctx, 11)");
						setState(76);
						((BinaryOpContext)_localctx).op = match(MISSING_FALLBACK);
						setState(77);
						expr(12);
						}
						break;
					case 2:
						{
						_localctx = new BinaryOpContext(new ExprContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_expr);
						setState(78);
						if (!(precpred(_ctx, 10))) throw new FailedPredicateException(this, "precpred(_ctx, 10)");
						setState(79);
						((BinaryOpContext)_localctx).op = match(EXPONENTIATE);
						setState(80);
						expr(10);
						}
						break;
					case 3:
						{
						_localctx = new BinaryOpContext(new ExprContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_expr);
						setState(81);
						if (!(precpred(_ctx, 8))) throw new FailedPredicateException(this, "precpred(_ctx, 8)");
						setState(82);
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
						setState(83);
						expr(9);
						}
						break;
					case 4:
						{
						_localctx = new BinaryOpContext(new ExprContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_expr);
						setState(84);
						if (!(precpred(_ctx, 7))) throw new FailedPredicateException(this, "precpred(_ctx, 7)");
						setState(85);
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
						setState(86);
						expr(8);
						}
						break;
					case 5:
						{
						_localctx = new BinaryOpContext(new ExprContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_expr);
						setState(87);
						if (!(precpred(_ctx, 6))) throw new FailedPredicateException(this, "precpred(_ctx, 6)");
						setState(88);
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
						setState(89);
						expr(7);
						}
						break;
					case 6:
						{
						_localctx = new BinaryOpContext(new ExprContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_expr);
						setState(90);
						if (!(precpred(_ctx, 4))) throw new FailedPredicateException(this, "precpred(_ctx, 4)");
						setState(91);
						((BinaryOpContext)_localctx).op = match(AND);
						setState(92);
						expr(5);
						}
						break;
					case 7:
						{
						_localctx = new BinaryOpContext(new ExprContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_expr);
						setState(93);
						if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
						setState(94);
						((BinaryOpContext)_localctx).op = match(OR);
						setState(95);
						expr(4);
						}
						break;
					}
					} 
				}
				setState(100);
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
			setState(101);
			expr(0);
			setState(106);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,11,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(102);
					match(T__3);
					setState(103);
					expr(0);
					}
					} 
				}
				setState(108);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,11,_ctx);
			}
			setState(110);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==T__3) {
				{
				setState(109);
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
			setState(124);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case BOOLEAN:
			case INTEGER:
			case FLOAT:
			case STRING:
			case MISSING:
			case MATH_CONSTANT:
			case ROW_INDEX:
			case ROW_NUMBER:
			case ROW_ID:
				enterOuterAlt(_localctx, 1);
				{
				{
				setState(112);
				positionalAggregationArgs();
				setState(115);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,13,_ctx) ) {
				case 1:
					{
					setState(113);
					match(T__3);
					setState(114);
					namedAggregationArgs();
					}
					break;
				}
				setState(118);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__3) {
					{
					setState(117);
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
				setState(120);
				namedAggregationArgs();
				setState(122);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__3) {
					{
					setState(121);
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
		public List<AtomContext> atom() {
			return getRuleContexts(AtomContext.class);
		}
		public AtomContext atom(int i) {
			return getRuleContext(AtomContext.class,i);
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
			setState(126);
			atom();
			setState(131);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,17,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(127);
					match(T__3);
					setState(128);
					atom();
					}
					} 
				}
				setState(133);
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
		enterRule(_localctx, 12, RULE_namedAggregationArgs);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(134);
			namedAggregationArg();
			setState(139);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,18,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(135);
					match(T__3);
					setState(136);
					namedAggregationArg();
					}
					} 
				}
				setState(141);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,18,_ctx);
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
		public AtomContext atom() {
			return getRuleContext(AtomContext.class,0);
		}
		public TerminalNode NAMED_ARGUMENT_IDENTIFIER() { return getToken(KnimeExpressionParser.NAMED_ARGUMENT_IDENTIFIER, 0); }
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
		enterRule(_localctx, 14, RULE_namedAggregationArg);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(142);
			((NamedAggregationArgContext)_localctx).argName = match(NAMED_ARGUMENT_IDENTIFIER);
			setState(143);
			atom();
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
		"\u0004\u0001\'\u0092\u0002\u0000\u0007\u0000\u0002\u0001\u0007\u0001\u0002"+
		"\u0002\u0007\u0002\u0002\u0003\u0007\u0003\u0002\u0004\u0007\u0004\u0002"+
		"\u0005\u0007\u0005\u0002\u0006\u0007\u0006\u0002\u0007\u0007\u0007\u0001"+
		"\u0000\u0001\u0000\u0001\u0000\u0001\u0001\u0001\u0001\u0001\u0002\u0001"+
		"\u0002\u0001\u0002\u0004\u0002\u0019\b\u0002\u000b\u0002\f\u0002\u001a"+
		"\u0001\u0002\u0001\u0002\u0003\u0002\u001f\b\u0002\u0001\u0002\u0001\u0002"+
		"\u0004\u0002#\b\u0002\u000b\u0002\f\u0002$\u0001\u0002\u0001\u0002\u0004"+
		"\u0002)\b\u0002\u000b\u0002\f\u0002*\u0001\u0002\u0003\u0002.\b\u0002"+
		"\u0001\u0002\u0003\u00021\b\u0002\u0001\u0002\u0003\u00024\b\u0002\u0001"+
		"\u0002\u0001\u0002\u0001\u0002\u0003\u00029\b\u0002\u0001\u0002\u0001"+
		"\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001"+
		"\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001"+
		"\u0002\u0001\u0002\u0003\u0002J\b\u0002\u0001\u0002\u0001\u0002\u0001"+
		"\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001"+
		"\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001"+
		"\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0001"+
		"\u0002\u0005\u0002a\b\u0002\n\u0002\f\u0002d\t\u0002\u0001\u0003\u0001"+
		"\u0003\u0001\u0003\u0005\u0003i\b\u0003\n\u0003\f\u0003l\t\u0003\u0001"+
		"\u0003\u0003\u0003o\b\u0003\u0001\u0004\u0001\u0004\u0001\u0004\u0003"+
		"\u0004t\b\u0004\u0001\u0004\u0003\u0004w\b\u0004\u0001\u0004\u0001\u0004"+
		"\u0003\u0004{\b\u0004\u0003\u0004}\b\u0004\u0001\u0005\u0001\u0005\u0001"+
		"\u0005\u0005\u0005\u0082\b\u0005\n\u0005\f\u0005\u0085\t\u0005\u0001\u0006"+
		"\u0001\u0006\u0001\u0006\u0005\u0006\u008a\b\u0006\n\u0006\f\u0006\u008d"+
		"\t\u0006\u0001\u0007\u0001\u0007\u0001\u0007\u0001\u0007\u0000\u0001\u0004"+
		"\b\u0000\u0002\u0004\u0006\b\n\f\u000e\u0000\u0004\u0001\u0000\t\u0011"+
		"\u0002\u0000\u0014\u0016\u0018\u0018\u0001\u0000\u0012\u0013\u0001\u0000"+
		"\u0019\u001e\u00a7\u0000\u0010\u0001\u0000\u0000\u0000\u0002\u0013\u0001"+
		"\u0000\u0000\u0000\u0004I\u0001\u0000\u0000\u0000\u0006e\u0001\u0000\u0000"+
		"\u0000\b|\u0001\u0000\u0000\u0000\n~\u0001\u0000\u0000\u0000\f\u0086\u0001"+
		"\u0000\u0000\u0000\u000e\u008e\u0001\u0000\u0000\u0000\u0010\u0011\u0003"+
		"\u0004\u0002\u0000\u0011\u0012\u0005\u0000\u0000\u0001\u0012\u0001\u0001"+
		"\u0000\u0000\u0000\u0013\u0014\u0007\u0000\u0000\u0000\u0014\u0003\u0001"+
		"\u0000\u0000\u0000\u0015\u001e\u0006\u0002\uffff\uffff\u0000\u0016\u001f"+
		"\u0005&\u0000\u0000\u0017\u0019\u0005\u0001\u0000\u0000\u0018\u0017\u0001"+
		"\u0000\u0000\u0000\u0019\u001a\u0001\u0000\u0000\u0000\u001a\u0018\u0001"+
		"\u0000\u0000\u0000\u001a\u001b\u0001\u0000\u0000\u0000\u001b\u001c\u0001"+
		"\u0000\u0000\u0000\u001c\u001d\u0005\f\u0000\u0000\u001d\u001f\u0005\u0002"+
		"\u0000\u0000\u001e\u0016\u0001\u0000\u0000\u0000\u001e\u0018\u0001\u0000"+
		"\u0000\u0000\u001fJ\u0001\u0000\u0000\u0000 4\u0005%\u0000\u0000!#\u0005"+
		"\u0003\u0000\u0000\"!\u0001\u0000\u0000\u0000#$\u0001\u0000\u0000\u0000"+
		"$\"\u0001\u0000\u0000\u0000$%\u0001\u0000\u0000\u0000%&\u0001\u0000\u0000"+
		"\u0000&0\u0005\f\u0000\u0000\')\u0005\u0004\u0000\u0000(\'\u0001\u0000"+
		"\u0000\u0000)*\u0001\u0000\u0000\u0000*(\u0001\u0000\u0000\u0000*+\u0001"+
		"\u0000\u0000\u0000+-\u0001\u0000\u0000\u0000,.\u0005\u0013\u0000\u0000"+
		"-,\u0001\u0000\u0000\u0000-.\u0001\u0000\u0000\u0000./\u0001\u0000\u0000"+
		"\u0000/1\u0005\n\u0000\u00000(\u0001\u0000\u0000\u000001\u0001\u0000\u0000"+
		"\u000012\u0001\u0000\u0000\u000024\u0005\u0002\u0000\u00003 \u0001\u0000"+
		"\u0000\u00003\"\u0001\u0000\u0000\u00004J\u0001\u0000\u0000\u000056\u0005"+
		"$\u0000\u000068\u0005\u0005\u0000\u000079\u0003\u0006\u0003\u000087\u0001"+
		"\u0000\u0000\u000089\u0001\u0000\u0000\u00009:\u0001\u0000\u0000\u0000"+
		":J\u0005\u0006\u0000\u0000;<\u0005#\u0000\u0000<=\u0005\u0005\u0000\u0000"+
		"=>\u0003\b\u0004\u0000>?\u0005\u0006\u0000\u0000?J\u0001\u0000\u0000\u0000"+
		"@A\u0005\u0013\u0000\u0000AJ\u0003\u0004\u0002\tBC\u0005!\u0000\u0000"+
		"CJ\u0003\u0004\u0002\u0005DE\u0005\u0005\u0000\u0000EF\u0003\u0004\u0002"+
		"\u0000FG\u0005\u0006\u0000\u0000GJ\u0001\u0000\u0000\u0000HJ\u0003\u0002"+
		"\u0001\u0000I\u0015\u0001\u0000\u0000\u0000I3\u0001\u0000\u0000\u0000"+
		"I5\u0001\u0000\u0000\u0000I;\u0001\u0000\u0000\u0000I@\u0001\u0000\u0000"+
		"\u0000IB\u0001\u0000\u0000\u0000ID\u0001\u0000\u0000\u0000IH\u0001\u0000"+
		"\u0000\u0000Jb\u0001\u0000\u0000\u0000KL\n\u000b\u0000\u0000LM\u0005\""+
		"\u0000\u0000Ma\u0003\u0004\u0002\fNO\n\n\u0000\u0000OP\u0005\u0017\u0000"+
		"\u0000Pa\u0003\u0004\u0002\nQR\n\b\u0000\u0000RS\u0007\u0001\u0000\u0000"+
		"Sa\u0003\u0004\u0002\tTU\n\u0007\u0000\u0000UV\u0007\u0002\u0000\u0000"+
		"Va\u0003\u0004\u0002\bWX\n\u0006\u0000\u0000XY\u0007\u0003\u0000\u0000"+
		"Ya\u0003\u0004\u0002\u0007Z[\n\u0004\u0000\u0000[\\\u0005\u001f\u0000"+
		"\u0000\\a\u0003\u0004\u0002\u0005]^\n\u0003\u0000\u0000^_\u0005 \u0000"+
		"\u0000_a\u0003\u0004\u0002\u0004`K\u0001\u0000\u0000\u0000`N\u0001\u0000"+
		"\u0000\u0000`Q\u0001\u0000\u0000\u0000`T\u0001\u0000\u0000\u0000`W\u0001"+
		"\u0000\u0000\u0000`Z\u0001\u0000\u0000\u0000`]\u0001\u0000\u0000\u0000"+
		"ad\u0001\u0000\u0000\u0000b`\u0001\u0000\u0000\u0000bc\u0001\u0000\u0000"+
		"\u0000c\u0005\u0001\u0000\u0000\u0000db\u0001\u0000\u0000\u0000ej\u0003"+
		"\u0004\u0002\u0000fg\u0005\u0004\u0000\u0000gi\u0003\u0004\u0002\u0000"+
		"hf\u0001\u0000\u0000\u0000il\u0001\u0000\u0000\u0000jh\u0001\u0000\u0000"+
		"\u0000jk\u0001\u0000\u0000\u0000kn\u0001\u0000\u0000\u0000lj\u0001\u0000"+
		"\u0000\u0000mo\u0005\u0004\u0000\u0000nm\u0001\u0000\u0000\u0000no\u0001"+
		"\u0000\u0000\u0000o\u0007\u0001\u0000\u0000\u0000ps\u0003\n\u0005\u0000"+
		"qr\u0005\u0004\u0000\u0000rt\u0003\f\u0006\u0000sq\u0001\u0000\u0000\u0000"+
		"st\u0001\u0000\u0000\u0000tv\u0001\u0000\u0000\u0000uw\u0005\u0004\u0000"+
		"\u0000vu\u0001\u0000\u0000\u0000vw\u0001\u0000\u0000\u0000w}\u0001\u0000"+
		"\u0000\u0000xz\u0003\f\u0006\u0000y{\u0005\u0004\u0000\u0000zy\u0001\u0000"+
		"\u0000\u0000z{\u0001\u0000\u0000\u0000{}\u0001\u0000\u0000\u0000|p\u0001"+
		"\u0000\u0000\u0000|x\u0001\u0000\u0000\u0000}\t\u0001\u0000\u0000\u0000"+
		"~\u0083\u0003\u0002\u0001\u0000\u007f\u0080\u0005\u0004\u0000\u0000\u0080"+
		"\u0082\u0003\u0002\u0001\u0000\u0081\u007f\u0001\u0000\u0000\u0000\u0082"+
		"\u0085\u0001\u0000\u0000\u0000\u0083\u0081\u0001\u0000\u0000\u0000\u0083"+
		"\u0084\u0001\u0000\u0000\u0000\u0084\u000b\u0001\u0000\u0000\u0000\u0085"+
		"\u0083\u0001\u0000\u0000\u0000\u0086\u008b\u0003\u000e\u0007\u0000\u0087"+
		"\u0088\u0005\u0004\u0000\u0000\u0088\u008a\u0003\u000e\u0007\u0000\u0089"+
		"\u0087\u0001\u0000\u0000\u0000\u008a\u008d\u0001\u0000\u0000\u0000\u008b"+
		"\u0089\u0001\u0000\u0000\u0000\u008b\u008c\u0001\u0000\u0000\u0000\u008c"+
		"\r\u0001\u0000\u0000\u0000\u008d\u008b\u0001\u0000\u0000\u0000\u008e\u008f"+
		"\u0005\'\u0000\u0000\u008f\u0090\u0003\u0002\u0001\u0000\u0090\u000f\u0001"+
		"\u0000\u0000\u0000\u0013\u001a\u001e$*-038I`bjnsvz|\u0083\u008b";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}