// Generated from KnimeExpression.g4 by ANTLR 4.13.1
package org.knime.core.expressions.antlr;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.misc.*;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast", "CheckReturnValue", "this-escape"})
public class KnimeExpressionLexer extends Lexer {
	static { RuntimeMetaData.checkVersion("4.13.1", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__0=1, T__1=2, T__2=3, T__3=4, T__4=5, T__5=6, LINE_COMMENT=7, WHITESPACE=8, 
		BOOLEAN=9, INTEGER=10, FLOAT=11, STRING=12, MISSING=13, PLUS=14, MINUS=15, 
		MULTIPLY=16, DIVIDE=17, FLOOR_DIVIDE=18, EXPONENTIATE=19, MODULO=20, LESS_THAN=21, 
		LESS_THAN_EQUAL=22, GREATER_THAN=23, GREATER_THAN_EQUAL=24, EQUAL=25, 
		NOT_EQUAL=26, AND=27, OR=28, NOT=29, MISSING_FALLBACK=30, AGGREGATION_IDENTIFIER=31, 
		FUNCTION_IDENTIFIER=32, COLUMN_IDENTIFIER=33, FLOW_VAR_IDENTIFIER=34, 
		NAMED_ARGUMENT_IDENTIFIER=35;
	public static String[] channelNames = {
		"DEFAULT_TOKEN_CHANNEL", "HIDDEN"
	};

	public static String[] modeNames = {
		"DEFAULT_MODE"
	};

	private static String[] makeRuleNames() {
		return new String[] {
			"T__0", "T__1", "T__2", "T__3", "T__4", "T__5", "LINE_COMMENT", "WHITESPACE", 
			"BOOLEAN", "INTEGER", "NON_ZERO_DIGIT", "DIGIT", "FLOAT", "POINT_FLOAT", 
			"EXPONENT_FLOAT", "DIGIT_PART", "EXPONENT", "FRACTION", "STRING", "ESC", 
			"UNICODE_16", "HEX_DIGIT", "MISSING", "PLUS", "MINUS", "MULTIPLY", "DIVIDE", 
			"FLOOR_DIVIDE", "EXPONENTIATE", "MODULO", "LESS_THAN", "LESS_THAN_EQUAL", 
			"GREATER_THAN", "GREATER_THAN_EQUAL", "EQUAL", "NOT_EQUAL", "AND", "OR", 
			"NOT", "MISSING_FALLBACK", "AGGREGATION_IDENTIFIER", "FUNCTION_IDENTIFIER", 
			"COLUMN_IDENTIFIER", "FLOW_VAR_IDENTIFIER", "NAMED_ARGUMENT_IDENTIFIER"
		};
	}
	public static final String[] ruleNames = makeRuleNames();

	private static String[] makeLiteralNames() {
		return new String[] {
			null, "'$$['", "']'", "'$['", "'('", "')'", "','", null, null, null, 
			null, null, null, "'MISSING'", "'+'", "'-'", "'*'", "'/'", "'//'", "'**'", 
			"'%'", "'<'", "'<='", "'>'", "'>='", null, null, "'and'", "'or'", "'not'", 
			"'??'"
		};
	}
	private static final String[] _LITERAL_NAMES = makeLiteralNames();
	private static String[] makeSymbolicNames() {
		return new String[] {
			null, null, null, null, null, null, null, "LINE_COMMENT", "WHITESPACE", 
			"BOOLEAN", "INTEGER", "FLOAT", "STRING", "MISSING", "PLUS", "MINUS", 
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


	public KnimeExpressionLexer(CharStream input) {
		super(input);
		_interp = new LexerATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	@Override
	public String getGrammarFileName() { return "KnimeExpression.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public String[] getChannelNames() { return channelNames; }

	@Override
	public String[] getModeNames() { return modeNames; }

	@Override
	public ATN getATN() { return _ATN; }

	public static final String _serializedATN =
		"\u0004\u0000#\u013f\u0006\uffff\uffff\u0002\u0000\u0007\u0000\u0002\u0001"+
		"\u0007\u0001\u0002\u0002\u0007\u0002\u0002\u0003\u0007\u0003\u0002\u0004"+
		"\u0007\u0004\u0002\u0005\u0007\u0005\u0002\u0006\u0007\u0006\u0002\u0007"+
		"\u0007\u0007\u0002\b\u0007\b\u0002\t\u0007\t\u0002\n\u0007\n\u0002\u000b"+
		"\u0007\u000b\u0002\f\u0007\f\u0002\r\u0007\r\u0002\u000e\u0007\u000e\u0002"+
		"\u000f\u0007\u000f\u0002\u0010\u0007\u0010\u0002\u0011\u0007\u0011\u0002"+
		"\u0012\u0007\u0012\u0002\u0013\u0007\u0013\u0002\u0014\u0007\u0014\u0002"+
		"\u0015\u0007\u0015\u0002\u0016\u0007\u0016\u0002\u0017\u0007\u0017\u0002"+
		"\u0018\u0007\u0018\u0002\u0019\u0007\u0019\u0002\u001a\u0007\u001a\u0002"+
		"\u001b\u0007\u001b\u0002\u001c\u0007\u001c\u0002\u001d\u0007\u001d\u0002"+
		"\u001e\u0007\u001e\u0002\u001f\u0007\u001f\u0002 \u0007 \u0002!\u0007"+
		"!\u0002\"\u0007\"\u0002#\u0007#\u0002$\u0007$\u0002%\u0007%\u0002&\u0007"+
		"&\u0002\'\u0007\'\u0002(\u0007(\u0002)\u0007)\u0002*\u0007*\u0002+\u0007"+
		"+\u0002,\u0007,\u0001\u0000\u0001\u0000\u0001\u0000\u0001\u0000\u0001"+
		"\u0001\u0001\u0001\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0003\u0001"+
		"\u0003\u0001\u0004\u0001\u0004\u0001\u0005\u0001\u0005\u0001\u0006\u0001"+
		"\u0006\u0005\u0006m\b\u0006\n\u0006\f\u0006p\t\u0006\u0001\u0006\u0001"+
		"\u0006\u0001\u0007\u0004\u0007u\b\u0007\u000b\u0007\f\u0007v\u0001\u0007"+
		"\u0001\u0007\u0001\b\u0001\b\u0001\b\u0001\b\u0001\b\u0001\b\u0001\b\u0001"+
		"\b\u0001\b\u0003\b\u0084\b\b\u0001\t\u0001\t\u0001\t\u0003\t\u0089\b\t"+
		"\u0001\t\u0005\t\u008c\b\t\n\t\f\t\u008f\t\t\u0003\t\u0091\b\t\u0001\n"+
		"\u0001\n\u0001\u000b\u0001\u000b\u0001\f\u0001\f\u0003\f\u0099\b\f\u0001"+
		"\r\u0003\r\u009c\b\r\u0001\r\u0001\r\u0001\r\u0001\r\u0003\r\u00a2\b\r"+
		"\u0001\u000e\u0001\u000e\u0003\u000e\u00a6\b\u000e\u0001\u000e\u0001\u000e"+
		"\u0001\u000f\u0001\u000f\u0003\u000f\u00ac\b\u000f\u0001\u000f\u0005\u000f"+
		"\u00af\b\u000f\n\u000f\f\u000f\u00b2\t\u000f\u0001\u0010\u0001\u0010\u0003"+
		"\u0010\u00b6\b\u0010\u0001\u0010\u0001\u0010\u0001\u0011\u0001\u0011\u0001"+
		"\u0011\u0001\u0012\u0001\u0012\u0001\u0012\u0005\u0012\u00c0\b\u0012\n"+
		"\u0012\f\u0012\u00c3\t\u0012\u0001\u0012\u0001\u0012\u0001\u0012\u0001"+
		"\u0012\u0005\u0012\u00c9\b\u0012\n\u0012\f\u0012\u00cc\t\u0012\u0001\u0012"+
		"\u0003\u0012\u00cf\b\u0012\u0001\u0013\u0001\u0013\u0001\u0013\u0003\u0013"+
		"\u00d4\b\u0013\u0001\u0014\u0001\u0014\u0001\u0014\u0001\u0014\u0001\u0014"+
		"\u0001\u0014\u0001\u0015\u0001\u0015\u0001\u0016\u0001\u0016\u0001\u0016"+
		"\u0001\u0016\u0001\u0016\u0001\u0016\u0001\u0016\u0001\u0016\u0001\u0017"+
		"\u0001\u0017\u0001\u0018\u0001\u0018\u0001\u0019\u0001\u0019\u0001\u001a"+
		"\u0001\u001a\u0001\u001b\u0001\u001b\u0001\u001b\u0001\u001c\u0001\u001c"+
		"\u0001\u001c\u0001\u001d\u0001\u001d\u0001\u001e\u0001\u001e\u0001\u001f"+
		"\u0001\u001f\u0001\u001f\u0001 \u0001 \u0001!\u0001!\u0001!\u0001\"\u0001"+
		"\"\u0001\"\u0003\"\u0103\b\"\u0001#\u0001#\u0001#\u0001#\u0003#\u0109"+
		"\b#\u0001$\u0001$\u0001$\u0001$\u0001%\u0001%\u0001%\u0001&\u0001&\u0001"+
		"&\u0001&\u0001\'\u0001\'\u0001\'\u0001(\u0001(\u0005(\u011b\b(\n(\f(\u011e"+
		"\t(\u0001)\u0001)\u0005)\u0122\b)\n)\f)\u0125\t)\u0001*\u0001*\u0005*"+
		"\u0129\b*\n*\f*\u012c\t*\u0001+\u0001+\u0001+\u0001+\u0005+\u0132\b+\n"+
		"+\f+\u0135\t+\u0001,\u0001,\u0005,\u0139\b,\n,\f,\u013c\t,\u0001,\u0001"+
		",\u0000\u0000-\u0001\u0001\u0003\u0002\u0005\u0003\u0007\u0004\t\u0005"+
		"\u000b\u0006\r\u0007\u000f\b\u0011\t\u0013\n\u0015\u0000\u0017\u0000\u0019"+
		"\u000b\u001b\u0000\u001d\u0000\u001f\u0000!\u0000#\u0000%\f\'\u0000)\u0000"+
		"+\u0000-\r/\u000e1\u000f3\u00105\u00117\u00129\u0013;\u0014=\u0015?\u0016"+
		"A\u0017C\u0018E\u0019G\u001aI\u001bK\u001cM\u001dO\u001eQ\u001fS U!W\""+
		"Y#\u0001\u0000\u000f\u0002\u0000\n\n\r\r\u0003\u0000\t\n\r\r  \u0001\u0000"+
		"19\u0001\u000009\u0002\u0000EEee\u0002\u0000++--\u0002\u0000\"\"\\\\\u0002"+
		"\u0000\'\'\\\\\n\u0000\n\n\"\"\'\'\\\\abffnnrrttvv\u0003\u000009AFaf\u0001"+
		"\u0000AZ\u0003\u000009AZ__\u0001\u0000az\u0003\u000009__az\u0004\u0000"+
		"09AZ__az\u014e\u0000\u0001\u0001\u0000\u0000\u0000\u0000\u0003\u0001\u0000"+
		"\u0000\u0000\u0000\u0005\u0001\u0000\u0000\u0000\u0000\u0007\u0001\u0000"+
		"\u0000\u0000\u0000\t\u0001\u0000\u0000\u0000\u0000\u000b\u0001\u0000\u0000"+
		"\u0000\u0000\r\u0001\u0000\u0000\u0000\u0000\u000f\u0001\u0000\u0000\u0000"+
		"\u0000\u0011\u0001\u0000\u0000\u0000\u0000\u0013\u0001\u0000\u0000\u0000"+
		"\u0000\u0019\u0001\u0000\u0000\u0000\u0000%\u0001\u0000\u0000\u0000\u0000"+
		"-\u0001\u0000\u0000\u0000\u0000/\u0001\u0000\u0000\u0000\u00001\u0001"+
		"\u0000\u0000\u0000\u00003\u0001\u0000\u0000\u0000\u00005\u0001\u0000\u0000"+
		"\u0000\u00007\u0001\u0000\u0000\u0000\u00009\u0001\u0000\u0000\u0000\u0000"+
		";\u0001\u0000\u0000\u0000\u0000=\u0001\u0000\u0000\u0000\u0000?\u0001"+
		"\u0000\u0000\u0000\u0000A\u0001\u0000\u0000\u0000\u0000C\u0001\u0000\u0000"+
		"\u0000\u0000E\u0001\u0000\u0000\u0000\u0000G\u0001\u0000\u0000\u0000\u0000"+
		"I\u0001\u0000\u0000\u0000\u0000K\u0001\u0000\u0000\u0000\u0000M\u0001"+
		"\u0000\u0000\u0000\u0000O\u0001\u0000\u0000\u0000\u0000Q\u0001\u0000\u0000"+
		"\u0000\u0000S\u0001\u0000\u0000\u0000\u0000U\u0001\u0000\u0000\u0000\u0000"+
		"W\u0001\u0000\u0000\u0000\u0000Y\u0001\u0000\u0000\u0000\u0001[\u0001"+
		"\u0000\u0000\u0000\u0003_\u0001\u0000\u0000\u0000\u0005a\u0001\u0000\u0000"+
		"\u0000\u0007d\u0001\u0000\u0000\u0000\tf\u0001\u0000\u0000\u0000\u000b"+
		"h\u0001\u0000\u0000\u0000\rj\u0001\u0000\u0000\u0000\u000ft\u0001\u0000"+
		"\u0000\u0000\u0011\u0083\u0001\u0000\u0000\u0000\u0013\u0090\u0001\u0000"+
		"\u0000\u0000\u0015\u0092\u0001\u0000\u0000\u0000\u0017\u0094\u0001\u0000"+
		"\u0000\u0000\u0019\u0098\u0001\u0000\u0000\u0000\u001b\u00a1\u0001\u0000"+
		"\u0000\u0000\u001d\u00a5\u0001\u0000\u0000\u0000\u001f\u00a9\u0001\u0000"+
		"\u0000\u0000!\u00b3\u0001\u0000\u0000\u0000#\u00b9\u0001\u0000\u0000\u0000"+
		"%\u00ce\u0001\u0000\u0000\u0000\'\u00d0\u0001\u0000\u0000\u0000)\u00d5"+
		"\u0001\u0000\u0000\u0000+\u00db\u0001\u0000\u0000\u0000-\u00dd\u0001\u0000"+
		"\u0000\u0000/\u00e5\u0001\u0000\u0000\u00001\u00e7\u0001\u0000\u0000\u0000"+
		"3\u00e9\u0001\u0000\u0000\u00005\u00eb\u0001\u0000\u0000\u00007\u00ed"+
		"\u0001\u0000\u0000\u00009\u00f0\u0001\u0000\u0000\u0000;\u00f3\u0001\u0000"+
		"\u0000\u0000=\u00f5\u0001\u0000\u0000\u0000?\u00f7\u0001\u0000\u0000\u0000"+
		"A\u00fa\u0001\u0000\u0000\u0000C\u00fc\u0001\u0000\u0000\u0000E\u0102"+
		"\u0001\u0000\u0000\u0000G\u0108\u0001\u0000\u0000\u0000I\u010a\u0001\u0000"+
		"\u0000\u0000K\u010e\u0001\u0000\u0000\u0000M\u0111\u0001\u0000\u0000\u0000"+
		"O\u0115\u0001\u0000\u0000\u0000Q\u0118\u0001\u0000\u0000\u0000S\u011f"+
		"\u0001\u0000\u0000\u0000U\u0126\u0001\u0000\u0000\u0000W\u012d\u0001\u0000"+
		"\u0000\u0000Y\u0136\u0001\u0000\u0000\u0000[\\\u0005$\u0000\u0000\\]\u0005"+
		"$\u0000\u0000]^\u0005[\u0000\u0000^\u0002\u0001\u0000\u0000\u0000_`\u0005"+
		"]\u0000\u0000`\u0004\u0001\u0000\u0000\u0000ab\u0005$\u0000\u0000bc\u0005"+
		"[\u0000\u0000c\u0006\u0001\u0000\u0000\u0000de\u0005(\u0000\u0000e\b\u0001"+
		"\u0000\u0000\u0000fg\u0005)\u0000\u0000g\n\u0001\u0000\u0000\u0000hi\u0005"+
		",\u0000\u0000i\f\u0001\u0000\u0000\u0000jn\u0005#\u0000\u0000km\b\u0000"+
		"\u0000\u0000lk\u0001\u0000\u0000\u0000mp\u0001\u0000\u0000\u0000nl\u0001"+
		"\u0000\u0000\u0000no\u0001\u0000\u0000\u0000oq\u0001\u0000\u0000\u0000"+
		"pn\u0001\u0000\u0000\u0000qr\u0006\u0006\u0000\u0000r\u000e\u0001\u0000"+
		"\u0000\u0000su\u0007\u0001\u0000\u0000ts\u0001\u0000\u0000\u0000uv\u0001"+
		"\u0000\u0000\u0000vt\u0001\u0000\u0000\u0000vw\u0001\u0000\u0000\u0000"+
		"wx\u0001\u0000\u0000\u0000xy\u0006\u0007\u0000\u0000y\u0010\u0001\u0000"+
		"\u0000\u0000z{\u0005t\u0000\u0000{|\u0005r\u0000\u0000|}\u0005u\u0000"+
		"\u0000}\u0084\u0005e\u0000\u0000~\u007f\u0005f\u0000\u0000\u007f\u0080"+
		"\u0005a\u0000\u0000\u0080\u0081\u0005l\u0000\u0000\u0081\u0082\u0005s"+
		"\u0000\u0000\u0082\u0084\u0005e\u0000\u0000\u0083z\u0001\u0000\u0000\u0000"+
		"\u0083~\u0001\u0000\u0000\u0000\u0084\u0012\u0001\u0000\u0000\u0000\u0085"+
		"\u0091\u00050\u0000\u0000\u0086\u008d\u0003\u0015\n\u0000\u0087\u0089"+
		"\u0005_\u0000\u0000\u0088\u0087\u0001\u0000\u0000\u0000\u0088\u0089\u0001"+
		"\u0000\u0000\u0000\u0089\u008a\u0001\u0000\u0000\u0000\u008a\u008c\u0003"+
		"\u0017\u000b\u0000\u008b\u0088\u0001\u0000\u0000\u0000\u008c\u008f\u0001"+
		"\u0000\u0000\u0000\u008d\u008b\u0001\u0000\u0000\u0000\u008d\u008e\u0001"+
		"\u0000\u0000\u0000\u008e\u0091\u0001\u0000\u0000\u0000\u008f\u008d\u0001"+
		"\u0000\u0000\u0000\u0090\u0085\u0001\u0000\u0000\u0000\u0090\u0086\u0001"+
		"\u0000\u0000\u0000\u0091\u0014\u0001\u0000\u0000\u0000\u0092\u0093\u0007"+
		"\u0002\u0000\u0000\u0093\u0016\u0001\u0000\u0000\u0000\u0094\u0095\u0007"+
		"\u0003\u0000\u0000\u0095\u0018\u0001\u0000\u0000\u0000\u0096\u0099\u0003"+
		"\u001b\r\u0000\u0097\u0099\u0003\u001d\u000e\u0000\u0098\u0096\u0001\u0000"+
		"\u0000\u0000\u0098\u0097\u0001\u0000\u0000\u0000\u0099\u001a\u0001\u0000"+
		"\u0000\u0000\u009a\u009c\u0003\u001f\u000f\u0000\u009b\u009a\u0001\u0000"+
		"\u0000\u0000\u009b\u009c\u0001\u0000\u0000\u0000\u009c\u009d\u0001\u0000"+
		"\u0000\u0000\u009d\u00a2\u0003#\u0011\u0000\u009e\u009f\u0003\u001f\u000f"+
		"\u0000\u009f\u00a0\u0005.\u0000\u0000\u00a0\u00a2\u0001\u0000\u0000\u0000"+
		"\u00a1\u009b\u0001\u0000\u0000\u0000\u00a1\u009e\u0001\u0000\u0000\u0000"+
		"\u00a2\u001c\u0001\u0000\u0000\u0000\u00a3\u00a6\u0003\u001f\u000f\u0000"+
		"\u00a4\u00a6\u0003\u001b\r\u0000\u00a5\u00a3\u0001\u0000\u0000\u0000\u00a5"+
		"\u00a4\u0001\u0000\u0000\u0000\u00a6\u00a7\u0001\u0000\u0000\u0000\u00a7"+
		"\u00a8\u0003!\u0010\u0000\u00a8\u001e\u0001\u0000\u0000\u0000\u00a9\u00b0"+
		"\u0003\u0017\u000b\u0000\u00aa\u00ac\u0005_\u0000\u0000\u00ab\u00aa\u0001"+
		"\u0000\u0000\u0000\u00ab\u00ac\u0001\u0000\u0000\u0000\u00ac\u00ad\u0001"+
		"\u0000\u0000\u0000\u00ad\u00af\u0003\u0017\u000b\u0000\u00ae\u00ab\u0001"+
		"\u0000\u0000\u0000\u00af\u00b2\u0001\u0000\u0000\u0000\u00b0\u00ae\u0001"+
		"\u0000\u0000\u0000\u00b0\u00b1\u0001\u0000\u0000\u0000\u00b1 \u0001\u0000"+
		"\u0000\u0000\u00b2\u00b0\u0001\u0000\u0000\u0000\u00b3\u00b5\u0007\u0004"+
		"\u0000\u0000\u00b4\u00b6\u0007\u0005\u0000\u0000\u00b5\u00b4\u0001\u0000"+
		"\u0000\u0000\u00b5\u00b6\u0001\u0000\u0000\u0000\u00b6\u00b7\u0001\u0000"+
		"\u0000\u0000\u00b7\u00b8\u0003\u001f\u000f\u0000\u00b8\"\u0001\u0000\u0000"+
		"\u0000\u00b9\u00ba\u0005.\u0000\u0000\u00ba\u00bb\u0003\u001f\u000f\u0000"+
		"\u00bb$\u0001\u0000\u0000\u0000\u00bc\u00c1\u0005\"\u0000\u0000\u00bd"+
		"\u00c0\u0003\'\u0013\u0000\u00be\u00c0\b\u0006\u0000\u0000\u00bf\u00bd"+
		"\u0001\u0000\u0000\u0000\u00bf\u00be\u0001\u0000\u0000\u0000\u00c0\u00c3"+
		"\u0001\u0000\u0000\u0000\u00c1\u00bf\u0001\u0000\u0000\u0000\u00c1\u00c2"+
		"\u0001\u0000\u0000\u0000\u00c2\u00c4\u0001\u0000\u0000\u0000\u00c3\u00c1"+
		"\u0001\u0000\u0000\u0000\u00c4\u00cf\u0005\"\u0000\u0000\u00c5\u00ca\u0005"+
		"\'\u0000\u0000\u00c6\u00c9\u0003\'\u0013\u0000\u00c7\u00c9\b\u0007\u0000"+
		"\u0000\u00c8\u00c6\u0001\u0000\u0000\u0000\u00c8\u00c7\u0001\u0000\u0000"+
		"\u0000\u00c9\u00cc\u0001\u0000\u0000\u0000\u00ca\u00c8\u0001\u0000\u0000"+
		"\u0000\u00ca\u00cb\u0001\u0000\u0000\u0000\u00cb\u00cd\u0001\u0000\u0000"+
		"\u0000\u00cc\u00ca\u0001\u0000\u0000\u0000\u00cd\u00cf\u0005\'\u0000\u0000"+
		"\u00ce\u00bc\u0001\u0000\u0000\u0000\u00ce\u00c5\u0001\u0000\u0000\u0000"+
		"\u00cf&\u0001\u0000\u0000\u0000\u00d0\u00d3\u0005\\\u0000\u0000\u00d1"+
		"\u00d4\u0003)\u0014\u0000\u00d2\u00d4\u0007\b\u0000\u0000\u00d3\u00d1"+
		"\u0001\u0000\u0000\u0000\u00d3\u00d2\u0001\u0000\u0000\u0000\u00d4(\u0001"+
		"\u0000\u0000\u0000\u00d5\u00d6\u0005u\u0000\u0000\u00d6\u00d7\u0003+\u0015"+
		"\u0000\u00d7\u00d8\u0003+\u0015\u0000\u00d8\u00d9\u0003+\u0015\u0000\u00d9"+
		"\u00da\u0003+\u0015\u0000\u00da*\u0001\u0000\u0000\u0000\u00db\u00dc\u0007"+
		"\t\u0000\u0000\u00dc,\u0001\u0000\u0000\u0000\u00dd\u00de\u0005M\u0000"+
		"\u0000\u00de\u00df\u0005I\u0000\u0000\u00df\u00e0\u0005S\u0000\u0000\u00e0"+
		"\u00e1\u0005S\u0000\u0000\u00e1\u00e2\u0005I\u0000\u0000\u00e2\u00e3\u0005"+
		"N\u0000\u0000\u00e3\u00e4\u0005G\u0000\u0000\u00e4.\u0001\u0000\u0000"+
		"\u0000\u00e5\u00e6\u0005+\u0000\u0000\u00e60\u0001\u0000\u0000\u0000\u00e7"+
		"\u00e8\u0005-\u0000\u0000\u00e82\u0001\u0000\u0000\u0000\u00e9\u00ea\u0005"+
		"*\u0000\u0000\u00ea4\u0001\u0000\u0000\u0000\u00eb\u00ec\u0005/\u0000"+
		"\u0000\u00ec6\u0001\u0000\u0000\u0000\u00ed\u00ee\u0005/\u0000\u0000\u00ee"+
		"\u00ef\u0005/\u0000\u0000\u00ef8\u0001\u0000\u0000\u0000\u00f0\u00f1\u0005"+
		"*\u0000\u0000\u00f1\u00f2\u0005*\u0000\u0000\u00f2:\u0001\u0000\u0000"+
		"\u0000\u00f3\u00f4\u0005%\u0000\u0000\u00f4<\u0001\u0000\u0000\u0000\u00f5"+
		"\u00f6\u0005<\u0000\u0000\u00f6>\u0001\u0000\u0000\u0000\u00f7\u00f8\u0005"+
		"<\u0000\u0000\u00f8\u00f9\u0005=\u0000\u0000\u00f9@\u0001\u0000\u0000"+
		"\u0000\u00fa\u00fb\u0005>\u0000\u0000\u00fbB\u0001\u0000\u0000\u0000\u00fc"+
		"\u00fd\u0005>\u0000\u0000\u00fd\u00fe\u0005=\u0000\u0000\u00feD\u0001"+
		"\u0000\u0000\u0000\u00ff\u0100\u0005=\u0000\u0000\u0100\u0103\u0005=\u0000"+
		"\u0000\u0101\u0103\u0005=\u0000\u0000\u0102\u00ff\u0001\u0000\u0000\u0000"+
		"\u0102\u0101\u0001\u0000\u0000\u0000\u0103F\u0001\u0000\u0000\u0000\u0104"+
		"\u0105\u0005!\u0000\u0000\u0105\u0109\u0005=\u0000\u0000\u0106\u0107\u0005"+
		"<\u0000\u0000\u0107\u0109\u0005>\u0000\u0000\u0108\u0104\u0001\u0000\u0000"+
		"\u0000\u0108\u0106\u0001\u0000\u0000\u0000\u0109H\u0001\u0000\u0000\u0000"+
		"\u010a\u010b\u0005a\u0000\u0000\u010b\u010c\u0005n\u0000\u0000\u010c\u010d"+
		"\u0005d\u0000\u0000\u010dJ\u0001\u0000\u0000\u0000\u010e\u010f\u0005o"+
		"\u0000\u0000\u010f\u0110\u0005r\u0000\u0000\u0110L\u0001\u0000\u0000\u0000"+
		"\u0111\u0112\u0005n\u0000\u0000\u0112\u0113\u0005o\u0000\u0000\u0113\u0114"+
		"\u0005t\u0000\u0000\u0114N\u0001\u0000\u0000\u0000\u0115\u0116\u0005?"+
		"\u0000\u0000\u0116\u0117\u0005?\u0000\u0000\u0117P\u0001\u0000\u0000\u0000"+
		"\u0118\u011c\u0007\n\u0000\u0000\u0119\u011b\u0007\u000b\u0000\u0000\u011a"+
		"\u0119\u0001\u0000\u0000\u0000\u011b\u011e\u0001\u0000\u0000\u0000\u011c"+
		"\u011a\u0001\u0000\u0000\u0000\u011c\u011d\u0001\u0000\u0000\u0000\u011d"+
		"R\u0001\u0000\u0000\u0000\u011e\u011c\u0001\u0000\u0000\u0000\u011f\u0123"+
		"\u0007\f\u0000\u0000\u0120\u0122\u0007\r\u0000\u0000\u0121\u0120\u0001"+
		"\u0000\u0000\u0000\u0122\u0125\u0001\u0000\u0000\u0000\u0123\u0121\u0001"+
		"\u0000\u0000\u0000\u0123\u0124\u0001\u0000\u0000\u0000\u0124T\u0001\u0000"+
		"\u0000\u0000\u0125\u0123\u0001\u0000\u0000\u0000\u0126\u012a\u0005$\u0000"+
		"\u0000\u0127\u0129\u0007\u000e\u0000\u0000\u0128\u0127\u0001\u0000\u0000"+
		"\u0000\u0129\u012c\u0001\u0000\u0000\u0000\u012a\u0128\u0001\u0000\u0000"+
		"\u0000\u012a\u012b\u0001\u0000\u0000\u0000\u012bV\u0001\u0000\u0000\u0000"+
		"\u012c\u012a\u0001\u0000\u0000\u0000\u012d\u012e\u0005$\u0000\u0000\u012e"+
		"\u012f\u0005$\u0000\u0000\u012f\u0133\u0001\u0000\u0000\u0000\u0130\u0132"+
		"\u0007\u000e\u0000\u0000\u0131\u0130\u0001\u0000\u0000\u0000\u0132\u0135"+
		"\u0001\u0000\u0000\u0000\u0133\u0131\u0001\u0000\u0000\u0000\u0133\u0134"+
		"\u0001\u0000\u0000\u0000\u0134X\u0001\u0000\u0000\u0000\u0135\u0133\u0001"+
		"\u0000\u0000\u0000\u0136\u013a\u0007\f\u0000\u0000\u0137\u0139\u0007\r"+
		"\u0000\u0000\u0138\u0137\u0001\u0000\u0000\u0000\u0139\u013c\u0001\u0000"+
		"\u0000\u0000\u013a\u0138\u0001\u0000\u0000\u0000\u013a\u013b\u0001\u0000"+
		"\u0000\u0000\u013b\u013d\u0001\u0000\u0000\u0000\u013c\u013a\u0001\u0000"+
		"\u0000\u0000\u013d\u013e\u0005=\u0000\u0000\u013eZ\u0001\u0000\u0000\u0000"+
		"\u001b\u0000nv\u0083\u0088\u008d\u0090\u0098\u009b\u00a1\u00a5\u00ab\u00b0"+
		"\u00b5\u00bf\u00c1\u00c8\u00ca\u00ce\u00d3\u0102\u0108\u011c\u0123\u012a"+
		"\u0133\u013a\u0001\u0006\u0000\u0000";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}