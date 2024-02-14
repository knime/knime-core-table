// TODO how do we support Unicode?
grammar KnimeExpression;

// PARSER RULES

// Eternal rule for the full expression
fullExpr: expr EOF;

// Any valid expression
expr:
	name = IDENTIFIER '(' functionArgs? ')'							# functionCall
	| <assoc = right> expr op = EXPONENTIATE expr					# binaryOp
	| expr op = (MULTIPLY | DIVIDE | MODULO | FLOOR_DIVIDE) expr	# binaryOp
	| expr op = (PLUS | MINUS) expr									# binaryOp
	| '(' inner = expr ')'											# parenthesisedExpr
	| BOOLEAN														# atom
	| INTEGER														# atom
	| FLOAT															# atom
	| STRING														# atom
	| MISSING														# atom;

// TODO inline?
functionArgs: expr (',' expr)* ','?;

// LEXER RULES

WHITESPACE: [ \r\n\t]+ -> skip;

// BOOLEAN literal
BOOLEAN: 'true' | 'false';

// INTEGER literal
INTEGER: '0' | NON_ZERO_DIGIT ('_'? DIGIT)*;
fragment NON_ZERO_DIGIT: [1-9];
fragment DIGIT: [0-9];

// FLOAT literal
FLOAT: POINT_FLOAT | EXPONENT_FLOAT;
fragment POINT_FLOAT: DIGIT_PART? FRACTION | DIGIT_PART '.';
fragment EXPONENT_FLOAT: (DIGIT_PART | POINT_FLOAT) EXPONENT;
fragment DIGIT_PART: DIGIT ('_'? DIGIT)*;
fragment EXPONENT: ('e' | 'E') ('+' | '-')? DIGIT_PART;
fragment FRACTION: '.' DIGIT_PART;

// STRING literal
STRING: '"' (ESC | ~["\\])* '"';
fragment ESC: '\\' [\n\\'"abfnrtv];
// TODO unicode by hex value

// MISSING literal
MISSING: 'MISSING';

// Operators

// Arithmetic
PLUS: '+';
MINUS: '-';
MULTIPLY: '*';
DIVIDE: '/';
FLOOR_DIVIDE: '//';
EXPONENTIATE: '**';
MODULO: '%';

// Comparison
LESS_THAN: '<';
LESS_THAN_OR_EQUAL: '<=';
GREATER_THAN: '>';
GREATER_THAN_OR_EQUAL: '>=';
EQUAL: '==' | '=';
NOT_EQUAL: '!=' | '<>';

// Logical
AND: 'and';
OR: 'or';
NOT: 'not';

// Identifier
IDENTIFIER: [a-zA-Z_] [a-zA-Z_0-9]*;