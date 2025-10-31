/*
    ANLTR4 grammar for conditional rule logic and arithmetic expressions.
    @author Kai Niemi
 */
lexer grammar ExpressionLexer;

options { caseInsensitive = true; }

// ----------------------------------------
// Lexer rules that split input into tokens
// ----------------------------------------

SEMICOLON: ';';
COMMENT: '//';
LPAREN : '(' ;
RPAREN : ')' ;
LBRACKET : '{' ;
RBRACKET : '}' ;
COMMA : ',' ;

// Arithmetics
POW : '^' ;
MULT : '*' ;
DIV : '/' ;
PLUS : '+' ;
MINUS : '-' ;
MIN : 'min' ;
MAX : 'max' ;
MOD : ('mod'|'%') ;

// Conditionals

IF   : 'if' ;
THEN : 'then';
ELSE : ('else'|'otherwise');
IN : 'in';

// Logical

AND : ('and'|'&&') ;
OR  : ('or'|'||') ;
NOT : ('not'|'!') ;

// Comparative

GT : '>' ;
GE : '>=' ;
LT : '<' ;
LE : '<=' ;
EQ : '==' ;
NE : '!=' ;

// Literals
DatePrefix
    : LBRACKET 'd'
    ;

DateTimePrefix
    : LBRACKET 'dt'
    ;

TimePrefix
    : LBRACKET 't'
    ;

DateTimeLiteral
    : '\'' Date ' ' Time '\'' ;

DateLiteral
    : '\'' Date '\'' ;

TimeLiteral
    : '\'' Time '\'' ;

Date
    : FourDigit '-' TwoDigit '-' TwoDigit ;

Time
    : TwoDigit ':' TwoDigit ':' TwoDigit ;

DecimalLiteral
    : Digit+ ('.' Digit+)? ;

fragment SignedDecimal
    : Sign? Digit+ ('.' Digit+)? ;

BooleanLiteral : TRUE | FALSE ;

TRUE  : 'true' ;
FALSE : 'false' ;

StringLiteral
    :  '\'' (EscapeChars | ~['"\\])* '\'' ;

Identifier
    : Letter LetterOrDigit* ;

fragment FourDigit
    : Digit Digit Digit Digit ;

fragment TwoDigit
    : Digit Digit ;

fragment Sign
    : [+-] ;

fragment Digit
    : [0-9] ;

fragment Digits
    : '0' | [1-9] [0-9]* ;

fragment EscapeChars
    :   '\\' (["\\/bfnrt] | UniCode) ;

fragment UniCode
    : 'u' Hex Hex Hex Hex ;

fragment Hex
    : [0-9a-f] ;

fragment Letter
    : [a-z$_] ;

fragment LetterOrDigit
    : [a-z0-9$_] ;

WS  : [ \r\n\t]+ -> skip ;
