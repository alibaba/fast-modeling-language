lexer grammar FastModelZenLexer;
channels { WS_CHANNEL, COMMENT_CHANNEL }

EQUAL:'=';
LESSTHAN:'<';
GREATERTHAN:'>';
EQUAL_NS : '<=>';
NOTEQUAL : '<>' | '!=';
LESSTHANOREQUALTO : '<=';
GREATERTHANOREQUALTO : '>=';

DOT : '.'; // generated as a part of Number rule
COLON : ':' ;
COMMA : ',' ;
SEMICOLON : ';' ;
LPAREN : '(' ;
RPAREN : ')' ;
LSQUARE : '[' ;
RSQUARE : ']' ;
LCURLY : '{';
RCURLY : '}';
DIVIDE : '/';
PLUS : '+';
MINUS : '-';
MINUSMINUS : '--';
STAR : '*';
MOD : '%';
DIV : 'DIV';
NEW_LINE : '\r'?'\n';
TAB : '\t';
BLANK :' ';



AMPERSAND : '&';
TILDE : '~';
BITWISEOR : '|';
CONCATENATE : '||';
BITWISEXOR : '^';
QUESTION : '?';
DOLLAR : '$';
MACRO : '#';


// LITERALS
fragment
Letter
    : 'a'..'z' | 'A'..'Z'
    ;

fragment
Digit
    : '0'..'9'
    ;


fragment
RegexComponent
    : 'a'..'z' | 'A'..'Z' | '0'..'9' | '_'
    | PLUS | STAR | QUESTION | MINUS | DOT
    | LPAREN | RPAREN | LSQUARE | RSQUARE | LCURLY | RCURLY
    | BITWISEXOR | BITWISEOR | DOLLAR | '!'
    ;

StringLiteral
    : ( '\'' ( ~('\''|'\\') | ('\\' .) )* '\''
    | '"' ( ~('"'|'\\') | ('\\' .) )* '"'
    )+
    ;

INTEGER_VALUE
    : DIGIT+
    ;

// Ported from Java.g
fragment
IDLetter
    :  '\u0024' |
       '\u0041'..'\u005a' |
       '\u005f' |
       '\u0061'..'\u007a' |
       '\u00c0'..'\u00d6' |
       '\u00d8'..'\u00f6' |
       '\u00f8'..'\u00ff' |
       '\u0100'..'\u1fff' |
       '\u3040'..'\u318f' |
       '\u3300'..'\u337f' |
       '\u3400'..'\u3d2d' |
       '\u4e00'..'\u9fff' |
       '\uf900'..'\ufaff'
    ;

fragment
IDDigit
    :  '\u0030'..'\u0039' |
       '\u0660'..'\u0669' |
       '\u06f0'..'\u06f9' |
       '\u0966'..'\u096f' |
       '\u09e6'..'\u09ef' |
       '\u0a66'..'\u0a6f' |
       '\u0ae6'..'\u0aef' |
       '\u0b66'..'\u0b6f' |
       '\u0be7'..'\u0bef' |
       '\u0c66'..'\u0c6f' |
       '\u0ce6'..'\u0cef' |
       '\u0d66'..'\u0d6f' |
       '\u0e50'..'\u0e59' |
       '\u0ed0'..'\u0ed9' |
       '\u1040'..'\u1049'
   ;

Identifier
    : (IDLetter | IDDigit) (IDLetter | IDDigit | '_'  | LPAREN | RPAREN | COLON | COMMA  | LESSTHAN | GREATERTHAN )*
    | QuotedIdentifier  /* though at the language level we allow all Identifiers to be QuotedIdentifiers;
                                              at the API level only columns are allowed to be of this form */
    | '`' RegexComponent+ '`'
    ;

QuotedIdentifier
    :
    '`'  ( '``' | ~('`') )* '`'
    ;


SINGLE_LINE_COMMENT:
	'--' ~[\r\n]* (('\r'? '\n') | EOF) -> channel(HIDDEN);


MULTILINE_COMMENT: '/*'  ( '*/' | EOF) -> channel(HIDDEN);


WS  : (' '|'\r'|'\t'|'\n') -> channel(HIDDEN);



fragment DIGIT
    : [0-9]
    ;


UNRECOGNIZED
    : .
    ;