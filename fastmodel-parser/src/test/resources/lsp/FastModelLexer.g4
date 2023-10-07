lexer grammar FastModelLexer;
channels { WS_CHANNEL, COMMENT_CHANNEL }

@lexer::members{
     /**
       * This method will be called when we see '/*' and try to match it as a bracketed comment.
       * If the next character is '+', it should be parsed as hint later, and we cannot match
       * it as a bracketed comment.
       *
       * Returns true if the next character is '+'.
       */
      public boolean isHint() {
        int nextChar = _input.LA(1);
        if (nextChar == '+') {
          return true;
        } else {
          return false;
        }
      }
}

KW_CREATE : C R E A T E ;
KW_ALTER: A L T E R ;

KW_PROCESS : B U S I N E S S '_' P R O C E S S ;
KW_PROCESSES : B U S I N E S S '_' P R O C E S S E S ;
KW_BUSINESS_CATEGORY : B U S I N E S S '_' C A T E G O R Y ;
KW_BUSINESS_CATEGORIES : B U S I N E S S '_' C A T E G O R I E S ;

KW_DOMAIN  : D O M A I N ;
KW_DOMAINS  : D O M A I N S ;
KW_TABLE   : T A B L E ;
KW_TABLES : T A B L E S ;
KW_TYPE : T Y P E ;
KW_DIALECT : D I A L E C T ;
KW_FULL_BU : B U S I N E S S '_' U N I T ;
KW_FULL_BUS : B U S I N E S S '_' U N I T S ;
KW_ADJUNCT : A D J U N C T ;
KW_ADJUNCTS : A D J U N C T S ;
KW_DICT : D I C T ;
KW_DICTS : D I C T S ;
KW_TIMEPERIOD : T I M E '_' P E R I O D ;
KW_TIMEPERIODS : T I M E '_' P E R I O D S ;
KW_MEASUREUNIT : M E A S U R E '_' U N I T ;
KW_MEASUREUNITS : M E A S U R E '_' U N I T S ;
KW_INDICATOR    : I N D I C A T O R ;
KW_INDICATORS    : I N D I C A T O R S ;
KW_BATCH : B A T C H ;
KW_BATCHES : B A T C H E S ;
KW_PATH : P A T H ;
KW_COLUMN: C O L U M N ;
KW_CHECKER : C H E C K E R ;
KW_LAYER : L A Y E R  ;
KW_CODE : C O D E  ;
KW_CODES : C O D E S  ;
KW_COlGROUP : C O L U M N '_' G R O U P  ;
KW_ATTRIBUTE : A T T R I B U T E ;
KW_MEASUREMENT : M E A S U R E M E N T ;
KW_CORRELATION : C O R R E L A T I O N ;
KW_PARTITIONED  : P A R T I T I O N E D ;
KW_INCLUDE : I N C L U D E ;
KW_EXCLUDE : E X C L U D E ;
KW_CHECKERS : C H E C K E R S ;
KW_LAYERS : L A Y E R S ;
KW_DROP: D R O P ;
KW_RENAME: R E N A M E ;
KW_USE : U S E ;
KW_TO: T O ;
KW_COMMENT : C O M M E N T ;
KW_SHOW    : S H O W ;
KW_OUTPUT : O U T P U T ;
KW_INSERT : I N S E R T ;
KW_INTO : I N T O ;
KW_AS : A S ;
KW_IF : I F ;
KW_OR : O R ;
KW_IN : I N ;
KW_DATEFIELD: D A T E '_' F I E L D ;
KW_DELETE : D E L E T E ;
KW_ALIAS : A L I A S ;
KW_DIMENSION : D I M E N S I O N ;
KW_DIMENSIONS : D I M E N S I O N S ;
KW_ATTRIBUTES : A T T R I B U T E S ;
KW_DIM_ATTRIBUTES : D I M '_' A T T R I B U T E S ;

KW_SUBJECT : S U B J E C T ;
KW_SUBJECTS : S U B J E C T S ;
KW_MARKET : M A R K E T ;
KW_MARKETS : M A R K E T S ;


KW_CURRENT: C U R R E N T ;
KW_CURRENT_DATE: C U R R E N T '_' D A T E ;
KW_CURRENT_TIMESTAMP: C U R R E N T '_' T I M E S T A M P ;
KW_GROUPING: G R O U P I N G ;
KW_UNNEST: U N N E S T ;
KW_LATERAL: L A T E R A L ;
KW_VIEW : V I E W ;
KW_VIEWS : V I E W S ;
KW_LIKE : L I K E ;
KW_TRUE : T R U E ;
KW_FALSE : F A L S E ;
KW_CAST : C A S T ;
KW_HAVING : H A V I N G ;
KW_ORDER : O R D E R ;
KW_GROUP : G R O U P ;
KW_BY : B Y ;
KW_WHERE : W H E R E ;
KW_PERCENT : P E R C E N T ;
KW_OF : O F ;
KW_EXISTS : E X I S T S ;
KW_NOT : 'NOT' | '!';
KW_VALUES : V A L U E S ;
KW_ON : O N  ;
KW_USING : U S I N G ;
KW_PRESERVE : P R E S E R V E ;
KW_JOIN : J O I N ;
KW_NATURAL : N A T U R A L  ;
KW_LEFT : L E F T ;
KW_RIGHT : R I G H T ;
KW_FULL : F U L L ;
KW_INNER: I N N E R ;
KW_CROSS : C R O S S ;
KW_OUTER : O U T E R ;
KW_SEMI : S E M I  ;
KW_TABLESAMPLE : T A B L E S A M P L E ;
KW_TRUNCATE : T R U N C A T E ;
KW_DWS : D W S ;
KW_ADS : A D S ;
KW_ODS : O D S ;

KW_OVERWRITE : O V E R W R I T E ;
KW_BINARY: B I N A R Y ;
KW_PRECISION : P R E C I S I O N ;

KW_NULL: N U L L ;
KW_BOOLEAN: B O O L E A N ;
KW_TINYINT: T I N Y I N T ;
KW_SMALLINT: S M A L L I N T ;
KW_INT: I N T ;
KW_BIGINT: B I G I N T ;
KW_FLOAT: F L O A T ;
KW_DOUBLE: D O U B L E ;
KW_DATE: D A T E ;
KW_DATETIME: D A T E T I M E ;
KW_TIMESTAMP: T I M E S T A M P ;
KW_DECIMAL: D E C I M A L ;
KW_STRING: S T R I N G ;
KW_CHAR: C H A R ;
KW_VARCHAR: V A R C H A R ;
KW_CUSTOM: C U S T O M ;
KW_ARRAY: A R R A Y ;
KW_STRUCT: S T R U C T ;
KW_JSON : J S O N ;
KW_MAP: M A P ;
KW_WITH: W I T H ;
KW_SET: S E T ;
KW_UNSET: U N S E T ;
KW_ORDINALITY: O R D I N A L I T Y ;
KW_LEVEL: L E V E L ;
KW_ENUM: E N U M ;
KW_PERIODIC_SNAPSHOT : P E R I O D I C '_' S N A P S H O T ;
KW_ACCUMULATING_SNAPSHOT : A C C U M U L A T I N G '_' S N A P S H O T ;
KW_AGGREGATE : A G G R E G A T E ;
KW_CONSOLIDATED : C O N S O L I D A T E D ;
KW_TRANSACTION : T R A N S A C T I O N ;
KW_FACTLESS : F A C T L E S S ;
KW_NORMAL : N O R M A L ;
KW_ADVANCED : A D V A N C E D ;
KW_MATERIALIZED: M A T E R I A L I Z E D ;
KW_ENGINE: E N G I N E ;
KW_PROPERTIES: P R O P E R T I E S ;
KW_FLOOR: F L O O R ;
KW_ATOMIC: A T O M I C ;
KW_COMPOSITE: C O M P O S I T E ;
KW_DERIVATIVE: D E R I V A T I V E ;
KW_CALL : C A L L ;

KW_CASE: C A S E ;
KW_WHEN: W H E N ;
KW_THEN: T H E N ;
KW_ELSE: E L S E ;
KW_END: E N D ;


KW_LIMIT: L I M I T ;

KW_OFFSET: O F F S E T ;

KW_UNION: U N I O N ;
KW_DISTINCT: D I S T I N C T ;

KW_ALL: A L L ;
KW_INTERSECT: I N T E R S E C T ;
KW_EXCEPT: E X C E P T ;
KW_SELECT: S E L E C T ;
KW_FROM: F R O M ;
KW_AND: A N D ;
KW_BETWEEN: B E T W E E N ;


KW_IS: I S ;
KW_DIM: D I M ;
KW_FACT: F A C T ;
KW_PRIMARY: P R I M A R Y ;
KW_KEY: K E Y ;
KW_CHANGE: C H A N G E ;
KW_COLUMNS: C O L U M N S ;
KW_ADD: A D D ;
KW_REPLACE: R E P L A C E ;

KW_CONSTRAINT: C O N S T R A I N T ;

KW_ASC: A S C ;
KW_DESC: D E S C ;
KW_DESCRIBE : D E S C R I B E ;
KW_REFERENCES: R E F E R E N C E S ;

KW_INTERVAL: I N T E R V A L ;

KW_YEAR: Y E A R ;

KW_QUARTER: Q U A R T E R ;
KW_MONTH: M O N T H ;
KW_WEEK: W E E K ;
KW_DAY: D A Y ;
KW_DOW: D A Y O F W E E K ;
KW_HOUR: H O U R ;
KW_MINUTE: M I N U T E ;
KW_SECOND: S E C O N D ;
KW_TRANSFORM: T R A N S F O R M ;
KW_REDUCE: R E D U C E ;
KW_WINDOW: W I N D O W ;
KW_ROWS: R O W S ;
KW_RANGE: R A N G E ;
KW_GROUPS: G R O U P S ;
KW_IGNORE: I G N O R E ;
KW_RESPECT: R E S P E C T ;

KW_UNBOUNDED: U N B O U N D E D ;
KW_PRECEDING: P R E C E D I N G ;
KW_ROW: R O W ;
KW_FOLLOWING: F O L L O W I N G ;
KW_ROLLUP: R O L L U P ;
KW_CUBE: C U B E ;
KW_SETS: S E T S ;
KW_CLUSTER: C L U S T E R ;
KW_PARTITION: P A R T I T I O N ;
KW_DISTRIBUTE: D I S T R I B U T E ;
KW_SORT: S O R T ;
KW_OVER: O V E R ;
KW_FILTER: F I L T E R ;
KW_EXTRACT: E X T R A C T ;
KW_TIMESTAMPLOCALTZ: T I M E S T A M P L O C A L T Z ;
KW_UNIONTYPE: U N I O N T Y P E ;
KW_RLIKE: R L I K E ;
KW_REGEXP: R E G E X P ;
KW_ANY: A N Y ;
KW_DEFAULT: D E F A U L T ;

KW_BERNOULLI: B E R N O U L L I ;
KW_SYSTEM: S Y S T E M ;
KW_RECURSIVE: R E C U R S I V E ;
KW_FETCH: F E T C H ;
KW_FIRST: F I R S T ;
KW_NEXT: N E X T ;
KW_ONLY: O N L Y ;
KW_TIES: T I E S ;
KW_NULLS: N U L L S ;
KW_LAST: L A S T ;


KW_ENABLE: E N A B L E ;
KW_DISABLE: D I S A B L E ;
KW_VALIDATE: V A L I D A T E ;
KW_NOVALIDATE: N O V A L I D A T E ;
KW_RELY: R E L Y ;
KW_NORELY: N O R E L Y ;
KW_ENFORCED: E N F O R C E D ;
KW_FOR: F O R ;
KW_SUBSTRING: S U B S T R I N G ;
KW_PIPE : P I P E ;
KW_TARGET : T A R G E T ;
KW_TARGET_TYPE : T A R G E T '_' T Y P E ;
KW_URL : U R L ;
KW_USER : U S E R ;
KW_PASSWORD : P A S S W O R D ;
KW_SYNC : S Y N C ;
KW_ASYNC : A S Y N C ;
KW_COPY : C O P Y ;
KW_BIZ_DATE : B I Z '_' D A T E ;
KW_COPY_MODE : C O P Y '_' M O D E ;
KW_SOURCE : S O U R C E ;
KW_RULES : R U L E S ;
KW_SQL : S Q L ;
KW_TASK : T A S K ;
KW_STRONG : S T R O N G ;
KW_WEAK : W E A K ;
KW_RULE : R U L E ;
KW_INCREASE : I N C R E A S E ;
KW_DECREASE : D E C R E A S E ;
KW_DYNAMIC  : D Y N A M I C ;
KW_DQC_RULE : D Q C '_' R U L E ;
KW_CHECK : C H E C K ;
KW_NAMING : N A M I N G ;
KW_REL_DIMENSION : R E L '_' D I M E N S I O N ;
KW_REL_INDICATOR : R E L '_' I N D I C A T O R ;
KW_STAT_TIME : S T A T '_' T I M E ;
KW_REDUNDANT : R E D U N D A N T ;
KW_EXP : E X P ;
KW_EXPORT : E X P O R T ;
KW_IMPORT : I M P O R T ;
KW_IMPORT_SQL : 'IMP_SQL' | 'IMPORT_SQL';
KW_EXPORT_SQL : 'EXP_SQL' | 'EXPORT_SQL';
KW_UNIQUE : U N I Q U E ;
KW_INDEX : I N D E X ;
KW_RENDER : R E N D E R ;
KW_REF : R E F ;
KW_DEPENDENCY : D E P E N D E N C Y ;
KW_FORMAT : F O R M A T ;
KW_HELP : H E L P ;
KW_AFTER : A F T E R ;
KW_MOVE : M O V E ;
KW_STATISTIC : S T A T I S T I C ;

EQUAL:'=';
LESSTHAN:'<';
GREATERTHAN:'>';
EQUAL_NS : '<=>';
NOTEQUAL : '<>' | '!=';
LESSTHANOREQUALTO : '<=';
GREATERTHANOREQUALTO : '>=';
LEFT_DIRECTION_RIGHT : '->';
RIGHT_DIRECTON_LEFT : '<-';


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
DIV : D I V ;
MODE_OPTION : MINUS M;
TEXT_OPTION : MINUS T;
URI_OPTION : MINUS U;



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


TIME_ID: ('0'..'9')+ ('a'..'z' | 'A'..'Z')+;

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

fragment
CharSetName
        : '_' (Letter | Digit | '_' | '-' | '.' | ':' )+
        ;

fragment
CharSetLiteral
     : StringLiteral
     | '0' 'X' (HexDigit|Digit)+
     ;


NumberLiteral
    : Number ('D' | 'B' 'D')
    ;

ByteLengthLiteral
    : (Digit)+ ('b' | 'B' | 'k' | 'K' | 'm' | 'M' | 'g' | 'G')
    ;


INTEGER_VALUE
    : DIGIT+
    ;

DECIMAL_VALUE
    : DIGIT+ '.' DIGIT*
    ;

DOUBLE_VALUE
    : DIGIT+ ('.' DIGIT*)? EXPONENT
    ;

Number
    : (Digit)+ ( DOT (Digit)* (Exponent)? | Exponent)?
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

fragment
Substitution
    :
    DOLLAR LCURLY (IDLetter | IDDigit) (IDLetter | IDDigit | '_' )* RCURLY
    ;


Identifier
    : (IDLetter | IDDigit) (IDLetter | IDDigit | '_' | Substitution)*
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

HINT_START : '/*+' ;

HINT_END : '*/';

MULTILINE_COMMENT: '/*' {!isHint()}? .*? ( '*/' | EOF) -> channel(HIDDEN);


WS  : (' '|'\r'|'\t'|'\n') -> channel(HIDDEN);



fragment
Exponent
    : ('e' | 'E') ( PLUS|MINUS )? (Digit)+
    ;

fragment
HexDigit
    : 'a'..'f' | 'A'..'F'
    ;

fragment DIGIT
    : [0-9]
    ;

fragment EXPONENT
    : 'E' [+-]? DIGIT+
    ;

fragment A: [aA];
fragment B: [bB];
fragment C: [cC];
fragment D: [dD];
fragment E: [eE];
fragment F: [fF];
fragment G: [gG];
fragment H: [hH];
fragment I: [iI];
fragment J: [jJ];
fragment K: [kK];
fragment L: [lL];
fragment M: [mM];
fragment N: [nN];
fragment O: [oO];
fragment P: [pP];
fragment Q: [qQ];
fragment R: [rR];
fragment S: [sS];
fragment T: [tT];
fragment U: [uU];
fragment V: [vV];
fragment W: [wW];
fragment X: [xX];
fragment Y: [yY];
fragment Z: [zZ];

UNRECOGNIZED
    : .
    ;
