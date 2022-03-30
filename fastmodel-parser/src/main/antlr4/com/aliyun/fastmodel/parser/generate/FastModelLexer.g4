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

KW_CREATE : 'CREATE';
KW_ALTER:   'ALTER';

KW_PROCESS : 'BUSINESS_PROCESS';
KW_PROCESSES : 'BUSINESS_PROCESSES';
KW_BUSINESS_CATEGORY : 'BUSINESS_CATEGORY';
KW_BUSINESS_CATEGORIES : 'BUSINESS_CATEGORIES';

KW_DOMAIN  : 'DOMAIN';
KW_DOMAINS  : 'DOMAINS';
KW_TABLE   : 'TABLE';
KW_TABLES : 'TABLES';
KW_TYPE : 'TYPE';
KW_DIALECT : 'DIALECT';
KW_FULL_BU : 'BUSINESS_UNIT';
KW_FULL_BUS : 'BUSINESS_UNITS';
KW_ADJUNCT : 'ADJUNCT';
KW_ADJUNCTS : 'ADJUNCTS';
KW_DICT : 'DICT';
KW_DICTS : 'DICTS';
KW_TIMEPERIOD : 'TIME_PERIOD';
KW_TIMEPERIODS : 'TIME_PERIODS';
KW_MEASUREUNIT : 'MEASURE_UNIT';
KW_MEASUREUNITS : 'MEASURE_UNITS';
KW_INDICATOR    : 'INDICATOR';
KW_INDICATORS    : 'INDICATORS';
KW_BATCH : 'BATCH';
KW_BATCHES : 'BATCHES';
KW_PATH : 'PATH';
KW_COLUMN: 'COLUMN';
KW_CHECKER : 'CHECKER';
KW_LAYER : 'LAYER' ;
KW_CODE : 'CODE' ;
KW_CODES : 'CODES' ;
KW_COlGROUP : 'COLUMN_GROUP' ;
KW_ATTRIBUTE : 'ATTRIBUTE';
KW_MEASUREMENT : 'MEASUREMENT';
KW_CORRELATION : 'CORRELATION';
KW_PARTITIONED  : 'PARTITIONED';
KW_INCLUDE : 'INCLUDE';
KW_EXCLUDE : 'EXCLUDE';
KW_CHECKERS : 'CHECKERS';
KW_LAYERS : 'LAYERS';
KW_DROP: 'DROP';
KW_RENAME: 'RENAME';
KW_USE : 'USE';
KW_TO: 'TO';
KW_COMMENT : 'COMMENT';
KW_SHOW    : 'SHOW';
KW_OUTPUT : 'OUTPUT';
KW_INSERT : 'INSERT';
KW_INTO : 'INTO';
KW_AS : 'AS';
KW_IF : 'IF';
KW_OR : 'OR';
KW_IN : 'IN';
KW_DATEFIELD:'DATE_FIELD';
KW_DELETE : 'DELETE';
KW_ALIAS : 'ALIAS';
KW_DIMENSION : 'DIMENSION';
KW_DIMENSIONS : 'DIMENSIONS';
KW_ATTRIBUTES : 'ATTRIBUTES';
KW_DIM_ATTRIBUTES : 'DIM_ATTRIBUTES';

KW_SUBJECT : 'SUBJECT';
KW_SUBJECTS : 'SUBJECTS';
KW_MARKET : 'MARKET';
KW_MARKETS : 'MARKETS';


KW_CURRENT: 'CURRENT';
KW_CURRENT_DATE: 'CURRENT_DATE';
KW_CURRENT_TIMESTAMP: 'CURRENT_TIMESTAMP';
KW_GROUPING: 'GROUPING';
KW_UNNEST: 'UNNEST';
KW_LATERAL: 'LATERAL';
KW_VIEW : 'VIEW';
KW_LIKE : 'LIKE';
KW_TRUE : 'TRUE';
KW_FALSE : 'FALSE';
KW_CAST : 'CAST';
KW_HAVING : 'HAVING';
KW_ORDER : 'ORDER';
KW_GROUP : 'GROUP';
KW_BY : 'BY';
KW_WHERE : 'WHERE';
KW_PERCENT : 'PERCENT';
KW_OF : 'OF';
KW_EXISTS : 'EXISTS';
KW_NOT : 'NOT' | '!';
KW_VALUES : 'VALUES';
KW_ON : 'ON' ;
KW_USING : 'USING';
KW_PRESERVE : 'PRESERVE';
KW_JOIN : 'JOIN';
KW_NATURAL : 'NATURAL' ;
KW_LEFT : 'LEFT';
KW_RIGHT : 'RIGHT';
KW_FULL : 'FULL';
KW_INNER: 'INNER';
KW_CROSS : 'CROSS';
KW_OUTER : 'OUTER';
KW_SEMI : 'SEMI' ;
KW_TABLESAMPLE : 'TABLESAMPLE';
KW_TRUNCATE : 'TRUNCATE';
KW_DWS : 'DWS';
KW_ADS : 'ADS';

KW_OVERWRITE : 'OVERWRITE';
KW_BINARY:  'BINARY';
KW_PRECISION : 'PRECISION';

KW_NULL: 'NULL';
KW_BOOLEAN: 'BOOLEAN';
KW_TINYINT: 'TINYINT';
KW_SMALLINT: 'SMALLINT';
KW_INT: 'INT';
KW_BIGINT: 'BIGINT';
KW_FLOAT: 'FLOAT';
KW_DOUBLE: 'DOUBLE';
KW_DATE: 'DATE';
KW_DATETIME: 'DATETIME';
KW_TIMESTAMP: 'TIMESTAMP';
KW_DECIMAL: 'DECIMAL';
KW_STRING: 'STRING';
KW_CHAR: 'CHAR';
KW_VARCHAR: 'VARCHAR';
KW_CUSTOM: 'CUSTOM';
KW_ARRAY: 'ARRAY';
KW_STRUCT: 'STRUCT';
KW_MAP: 'MAP';
KW_WITH: 'WITH';
KW_SET:'SET';
KW_UNSET:'UNSET';
KW_ORDINALITY: 'ORDINALITY';
KW_LEVEL:'LEVEL';
KW_ENUM:'ENUM';
KW_PERIODIC_SNAPSHOT : 'PERIODIC_SNAPSHOT';
KW_ACCUMULATING_SNAPSHOT : 'ACCUMULATING_SNAPSHOT';
KW_AGGREGATE : 'AGGREGATE';
KW_CONSOLIDATED : 'CONSOLIDATED';
KW_TRANSACTION : 'TRANSACTION';
KW_FACTLESS : 'FACTLESS';
KW_NORMAL : 'NORMAL';
KW_ADVANCED : 'ADVANCED';
KW_MATERIALIZED:'MATERIALIZED';
KW_ENGINE:'ENGINE';
KW_PROPERTIES:'PROPERTIES';
KW_FLOOR:'FLOOR';
KW_ATOMIC:'ATOMIC';
KW_COMPOSITE:'COMPOSITE';
KW_DERIVATIVE:'DERIVATIVE';
KW_CALL : 'CALL';

KW_CASE:'CASE';
KW_WHEN:'WHEN';
KW_THEN:'THEN';
KW_ELSE:'ELSE';
KW_END:'END';


KW_LIMIT:'LIMIT';

KW_OFFSET:'OFFSET';

KW_UNION:'UNION';
KW_DISTINCT:'DISTINCT';

KW_ALL:'ALL';
KW_INTERSECT:'INTERSECT';
KW_EXCEPT:'EXCEPT';
KW_SELECT:'SELECT';
KW_FROM:'FROM';
KW_AND:'AND';
KW_BETWEEN:'BETWEEN';


KW_IS:'IS';
KW_DIM:'DIM';
KW_FACT:'FACT';
KW_PRIMARY:'PRIMARY';
KW_KEY:'KEY';
KW_CHANGE:'CHANGE';
KW_COLUMNS:'COLUMNS';
KW_ADD:'ADD';
KW_REPLACE:'REPLACE';

KW_CONSTRAINT:'CONSTRAINT';

KW_ASC:'ASC';
KW_DESC:'DESC';
KW_DESCRIBE : 'DESCRIBE';
KW_REFERENCES:'REFERENCES';

KW_INTERVAL:'INTERVAL';

KW_YEAR:'YEAR';

KW_QUARTER:'QUARTER';
KW_MONTH:'MONTH';
KW_WEEK:'WEEK';
KW_DAY:'DAY';
KW_DOW:'DAYOFWEEK';
KW_HOUR:'HOUR';
KW_MINUTE:'MINUTE';
KW_SECOND:'SECOND';
KW_TRANSFORM:'TRANSFORM';
KW_REDUCE:'REDUCE';
KW_WINDOW:'WINDOW';
KW_ROWS:'ROWS';
KW_RANGE:'RANGE';
KW_GROUPS:'GROUPS';
KW_IGNORE:'IGNORE';
KW_RESPECT:'RESPECT';

KW_UNBOUNDED:'UNBOUNDED';
KW_PRECEDING:'PRECEDING';
KW_ROW:'ROW';
KW_FOLLOWING:'FOLLOWING';
KW_ROLLUP:'ROLLUP';
KW_CUBE:'CUBE';
KW_SETS:'SETS';
KW_CLUSTER:'CLUSTER';
KW_PARTITION:'PARTITION';
KW_DISTRIBUTE:'DISTRIBUTE';
KW_SORT:'SORT';
KW_OVER:'OVER';
KW_FILTER:'FILTER';
KW_EXTRACT:'EXTRACT';
KW_TIMESTAMPLOCALTZ:'TIMESTAMPLOCALTZ';
KW_UNIONTYPE:'UNIONTYPE';
KW_RLIKE:'RLIKE';
KW_REGEXP:'REGEXP';
KW_ANY:'ANY';
KW_DEFAULT:'DEFAULT';

KW_BERNOULLI:'BERNOULLI';
KW_SYSTEM:'SYSTEM';
KW_RECURSIVE:'RECURSIVE';
KW_FETCH:'FETCH';
KW_FIRST:'FIRST';
KW_NEXT:'NEXT';
KW_ONLY:'ONLY';
KW_TIES:'TIES';
KW_NULLS:'NULLS';
KW_LAST:'LAST';


KW_ENABLE:'ENABLE';
KW_DISABLE:'DISABLE';
KW_VALIDATE:'VALIDATE';
KW_NOVALIDATE:'NOVALIDATE';
KW_RELY:'RELY';
KW_NORELY:'NORELY';
KW_ENFORCED:'ENFORCED';
KW_FOR:'FOR';
KW_SUBSTRING:'SUBSTRING';
KW_PIPE : 'PIPE';
KW_TARGET : 'TARGET';
KW_TARGET_TYPE : 'TARGET_TYPE';
KW_URL : 'URL';
KW_USER : 'USER';
KW_PASSWORD : 'PASSWORD';
KW_SYNC : 'SYNC';
KW_ASYNC : 'ASYNC';
KW_COPY : 'COPY';
KW_BIZ_DATE : 'BIZ_DATE';
KW_COPY_MODE : 'COPY_MODE';
KW_SOURCE : 'SOURCE';
KW_RULES : 'RULES';
KW_SQL : 'SQL';
KW_TASK : 'TASK';
KW_STRONG : 'STRONG';
KW_WEAK : 'WEAK';
KW_RULE : 'RULE';
KW_INCREASE : 'INCREASE';
KW_DECREASE : 'DECREASE';
KW_DYNAMIC  : 'DYNAMIC';
KW_DQC_RULE : 'DQC_RULE';
KW_CHECK : 'CHECK';
KW_NAMING : 'NAMING';
KW_REL_DIMENSION : 'REL_DIMENSION';
KW_REL_INDICATOR : 'REL_INDICATOR';
KW_STAT_TIME : 'STAT_TIME';
KW_REDUNDANT : 'REDUNDANT';
KW_EXP : 'EXP';
KW_EXPORT : 'EXPORT';
KW_IMPORT : 'IMPORT';
KW_IMPORT_SQL : 'IMP_SQL' | 'IMPORT_SQL';
KW_EXPORT_SQL : 'EXP_SQL' | 'EXPORT_SQL';
KW_UNIQUE : 'UNIQUE';
KW_INDEX : 'INDEX';
KW_RENDER : 'RENDER';
KW_REF : 'REF';
KW_DEPENDENCY : 'DEPENDENCY';
KW_FORMAT : 'FORMAT';
KW_HELP : 'HELP';
KW_AFTER : 'AFTER';
KW_MOVE : 'MOVE';

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
DIV : 'DIV';
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