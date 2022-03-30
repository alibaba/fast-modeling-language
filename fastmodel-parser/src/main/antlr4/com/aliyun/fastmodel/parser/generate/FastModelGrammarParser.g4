parser grammar FastModelGrammarParser;

import BusinessUnitParser,DomainParser, BusinessProcessParser, TableParser,IndicatorParser, MaterializeParser, LayerParser, DictParser, AdjunctParser, TimePeriodParser,GroupParser, MeasureUnitParser, ShowParser,BatchParser,PipeParser, QueryParser, CallParser, Rules, DqcRule, BusinessCategoryParser,ImpExpParser,ScriptParser,DimensionParser,CommandParser, ReferencesParser;

tokens {
    DELIMITER
}

options
{
  tokenVocab=FastModelLexer;
}


root
    : sqlStatements? MINUSMINUS? EOF
    ;

sqlStatements
    : (sqlStatement MINUSMINUS? SEMICOLON? | emptyStatement)*
    (sqlStatement (MINUSMINUS? SEMICOLON)? | emptyStatement)
    ;


emptyStatement
    : SEMICOLON
    ;

sqlStatement
      : buStatements
      | domainStatements
      | bpStatements
      | tableStatements
      | indicatorStatements
      | materializeStatements
      | queryOrInsertStatements
      | adjunctStatements
      | timePeriodStatements
      | dictStatements
      | measureUnitStatements
      | layerStatements
      | batchStatements
      | showStatements
      | groupStatements
      | useStatement
      | call
      | pipeStatements
      | ruleStatements
      | dqcRuleStatements
      | businessCategoryStatements
      | impExpStatements
      | scriptStatements
      | dimensionStatements
      | commandStatements
      | referencesStatements
      ;

 replace
        : KW_OR KW_REPLACE
        ;

ifNotExists
    : KW_IF KW_NOT KW_EXISTS
    ;


alias
    : KW_ALIAS string
    ;

useStatement:
    KW_USE unit=identifier #use
;


qualifiedName
    : identifier
    | identifier (DOT identifier)*
    ;


comment
    : KW_COMMENT string
    ;



tableNameList
    : tableName (COMMA tableName)*
    ;



tableProperties
    : LPAREN keyValueProperty (COMMA keyValueProperty)* RPAREN
    ;


columnParenthesesList
    : LPAREN columnNameList RPAREN
    ;


columnNameList
    : columnName (COMMA columnName)*
    ;

columnName
    : identifier
    ;

alterStatementSuffixRename
    : KW_RENAME KW_TO qualifiedName
    ;

 keyValueProperty
    : string EQUAL string
    ;

 keyProperty
    : string
    ;

 ifExists
    : KW_IF KW_EXISTS
    ;

unSetProperties :
        KW_PROPERTIES? LPAREN keyProperty (COMMA keyProperty)* RPAREN
    ;

alterStatementSuffixSetComment
    : KW_SET comment
    ;


setAliasedName
    : KW_SET alias
    ;

colType
     : typeDbCol
     ;

typeDbCol
    : primitiveType
    | listType
    | mapType
    | structType
    ;



primitiveType
    : name=KW_TINYINT    #genericType
    | name=KW_SMALLINT   #genericType
    | name=KW_INT        #genericType
    | name=KW_BIGINT     #genericType
    | name=KW_BOOLEAN    #genericType
    | name=KW_FLOAT      #genericType
    | name=KW_DOUBLE KW_PRECISION?  #doublePrecisionType
    | name=KW_DATE       #genericType
    | name=KW_DATETIME   #genericType
    | name=KW_TIMESTAMP  #genericType
    | name=KW_STRING     #genericType
    | name=KW_BINARY     #genericType
    | name=KW_DECIMAL   (LPAREN typeParameter (COMMA typeParameter)* RPAREN)? #genericType
    | name=KW_VARCHAR   LPAREN typeParameter RPAREN #genericType
    | name=KW_CHAR      LPAREN typeParameter RPAREN  #genericType
    | name=KW_CUSTOM    (LPAREN string RPAREN)  (LPAREN typeParameter (COMMA typeParameter)* RPAREN)? #customType
    ;

typeParameter
    :  INTEGER_VALUE | typeDbCol
    ;


listType
    : KW_ARRAY LESSTHAN typeDbCol GREATERTHAN
    ;

structType
    : KW_STRUCT LESSTHAN typeList GREATERTHAN
    ;

mapType
    : KW_MAP LESSTHAN key=typeDbCol COMMA value=typeDbCol GREATERTHAN
    ;

typeList
    : colonType (COMMA colonType)*
    ;

colonType
    : identifier COLON typeDbCol (comment)?
    ;


//-----------------------------------------------------------------------------------

//-------- Row Constructor ----------------------------------------------------------
//in support of SELECT * FROM (VALUES(1,2,3),(4,5,6),...) as FOO(a,b,c) and
// INSERT INTO <table> (col1,col2,...) VALUES(...),(...),...
// INSERT INTO <table> (col1,col2,...) SELECT * FROM (VALUES(1,2,3),(4,5,6),...) as Foo(a,b,c)
/*
VALUES(1),(2) means 2 rows, 1 column each.
VALUES(1,2),(3,4) means 2 rows, 2 columns each.
VALUES(1,2,3) means 1 row, 3 columns
*/
// (table|column)
tableOrColumn
    : tableName #tableColumn
    | columnVar #tableColumn
    ;


columnVar
    : DOLLAR LCURLY tableName RCURLY
    | MACRO tableName MACRO
    ;


//-----------------------------------------------------------------------------------

//----------------------- Rules for parsing selectClause -----------------------------
// select a,b,c ...

expressionsInParenthesis
    : LPAREN expression (COMMA expression)* RPAREN
    ;



castExpression
    : KW_CAST
    LPAREN
          expression
          KW_AS
          primitiveType
    RPAREN
    ;

caseExpression
    : KW_CASE operand=expression
    whenClause+
    (KW_ELSE elseExpression=expression)?
    KW_END
    ;

 whenClause
    :(KW_WHEN when=expression KW_THEN then=expression)
    ;

whenExpression
    : KW_CASE
    whenClause+
    (KW_ELSE elseExpression=expression)?
    KW_END
    ;

floorExpression
    : KW_FLOOR
    LPAREN
          expression
          (KW_TO floorDateQualifiers)?
    RPAREN
    ;

floorDateQualifiers
    : KW_YEAR
    | KW_QUARTER
    | KW_MONTH
    | KW_WEEK
    | KW_DAY
    | KW_HOUR
    | KW_MINUTE
    | KW_SECOND
    ;

extractExpression
    : KW_EXTRACT
    LPAREN
          timeQualifiers
          KW_FROM
          expression
    RPAREN
    ;

timeQualifiers
    : KW_YEAR
    | KW_QUARTER
    | KW_MONTH
    | KW_WEEK
    | KW_DAY
    | KW_DOW
    | KW_HOUR
    | KW_MINUTE
    | KW_SECOND
    ;

constant
    : intervalLiteral
    | numberLiteral
    | dateLiteral
    | timestampLiteral
    | timestampLocalTZLiteral
    | string
    | stringLiteralSequence
    | booleanValue
    | KW_NULL
    ;

string
    : StringLiteral
    ;

numberLiteral
    : MINUS? DECIMAL_VALUE  #decimalLiteral
    | MINUS? DOUBLE_VALUE   #doubleLiteral
    | MINUS? INTEGER_VALUE  #integerLiteral
    ;

stringLiteralSequence
    : string string+
    ;

dateLiteral
    : KW_DATE string
    | KW_CURRENT_DATE
    ;

timestampLiteral
    : KW_TIMESTAMP string
    | KW_CURRENT_TIMESTAMP
    ;

timestampLocalTZLiteral
    : KW_TIMESTAMPLOCALTZ string
    ;

intervalValue
    : string
    | numberLiteral
    ;

intervalLiteral
    : intervalValue intervalQualifiers
    ;

intervalExpression
    : LPAREN intervalValue RPAREN intervalQualifiers
    | KW_INTERVAL intervalValue intervalQualifiers
    | KW_INTERVAL LPAREN expression RPAREN intervalQualifiers
    ;

intervalQualifiers:
    KW_YEAR
    | KW_MONTH
    | KW_DAY
    | KW_HOUR
    | KW_MINUTE
    | KW_SECOND
    | KW_QUARTER
    | KW_WEEK

    ;

atomExpression
    : constant                                      #literal
    | intervalExpression                            #interval
    | castExpression                                 #cast
    | extractExpression                              #extract
    | floorExpression                               #floor
    | caseExpression                                #case
    | whenExpression                                 #when
    | tableOrColumn                                  #columnReference
    | base=atomExpression DOT fieldName=identifier   #dereference
    | rowConstructor                                 #rowExpression
    | functionExpression                             #functionCall
    | LPAREN query RPAREN                            #subqueryExpression
    | KW_EXISTS LPAREN query RPAREN                  #exists
    | KW_SUBSTRING LPAREN expression KW_FROM expression (KW_FOR expression)? RPAREN  #substring
    | KW_GROUPING LPAREN (qualifiedName (COMMA qualifiedName)*)? RPAREN                              #groupingOperation
    ;


functionExpression:
    qualifiedName LPAREN STAR RPAREN filter? over?
    | qualifiedName LPAREN (setQuantifier? expression (COMMA expression)*)?
                  (KW_ORDER KW_BY sortItem (COMMA sortItem)*)? RPAREN filter? (nullTreatment? over)?
 ;

nullTreatment
    : KW_IGNORE KW_NULLS
    | KW_RESPECT KW_NULLS
    ;

filter
    : KW_FILTER LPAREN KW_WHERE expression RPAREN
    ;


over
    : KW_OVER LPAREN
        (KW_PARTITION KW_BY partition+=expression (COMMA partition+=expression)*)?
        (KW_ORDER KW_BY sortItem (COMMA sortItem)*)?
        windowFrame?
     RPAREN
    ;


 windowFrame
     : frameType=KW_RANGE start=frameBound
     | frameType=KW_ROWS start=frameBound
     | frameType=KW_GROUPS start=frameBound
     | frameType=KW_RANGE KW_BETWEEN start=frameBound KW_AND end=frameBound
     | frameType=KW_ROWS KW_BETWEEN start=frameBound KW_AND end=frameBound
     | frameType=KW_GROUPS KW_BETWEEN start=frameBound KW_AND end=frameBound
     ;
frameBound
    : KW_UNBOUNDED boundType=KW_PRECEDING                 #unboundedFrame
    | KW_UNBOUNDED boundType=KW_FOLLOWING                 #unboundedFrame
    | KW_CURRENT KW_ROW                                   #currentRowBound
    | expression boundType=(KW_PRECEDING | KW_FOLLOWING)  #boundedFrame
    ;

isCondition
    : KW_NULL
    | KW_TRUE
    | KW_FALSE
    | KW_NOT KW_NULL
    | KW_NOT KW_TRUE
    | KW_NOT KW_FALSE
    ;



comparisonOperator:
    LESSTHANOREQUALTO
    | LESSTHAN
    | GREATERTHANOREQUALTO
    | GREATERTHAN
    | EQUAL
    | EQUAL_NS
    | NOTEQUAL
    ;

rowConstructor
    : LPAREN expression (COMMA expression)+ RPAREN
    | KW_ROW LPAREN expression (COMMA expression)* RPAREN
    ;

expression
    : atomExpression                                                                                #atom
    | operator=(PLUS | MINUS | TILDE) expression                                                    #unAryExpression
    | expression KW_IS isCondition                                                                  #isCondExpression
    | left=expression operator=(STAR | DIVIDE | MOD | DIV | PLUS | MINUS) right=expression          #arithmetic
    | left=expression operator=(CONCATENATE | AMPERSAND | BITWISEOR | BITWISEXOR)  right=expression #bitOperation
    | left=expression comparisonOperator right=expression                                           #comparison
    | left=expression KW_NOT? KW_IN expressionsInParenthesis                                        #inExpression
    | left=expression KW_NOT? KW_IN LPAREN query RPAREN                                             #inSubQuery
    | left=expression KW_NOT? KW_BETWEEN lower=expression KW_AND upper=expression                   #betweenPredict
    | left=expression KW_NOT? operator=(KW_LIKE|KW_RLIKE|KW_REGEXP) condition=(KW_ANY|KW_ALL)? right=expression                    #likeExpression
    | left=expression KW_IS KW_NOT? KW_DISTINCT KW_FROM right=expression                            #distinctFrom
    | KW_NOT expression                                                                             #notExpression
    | left=expression operator=KW_AND right=expression                                              #logicalBinary
    | left=expression operator=KW_OR right=expression                                               #logicalBinary
    | LPAREN expression RPAREN                                                                      #parenthesizedExpression
    ;



booleanValue
    : KW_TRUE
    | KW_FALSE
    ;


 setProperties :
    KW_PROPERTIES? tableProperties
  ;


identifier
      : Identifier
      | nonReserved
      | sql11ReservedKeywordsUsedAsIdentifier
      | sysFuncNames
      | TIME_ID
      ;

identifierWithoutSql11
    :
     Identifier
    | nonReserved
    ;

partitionSpec :
    KW_PARTITION LPAREN partitionExpression (COMMA partitionExpression)* RPAREN
;
partitionExpression:
    columnName EQUAL constant
    ;

nonReserved
    : KW_ADD |  KW_ASC  | KW_DESC
    | KW_CHANGE | KW_CLUSTER |  KW_COLUMNS | KW_COLUMN
    | KW_COMMENT | KW_DAY | KW_DOUBLE
    | KW_DATETIME | KW_IF | KW_PARTITION
    | KW_DISABLE | KW_DISTRIBUTE | KW_DOW
    | KW_ENABLE | KW_ENFORCED
    | KW_FIRST | KW_HOUR  | KW_TABLES
    | KW_LAST | KW_LIMIT | KW_OFFSET
    | KW_MINUTE | KW_MONTH |  KW_NULLS
    | KW_OVERWRITE | KW_QUARTER
    | KW_RENAME |  KW_REPLACE | KW_TYPE
    | KW_SECOND | KW_SEMI |  KW_SETS
    | KW_SHOW   |  KW_SORT | KW_STRING | KW_STRUCT
    | KW_TINYINT | KW_UNIONTYPE |  KW_UNSET
    | KW_WEEK |  KW_YEAR
    | KW_LEVEL | KW_RELY | KW_NORELY | KW_VALIDATE | KW_NOVALIDATE | KW_KEY | KW_DEFAULT
    | KW_CODE |  KW_DWS | KW_NORMAL | KW_TRANSACTION | KW_AGGREGATE
    | KW_PERIODIC_SNAPSHOT | KW_ACCUMULATING_SNAPSHOT | KW_CONSOLIDATED | KW_FACTLESS | KW_CALL
    | KW_FLOOR | KW_SOURCE | KW_URL | KW_PRIMARY |KW_BIZ_DATE | KW_TARGET | KW_TARGET_TYPE
    | KW_COPY_MODE | KW_PASSWORD | KW_MATERIALIZED | KW_ENGINE | KW_PROPERTIES
    | KW_OUTPUT | KW_RLIKE | KW_BERNOULLI | KW_SYSTEM | KW_RECURSIVE | KW_FETCH | KW_FIRST | KW_NEXT
    | KW_ONLY |  KW_TIES | KW_FOR | KW_SUBSTRING | KW_PIPE | KW_SYNC | KW_ASYNC | KW_COPY
    | KW_CASE | KW_WHEN | KW_THEN | KW_ELSE | KW_END | KW_LIMIT | KW_DERIVATIVE | KW_COMPOSITE
    | KW_ATOMIC | KW_ENUM | KW_LEVEL | KW_ORDINALITY | KW_MAP | KW_PROCESS | KW_PROCESSES
    | KW_DOMAIN | KW_DOMAINS | KW_DIALECT | KW_FULL_BU | KW_FULL_BUS | KW_ADJUNCT | KW_ADJUNCTS
    | KW_DICT | KW_DICTS | KW_TIMEPERIOD | KW_TIMEPERIODS | KW_MEASUREUNIT | KW_MEASUREUNITS
    | KW_INDICATOR | KW_INDICATORS | KW_BATCH | KW_BATCHES| KW_PATH | KW_LAYER | KW_COlGROUP | KW_ATTRIBUTE
    | KW_MEASUREMENT | KW_CORRELATION | KW_PARTITIONED | KW_INCLUDE | KW_EXCLUDE | KW_CHECKERS | KW_LAYERS
    | KW_DROP | KW_USE | KW_DATEFIELD | KW_UNNEST | KW_VIEW | KW_HAVING | KW_WHERE |  KW_PRESERVE | KW_JOIN
    | KW_CROSS | KW_TABLESAMPLE | KW_PRECISION | KW_CHECKER | KW_GROUPING | KW_ORDER
    | KW_CODES | KW_ALIAS | KW_FILTER | KW_EXTRACT | KW_ANY | KW_EXCEPT | ByteLengthLiteral
    | KW_CAST | KW_RESPECT | KW_DIM | KW_FACT | KW_INTERVAL | KW_ON | KW_GROUPS  | KW_CONSTRAINT | KW_TRANSFORM
    | KW_CURRENT | KW_FROM | KW_IGNORE | KW_REDUCE | KW_REFERENCES | KW_DISTINCT | KW_SQL | KW_TASK | KW_RULES | KW_RULE | KW_DQC_RULE
    | KW_CHECK | KW_NAMING | KW_REL_DIMENSION | KW_REL_INDICATOR | KW_REDUNDANT | KW_EXP | KW_EXPORT | KW_UNIQUE | KW_INDEX | KW_IMPORT
    | KW_CUSTOM | KW_BUSINESS_CATEGORIES | KW_BUSINESS_CATEGORY | KW_DIMENSION | KW_MARKET | KW_SUBJECT | KW_ATTRIBUTES | KW_DIMENSIONS | KW_DIM_ATTRIBUTES
    | KW_MARKETS | KW_SUBJECTS | KW_DYNAMIC | KW_ADS | KW_RENDER | KW_INCREASE | KW_VARCHAR | KW_DECREASE |KW_REF | KW_STAT_TIME | KW_IMPORT_SQL | KW_EXPORT_SQL
    | KW_STRONG | KW_WEAK | KW_SELECT | KW_PRECEDING | KW_UNBOUNDED | KW_FOLLOWING | KW_ADVANCED | KW_MATERIALIZED | KW_TIMESTAMPLOCALTZ
    | KW_UNIONTYPE | KW_FORMAT | KW_DEPENDENCY | KW_HELP | KW_AFTER | KW_MOVE
    ;



sysFuncNames
    :
      KW_AND
    | KW_OR
    | KW_NOT
    | KW_LIKE
    | KW_IF
    | KW_CASE
    | KW_WHEN
    | KW_TINYINT
    | KW_SMALLINT
    | KW_INT
    | KW_BIGINT
    | KW_FLOAT
    | KW_DOUBLE
    | KW_BOOLEAN
    | KW_STRING
    | KW_BINARY
    | KW_ARRAY
    | KW_MAP
    | KW_STRUCT
    | KW_UNIONTYPE
    | DIV
    | KW_RLIKE
    | KW_REGEXP
    | KW_IN
    | KW_BETWEEN
    | KW_CHAR

    ;


//The following SQL2011 reserved keywords are used as identifiers in many q tests, they may be added back due to backward compatibility.
sql11ReservedKeywordsUsedAsIdentifier :
         KW_ALL | KW_ALTER | KW_ARRAY | KW_AS |  KW_BETWEEN | KW_BIGINT | KW_BINARY | KW_BOOLEAN
         | KW_BY | KW_CREATE | KW_CURRENT_DATE | KW_CURRENT_TIMESTAMP | KW_DATE | KW_DECIMAL | KW_DELETE | KW_DESCRIBE
         | KW_DOUBLE | KW_DROP | KW_EXISTS |  KW_FALSE | KW_FETCH | KW_FLOAT | KW_FOR | KW_FULL
         | KW_GROUP | KW_GROUPING |  KW_IN | KW_INNER | KW_INSERT | KW_INT | KW_INTERSECT | KW_INTO | KW_IS | KW_LATERAL
         | KW_LEFT | KW_LIKE | KW_NULL | KW_OF | KW_ORDER |  KW_OUTER | KW_PARTITION
         | KW_PERCENT |  KW_RANGE |  KW_RIGHT | KW_OVER | KW_TABLE
         | KW_ROLLUP | KW_ROW | KW_ROWS | KW_SET | KW_SMALLINT | KW_TIMESTAMP | KW_TO |  KW_TRUE
         | KW_TRUNCATE | KW_UNION |  KW_USER | KW_USING | KW_VALUES | KW_WITH | KW_WINDOW | KW_NATURAL
    ;


