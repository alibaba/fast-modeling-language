parser grammar FastModelGrammarParser;


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
    | jsonType
    ;


jsonType
    : KW_JSON |
    KW_JSON LESSTHAN typeList GREATERTHAN
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
    | KW_UNIONTYPE | KW_FORMAT | KW_DEPENDENCY | KW_HELP | KW_AFTER | KW_MOVE | KW_STATISTIC | KW_VIEWS | KW_JSON | KW_ODS
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




buStatements:
    createBuStatement
    | setBuComment
    | setBuAlias
    ;

createBuStatement
    : KW_CREATE replace? KW_FULL_BU ifNotExists?
      qualifiedName
      alias?
      comment?
      (KW_WITH setProperties)?
    ;


setBuAlias
    : KW_ALTER KW_FULL_BU  identifier setAliasedName
    ;

setBuComment
    : KW_ALTER KW_FULL_BU  identifier alterStatementSuffixSetComment
    ;


domainStatements:
    createDomainStatement
    | setDomainComment
    | setDomainProperties
    | unSetDomainProperties
    | renameDomain
    | dropDomainStatement
    | setDomainAliasedName
    ;

createDomainStatement
    :KW_CREATE replace? KW_DOMAIN ifNotExists?
     qualifiedName alias? comment?
     (KW_WITH setProperties)?
    ;


setDomainComment
    :KW_ALTER KW_DOMAIN qualifiedName alterStatementSuffixSetComment
    ;


 setDomainAliasedName
    : KW_ALTER KW_DOMAIN qualifiedName setAliasedName
    ;

setDomainProperties
    :KW_ALTER KW_DOMAIN qualifiedName KW_SET setProperties
    ;

unSetDomainProperties
    : KW_ALTER KW_DOMAIN qualifiedName KW_UNSET unSetProperties
    ;

renameDomain
        : KW_ALTER KW_DOMAIN qualifiedName alterStatementSuffixRename
        ;

dropDomainStatement
    : KW_DROP KW_DOMAIN qualifiedName
    ;
bpStatements:
    createBpStatement
    | renameBp
    | setBpComment
    | setBpAliasedName
    | setBpProperties
    | unSetBpProperties
    | dropBpStatement
;


createBpStatement
    : KW_CREATE replace? KW_PROCESS
      ifNotExists?
      qualifiedName
      alias?
      comment?
      (KW_WITH setProperties)?
    ;

  renameBp
        : KW_ALTER KW_PROCESS qualifiedName alterStatementSuffixRename
        ;

  setBpAliasedName
      : KW_ALTER KW_PROCESS qualifiedName setAliasedName
      ;

  setBpComment
        : KW_ALTER KW_PROCESS qualifiedName alterStatementSuffixSetComment
        ;

  setBpProperties
        : KW_ALTER KW_PROCESS  qualifiedName KW_SET setProperties
        ;

  unSetBpProperties
        : KW_ALTER KW_PROCESS  qualifiedName KW_UNSET unSetProperties
        ;

  dropBpStatement
            : KW_DROP KW_PROCESS qualifiedName
            ;



tableStatements:
    createTableStatement
    | renameTable
    | addCol
    | dropCol
    | changeCol
    | addPartitionCol
    | dropPartitionCol
    | addConstraint
    | dropConstraint
    | setTableComment
    | setTableProperties
    | unSetTableProperties
    | dropTableStatement
    | setTableAliasedName
    | trunctTable
    | createIndex
    | dropIndex
;

createTableStatement
    : KW_CREATE (replace)? tableType KW_TABLE
      ifNotExists?
      tableName
      alias?
      (LPAREN  columnNameTypeOrConstraintList RPAREN)?
      comment?
      tablePartition?
      (KW_WITH setProperties)?
      likeTablePredicate?
    ;


    likeTablePredicate:
        KW_LIKE likeTable=tableName
    ;



    tablePartition
        : KW_PARTITIONED KW_BY LPAREN columnNameTypeOrConstraintList RPAREN
        ;

    tableType
        : dimDetailType? type=KW_DIM
        | factDetailType? type=KW_FACT
        | type=KW_CODE
        | dwDetailType? type=KW_DWS
        | type=KW_ADS
        | type=KW_ODS
        ;

    dwDetailType:
        KW_ADVANCED
        ;


    dimDetailType
        : KW_LEVEL
        | KW_ENUM
        | KW_NORMAL
        ;

    factDetailType
        : KW_PERIODIC_SNAPSHOT
        | KW_ACCUMULATING_SNAPSHOT
        | KW_AGGREGATE
        | KW_CONSOLIDATED
        | KW_TRANSACTION
        | KW_FACTLESS
        ;
    columnNameTypeOrConstraintList
        : columnNameTypeOrConstraint (COMMA columnNameTypeOrConstraint)*
        ;


    columnNameTypeOrConstraint
        : columnDefinition
        | tableConstraint
        | tableIndex
        ;


    tableIndex
        : (KW_INDEX | KW_KEY) identifier indexColumnNames indexOption?
        ;

     createIndex
         : KW_CREATE KW_INDEX qualifiedName KW_ON tableName indexColumnNames indexOption?
         ;

     dropIndex
         : KW_DROP KW_INDEX qualifiedName KW_ON tableName
         ;

     indexColumnNames
         : '(' indexColumnName (',' indexColumnName)* ')'
         ;


     indexColumnName
         : columnName ('(' numberLiteral ')')? sortType=(KW_ASC | KW_DESC)?
         ;


    indexOption
        :  KW_WITH setProperties
        ;

    tableConstraint
        : createDimKey
        | primaryConstraint
        | createLevelConstraint
        | colGroupConstraint
        | timePeriodConstraint
        | redundantConstraint
        | uniqueConstraint
        ;

    colGroupConstraint
        : (KW_CONSTRAINT identifier)? KW_COlGROUP KW_KEY? columnParenthesesList
        ;

    createDimKey
        : (KW_CONSTRAINT identifier)? KW_DIM KW_KEY? columnParenthesesList? KW_REFERENCES tableName referenceColumnList?
        ;


    timePeriodConstraint
        : (KW_CONSTRAINT identifier)? KW_TIMEPERIOD KW_KEY? KW_REFERENCES columnParenthesesList
        ;

    redundantConstraint
        : (KW_CONSTRAINT identifier)? KW_REDUNDANT KW_KEY?  columnName KW_REFERENCES joinColumn referenceColumnList?
        ;

     uniqueConstraint
       :  (KW_CONSTRAINT identifier)? KW_UNIQUE KW_KEY? columnParenthesesList
       ;

    joinColumn
       : qualifiedName
       ;


     referenceColumnList
        : columnParenthesesList
        ;

    createLevelConstraint
        : (KW_CONSTRAINT identifier)? KW_LEVEL levelColParenthesesList (comment)?
        ;

    levelColParenthesesList
            : LESSTHAN levelCol GREATERTHAN
            ;

   levelCol
            : levelColPath (COMMA levelColPath)*
            ;

   levelColPath
            : columnName (COLON columnParenthesesList)?
            ;

  primaryConstraint
            : (KW_CONSTRAINT identifier)? KW_PRIMARY KW_KEY columnParenthesesList
            ;

    columnDefinition
            : identifier alias? colType category? columnConstraintType* defaultValue? comment? (KW_WITH tableProperties)? referencesObjects?
            ;

    category
            : KW_ATTRIBUTE
            | KW_MEASUREMENT
            | KW_CORRELATION
            | KW_REL_DIMENSION
            | KW_REL_INDICATOR
            | KW_STAT_TIME
            | KW_PARTITION
            ;

    referencesObjects:
            KW_REFERENCES (referenceColumnList | qualifiedName)
        ;

    addCol
            : KW_ALTER tableType? KW_TABLE tableName alterStatementSuffixAddCol
            ;

    renameTable
            : KW_ALTER tableType? KW_TABLE tableName alterStatementSuffixRename
            ;

   setTableProperties
                : KW_ALTER tableType? KW_TABLE tableName KW_SET setProperties
                ;

   unSetTableProperties
                : KW_ALTER tableType? KW_TABLE tableName KW_UNSET unSetProperties
                ;

   setTableComment
                : KW_ALTER tableType? KW_TABLE tableName alterStatementSuffixSetComment
                ;

   dropCol
                : KW_ALTER tableType? KW_TABLE tableName KW_DROP KW_COLUMN columnName
                ;

   setTableAliasedName
                : KW_ALTER tableType? KW_TABLE tableName setAliasedName
                ;


  addPartitionCol
    : KW_ALTER tableType? KW_TABLE tableName KW_ADD KW_PARTITION KW_COLUMN columnDefinition
    ;

  dropPartitionCol
    : KW_ALTER tableType? KW_TABLE tableName KW_DROP KW_PARTITION KW_COLUMN columnName
    ;

  addConstraint
    : KW_ALTER tableType? KW_TABLE tableName alterStatementSuffixAddConstraint
    ;

  dropConstraint
    : KW_ALTER tableType? KW_TABLE tableName alterStatementSuffixDropConstraint
    ;

   changeCol
    : KW_ALTER tableType? KW_TABLE tableName alterStatementSuffixRenameCol #changeAll
    | KW_ALTER tableType? KW_TABLE tableName KW_CHANGE KW_COLUMN left=identifier KW_RENAME KW_TO right=identifier #renameCol
    | KW_ALTER tableType? KW_TABLE tableName KW_CHANGE KW_COLUMN identifier KW_COMMENT string #setColComment
    | KW_ALTER tableType? KW_TABLE tableName KW_CHANGE KW_COLUMN identifier KW_SET setProperties #setColProperties
    | KW_ALTER tableType? KW_TABLE tableName KW_CHANGE KW_COLUMN identifier KW_UNSET unSetProperties #unSetColProperties
    | KW_ALTER tableType? KW_TABLE tableName KW_CHANGE KW_COLUMN left=identifier right=identifier colType (KW_FIRST | KW_AFTER bf=identifier) #setColumnOrder

    ;

  alterStatementSuffixDropConstraint
    : KW_DROP KW_CONSTRAINT identifier
    ;

  alterStatementSuffixAddCol
    : (KW_ADD | KW_REPLACE) KW_COLUMNS LPAREN columnNameTypeList RPAREN
    ;

 alterStatementSuffixAddConstraint
     : KW_ADD tableConstraint
         ;

 alterStatementSuffixRenameCol
        : KW_CHANGE KW_COLUMN? old=identifier newIdentifier=identifier alias? colType category? alterColumnConstraint[$newIdentifier.ctx]? comment? (KW_WITH setProperties)? referencesObjects?
        ;

enableSpecification
             : KW_ENABLE
             | KW_DISABLE
             ;

alterColumnConstraint[ParserRuleContext value]
            : columnConstraintType enableSpecification?
            ;


dropTableStatement
                : KW_DROP KW_TABLE ifExists? tableName
                ;


defaultValue
                : KW_DEFAULT constant
                ;

columnConstraintType
                : KW_NOT? KW_NULL
                | KW_PRIMARY KW_KEY
                ;
columnNameTypeList
        : columnNameType (COMMA columnNameType)*
        ;
columnNameType
        : columnDefinition
        ;
trunctTable :
    KW_TRUNCATE KW_TABLE? tableName partitionSpec?
;


indicatorStatements:
    renameIndicator
    | setIndicatorComment
    | setIndicatorProperties
    | dropIndicatorStatement
    | createIndicatorStatement
    | setIndicatorAlias
;
indicatorType
    : KW_ATOMIC KW_COMPOSITE?
    | KW_DERIVATIVE KW_COMPOSITE?
    ;
renameIndicator
    : KW_ALTER indicatorType? KW_INDICATOR qualifiedName alterStatementSuffixRename
    ;

setIndicatorComment
    : KW_ALTER indicatorType? KW_INDICATOR qualifiedName alterStatementSuffixSetComment
    ;

setIndicatorProperties
    : KW_ALTER indicatorType? KW_INDICATOR qualifiedName alterIndicatorSuffixProperties
    ;


alterIndicatorSuffixProperties
    : colType? (KW_REFERENCES tableName)? (KW_SET setProperties)? (KW_AS expression)?
    ;


setIndicatorAlias
    : KW_ALTER indicatorType? KW_INDICATOR qualifiedName setAliasedName
    ;

dropIndicatorStatement
    :KW_DROP KW_INDICATOR qualifiedName
    ;

createIndicatorStatement
        : KW_CREATE
          replace?
          indicatorType?
          KW_INDICATOR
          ifNotExists?
          qualifiedName
          alias?
          typeDbCol?
          (KW_REFERENCES tableName)?
          comment?
         (KW_WITH setProperties)?
         (KW_AS expression)?
        ;

materializeStatements
    : createMaterializeStatement
    | renameMaterialize
    | setMaterializeComment
    | setMaterializeProperties
    | dropMaterializeStatement
    | setMaterializeAlias
    ;
createMaterializeStatement
    : KW_CREATE replace? KW_MATERIALIZED KW_VIEW
      qualifiedName
      alias?
      KW_REFERENCES (LPAREN tableNameList RPAREN)
      comment?
      KW_ENGINE identifier
      (KW_WITH setProperties)?
    ;


renameMaterialize
        : KW_ALTER KW_MATERIALIZED KW_VIEW
                 qualifiedName alterStatementSuffixRename
        ;

 setMaterializeComment:
         KW_ALTER KW_MATERIALIZED KW_VIEW
         qualifiedName alterStatementSuffixSetComment
        ;

 setMaterializeAlias:
         KW_ALTER KW_MATERIALIZED KW_VIEW
         qualifiedName setAliasedName
 ;
setMaterializeProperties:
            KW_ALTER KW_MATERIALIZED KW_VIEW
            qualifiedName (KW_REFERENCES (LPAREN tableNameList RPAREN))?
                         (KW_ENGINE identifier)? alterMaterializeProperties
        ;

 alterMaterializeProperties
            : KW_SET KW_PROPERTIES tableProperties
            | KW_UNSET KW_PROPERTIES ifExists? tableProperties
            ;

 dropMaterializeStatement
            : KW_DROP KW_MATERIALIZED KW_VIEW  ifExists? qualifiedName
            ;

layerStatements:
 createLayer
 | renameLayer
 | setLayerComment
 | setLayerProperties
 | addLayerChecker
 | dropLayerChecker
 | dropLayer
 | setLayerAlias

;

createLayer:
    KW_CREATE replace? KW_LAYER ifNotExists? qualifiedName
    alias?
    checkers?
    comment?
    (KW_WITH setProperties)?
;

checkers:
  LPAREN checker (COMMA checker)* RPAREN
;

checker:
   KW_CHECKER type=identifier name=identifier string comment?
;
renameLayer:
   KW_ALTER KW_LAYER qualifiedName alterStatementSuffixRename
;
setLayerComment:
   KW_ALTER KW_LAYER qualifiedName alterStatementSuffixSetComment
;
setLayerProperties:
   KW_ALTER KW_LAYER qualifiedName KW_SET setProperties
;

setLayerAlias:
    KW_ALTER KW_LAYER qualifiedName setAliasedName
;
addLayerChecker:
   KW_ALTER KW_LAYER qualifiedName KW_ADD checker
 ;

dropLayerChecker:
   KW_ALTER KW_LAYER qualifiedName KW_DROP KW_CHECKER identifier
;

dropLayer:
    KW_DROP KW_LAYER  ifExists?  qualifiedName
;
 dictStatements:
  createDict
  | renameDict
  | setDictComment
  | setDictProperties
  | dropDataDict
  | setDictAlias
 ;
 createDict:
    KW_CREATE replace? KW_DICT ifNotExists? qualifiedName alias?
    typeDbCol  columnConstraintType? defaultValue? comment? (KW_WITH  setProperties)?
  ;

  setDictAlias:
    KW_ALTER KW_DICT qualifiedName setAliasedName
  ;
  renameDict:
    KW_ALTER KW_DICT qualifiedName alterStatementSuffixRename
;
 setDictComment:
    KW_ALTER KW_DICT qualifiedName alterStatementSuffixSetComment
 ;
setDictProperties:
    KW_ALTER KW_DICT qualifiedName typeDbCol?
    columnConstraintType? enableSpecification? defaultValue?
    KW_SET setProperties?
;

dropDataDict:
    KW_DROP KW_DICT qualifiedName
;

adjunctStatements:
 createAdjunct
 | renameAdjunct
 | setAdjunctComment
 | setAdjunctProperties
 | setAdjunctAlias
 | dropAdjunct
;

/**创建修饰词**/
createAdjunct:
    KW_CREATE replace? KW_ADJUNCT ifNotExists?
    qualifiedName alias?
    (comment)? (KW_WITH setProperties)?
    (KW_AS expression)?
    ;

setAdjunctAlias :
    KW_ALTER KW_ADJUNCT qualifiedName setAliasedName
    ;
renameAdjunct:
    KW_ALTER KW_ADJUNCT qualifiedName alterStatementSuffixRename
    ;
setAdjunctComment:
    KW_ALTER KW_ADJUNCT qualifiedName alterStatementSuffixSetComment
   ;

 setAdjunctProperties:
    KW_ALTER KW_ADJUNCT qualifiedName KW_SET setProperties? (KW_AS? expression)?
    ;

dropAdjunct:
    KW_DROP KW_ADJUNCT qualifiedName
    ;

timePeriodStatements:
    createTimePeriod
 | timePeriodExpression
 | renameTimePeriod
 | setTimePeriodComment
 | setTimePeriodProperties
 | dropTimePeriod
 | setTimePeriodAlias
;

createTimePeriod:
    KW_CREATE replace? KW_TIMEPERIOD ifNotExists? qualifiedName alias?
    comment? (KW_WITH setProperties)? (KW_AS timePeriodExpression)?
;

timePeriodExpression
    : KW_BETWEEN lower=expression KW_AND upper=expression
    ;

renameTimePeriod:
    KW_ALTER KW_TIMEPERIOD qualifiedName alterStatementSuffixRename;

 setTimePeriodComment:
    KW_ALTER KW_TIMEPERIOD qualifiedName alterStatementSuffixSetComment;


  setTimePeriodAlias:
    KW_ALTER KW_TIMEPERIOD qualifiedName setAliasedName
  ;

 setTimePeriodProperties:
    KW_ALTER KW_TIMEPERIOD qualifiedName KW_SET setProperties? (KW_AS? timePeriodExpression)?
    ;

  dropTimePeriod:
    KW_DROP KW_TIMEPERIOD  ifExists?  qualifiedName
   ;

groupStatements:
    createGroup
    | setGroupComment
    | setGroupProperties
    | dropGroup
    | renameGroup
    | setGroupAlias
;
createGroup:
    KW_CREATE replace? KW_GROUP type=groupType
    ifNotExists? qualifiedName
    alias?
    comment? (KW_WITH setProperties)?
;
groupType :
      KW_MEASUREUNIT
    | KW_DICT
    | KW_CODE
;

setGroupAlias :
    KW_ALTER KW_GROUP type=groupType qualifiedName setAliasedName
;

setGroupComment:
   KW_ALTER KW_GROUP type=groupType qualifiedName alterStatementSuffixSetComment
;
setGroupProperties:
   KW_ALTER KW_GROUP type=groupType qualifiedName KW_SET setProperties
;
dropGroup:
   KW_DROP KW_GROUP type=groupType qualifiedName
;
 renameGroup
    : KW_ALTER KW_GROUP type=groupType qualifiedName alterStatementSuffixRename
    ;
measureUnitStatements
: createMeasureUnit
| renameMeasureUnit
| setMeasureUnitComment
| setMeasureUnitProperties
| dropMeasureUnit
| setMeasureUnitAlias
;
createMeasureUnit:
    KW_CREATE  replace? KW_MEASUREUNIT ifNotExists? qualifiedName alias? comment? (KW_WITH setProperties)?
  ;
  renameMeasureUnit:
    KW_ALTER  KW_MEASUREUNIT qualifiedName alterStatementSuffixRename;

  setMeasureUnitComment:
    KW_ALTER KW_MEASUREUNIT qualifiedName alterStatementSuffixSetComment
  ;

  setMeasureUnitProperties:
     KW_ALTER KW_MEASUREUNIT qualifiedName KW_SET setProperties
  ;

  dropMeasureUnit:
    KW_DROP KW_MEASUREUNIT  ifExists?  qualifiedName
  ;

  setMeasureUnitAlias:
    KW_ALTER KW_MEASUREUNIT qualifiedName setAliasedName
  ;

showStatements:
     showCreate
     | showObjects
     | describe
     | showStatistic
   ;

showCreate:
    KW_SHOW KW_CREATE type=showType qualifiedName output?
;
output:
    KW_OUTPUT EQUAL identifier
;

showObjects:
    KW_SHOW (KW_FULL)? (type=showObjectTypes)
    ((KW_FROM | KW_IN) qualifiedName (COMMA qualifiedName)* )?
    ((KW_LIKE string) | (KW_WHERE expression))?
    (KW_OFFSET offset=INTEGER_VALUE)?
    (KW_LIMIT limit=INTEGER_VALUE)?
    ;


showStatistic:
    KW_SHOW KW_STATISTIC
    (showObjectTypes | singleStatisticObject)
;

singleStatisticObject:
    showType qualifiedName
;

describe:
    (KW_DESC | KW_DESCRIBE) type=showType qualifiedName
;

showObjectTypes:
    tableType? KW_TABLES
    | indicatorType? KW_INDICATORS
    | KW_DOMAINS
    | dictType? KW_DICTS
    | KW_TIMEPERIODS
    | KW_ADJUNCTS
    | KW_MEASUREUNITS
    | KW_PROCESSES
    | KW_LAYERS
    | KW_DICT KW_GROUPS
    | KW_MEASUREUNIT KW_GROUPS
    | KW_BUSINESS_CATEGORIES
    | KW_DIMENSIONS
    | KW_MARKETS
    | KW_SUBJECTS
    | KW_DIM_ATTRIBUTES
    | KW_CODES
    | KW_COLUMNS
    | KW_MATERIALIZED KW_VIEWS
;

dictType:
    KW_NAMING
;


showType:
    KW_TABLE
    | indicatorType? KW_INDICATOR
    | KW_DOMAIN
    | KW_DICT
    | KW_TIMEPERIOD
    | KW_ADJUNCT
    | KW_MEASUREUNIT
    | KW_PROCESS
    | KW_LAYER
    | KW_CODE
    | KW_GROUP
    | KW_PROCESS
    | KW_BUSINESS_CATEGORY
    | KW_MARKET
    | KW_SUBJECT
  ;


batchStatements:
    createBatch
;

createBatch
   : createBatchIndicator
  | createBatchDomain
  | createBatchDict
  ;


createBatchIndicator:
    KW_CREATE  replace? KW_BATCH KW_INDICATOR?
        ifNotExists?
        qualifiedName
       LPAREN
           batchElement (COMMA batchElement)*
       RPAREN
      (KW_WITH setProperties)?
;



createBatchDomain:
    KW_CREATE replace? KW_BATCH KW_DOMAIN
           ifNotExists?
           qualifiedName
           LPAREN
               domainElement (COMMA domainElement)*
           RPAREN
          (KW_WITH setProperties)?
;

createBatchDict
   : KW_CREATE replace? KW_BATCH KW_NAMING? KW_DICT
     ifNotExists?
     qualifiedName
     LPAREN
        batchDictElement (COMMA batchDictElement)*
     RPAREN
     comment?
     (KW_WITH setProperties)?
   ;

batchDictElement :
    identifier alias? comment? (KW_WITH setProperties)?
;

batchElement
    : indicatorDefine
      | timePeriod
      | defaultAdjunct
      | fromTable
      | dimTable
      | dimPath
      | dateField
    ;

domainElement
    : identifier comment? (KW_WITH setProperties)?
    ;

indicatorDefine
    : define=identifier (comment)? (KW_ADJUNCT adjunct)? KW_REFERENCES atomic=identifier (KW_AS expression)?
    ;

 adjunct:
     LPAREN (identifier (COMMA identifier)*) RPAREN
 ;


timePeriod
    : KW_TIMEPERIOD identifier
    ;

defaultAdjunct
    : KW_ADJUNCT LPAREN identifier(COMMA identifier)* RPAREN
    ;

fromTable
    : KW_FROM KW_TABLE LPAREN identifier (COMMA identifier)* RPAREN
    ;

dimTable
    : KW_DIM KW_TABLE LPAREN identifier (COMMA identifier)* RPAREN
    ;

dateField
    : KW_DATEFIELD LPAREN tableOrColumn COMMA pattern=string RPAREN
    ;

dimPath
 :  KW_DIM KW_PATH LPAREN path (COMMA path)* RPAREN
 ;

 path
 :  LESSTHAN identifier (COMMA identifier)* GREATERTHAN
 ;

tableName
    : identifier (DOT identifier)*
    | identifier
    ;

pipeStatements :
    createPipe
;

createPipe :
    KW_CREATE replace? pipeType? KW_PIPE ifNotExists?
    qualifiedName alias? comment?
    (KW_WITH setProperties)?
    KW_AS
    copyIntoFrom
;


copyIntoFrom :
    KW_COPY KW_INTO
    targetType
    (KW_WITH copy=keyValuePairs)?
    KW_FROM tableName
    KW_WHERE
    expression
;



keyValuePairs :
    LPAREN
       keyValue (COMMA keyValue)*
    RPAREN
;
keyValue :
    pipeKey EQUAL constant
;

targetType:
    KW_TABLE
    | KW_DICT
;

pipeType:
    KW_SYNC
    | KW_ASYNC
;

pipeKey :
    KW_TARGET
    | KW_COPY_MODE
;

queryOrInsertStatements:
    query
    | insertInto
    | delete
;

query
    :  with? queryNoWith
    ;


insertInto
    : KW_INSERT (KW_INTO | KW_OVERWRITE) tableName
    partitionSpec? columnParenthesesList? query
    ;


delete:
        KW_DELETE deleteType? KW_FROM qualifiedName KW_WHERE expression
    ;

deleteType :
        KW_TABLE
        | indicatorType? KW_INDICATOR
    ;


with
    : KW_WITH KW_RECURSIVE? namedQuery (COMMA namedQuery)*
    ;

queryNoWith:
      queryTerm
      (KW_ORDER KW_BY sortItem (COMMA sortItem)*)?
      clusterByClause?
      distributeByClause?
      sortByClause?
      (KW_OFFSET offset=rowCount (KW_ROW | KW_ROWS)?)?
      ((KW_LIMIT limit=limitRowCount) | (KW_FETCH (KW_FIRST | KW_NEXT) (fetchFirst=rowCount)? (KW_ROW | KW_ROWS) (KW_ONLY | KW_WITH KW_TIES)))?
      ;

clusterByClause:
    KW_CLUSTER KW_BY expression (COMMA expression)*
    ;

distributeByClause:
    KW_DISTRIBUTE KW_BY sortItem (COMMA sortItem)*
    ;

sortByClause:
    KW_SORT KW_BY sortItem (COMMA sortItem)*
    ;

limitRowCount
    : KW_ALL
    | rowCount
    ;

rowCount
    : INTEGER_VALUE
    | '?'
    ;

queryTerm
    : queryPrimary                                                             #queryTermDefault
    | left=queryTerm operator=KW_INTERSECT setQuantifier? right=queryTerm         #setOperation
    | left=queryTerm operator=(KW_UNION | KW_EXCEPT) setQuantifier? right=queryTerm  #setOperation
    ;

queryPrimary
    : querySpecification                   #queryPrimaryDefault
    | KW_TABLE qualifiedName                  #table
    | KW_VALUES expression (COMMA expression)*  #inlineTable
    | LPAREN queryNoWith  RPAREN                 #subquery
    ;

sortItem
    : expression ordering=(KW_ASC | KW_DESC)? (KW_NULLS nullOrdering=(KW_FIRST | KW_LAST))?
    ;

querySpecification
    : KW_SELECT (hints=hint)? setQuantifier? selectItem (',' selectItem)*
      (KW_FROM relation (COMMA relation)*)?
      (KW_WHERE where=expression)?
      (KW_GROUP KW_BY groupBy)?
      (KW_HAVING having=expression)?
    ;
hint
    : HINT_START hintStatements+=hintStatement (COMMA? hintStatements+=hintStatement)* HINT_END
    ;

hintStatement
    : hintName=identifier
    | hintName=identifier LPAREN parameters+=expression (COMMA parameters+=expression)* RPAREN
    ;

groupBy
    : setQuantifier? groupingElement (COMMA groupingElement)*
    ;

groupingElement
    : groupingSet                                            #singleGroupingSet
    | KW_ROLLUP LPAREN (expression (COMMA expression)*)? RPAREN         #rollup
    | KW_CUBE LPAREN (expression (COMMA expression)*)? RPAREN           #cube
    | KW_GROUPING KW_SETS LPAREN groupingSet (COMMA groupingSet)* RPAREN   #multipleGroupingSets
    ;

groupingSet
    : LPAREN (expression (COMMA expression)*)? RPAREN
    | expression
    ;

namedQuery
    : name=identifier (columnAliases)? KW_AS LPAREN query RPAREN
    ;

setQuantifier
    : KW_DISTINCT
    | KW_ALL
    ;

selectItem
    : expression (KW_AS? identifier)?                          #selectSingle
    | atomExpression DOT STAR (KW_AS columnAliases)?       #selectAll
    | STAR                                              #selectAll
    ;

relation
    : left=relation
      ( KW_CROSS KW_JOIN right=sampledRelation
      | joinType KW_JOIN rightRelation=relation joinCriteria
      | KW_NATURAL joinType KW_JOIN right=sampledRelation
      )                                           #joinRelation
    | sampledRelation                             #relationDefault
    ;

joinType
    : KW_INNER?
    | KW_LEFT KW_OUTER?
    | KW_RIGHT KW_OUTER?
    | KW_FULL KW_OUTER?
    ;

joinCriteria
    : KW_ON expression
    | KW_USING LPAREN identifier (COMMA identifier)* RPAREN
    ;

sampledRelation
    : aliasedRelation (
        KW_TABLESAMPLE sampleType LPAREN percentage=expression RPAREN
      )?
    ;

sampleType
    : KW_BERNOULLI
    | KW_SYSTEM
    ;

aliasedRelation
    : relationPrimary (KW_AS? identifierWithoutSql11 columnAliases?)?
    ;

columnAliases
    : LPAREN identifier (COMMA identifier)* RPAREN
    ;

relationPrimary
    : qualifiedName                                                   #tableQualifiedName
    | LPAREN query RPAREN                                                   #subqueryRelation
    | KW_UNNEST LPAREN expression (COMMA expression)* RPAREN (KW_WITH KW_ORDINALITY)?  #unnest
    | KW_LATERAL LPAREN query RPAREN                                           #lateral
    | LPAREN relation RPAREN                                                #parenthesizedRelation
    ;


call :
    KW_CALL functionExpression
;

ruleStatements:
    createRules
    | addRule
    | changeRule
    | dropRule
;


createRules:
    KW_CREATE replace? ruleLevel? KW_RULES ifNotExists? qualifiedName alias?
    KW_REFERENCES tableName partitionSpec?
    (LPAREN  columnOrTableRuleList RPAREN)?
    comment?
    (KW_WITH setProperties)?
;


alterRuleSuffix:
     qualifiedName
     | KW_REFERENCES tableName partitionSpec?
;


addRule:
    KW_ALTER ruleLevel? KW_RULES
    alterRuleSuffix
    KW_ADD KW_RULE LPAREN columnOrTableRuleList RPAREN
;


changeRule:
    KW_ALTER ruleLevel? KW_RULES
    alterRuleSuffix
    KW_CHANGE KW_RULE LPAREN changeRuleItem (COMMA changeRuleItem)* RPAREN
;

changeRuleItem:
    oldRule=identifier columnOrTableRule
;



dropRule :
    KW_ALTER ruleLevel? KW_RULES
    alterRuleSuffix KW_DROP KW_RULE ruleName=identifier
;



ruleLevel:
    KW_SQL | KW_TASK
;

ruleGrade :
    KW_STRONG | KW_WEAK
;
columnOrTableRuleList
:
   columnOrTableRule (COMMA columnOrTableRule)*
;

columnOrTableRule
: ruleGrade? identifier alias? strategy comment? KW_DISABLE?
;

strategy
: functionExpression (KW_AS colType)? comparisonOperator numberLiteral #fixedStrategy
| functionExpression (KW_AS colType)? volOperator? intervalVol #volStrategy
| KW_DYNAMIC '(' functionExpression (KW_AS colType)? COMMA numberLiteral ')' #dynamicStrategy
;


volOperator
:
    KW_INCREASE | KW_DECREASE
;

intervalVol
: LSQUARE low=numberLiteral COMMA high=numberLiteral RSQUARE
;

dropRules:
    KW_DROP KW_RULES ifExists? qualifiedName
;



dqcRuleStatements:
    createDqcRule
    | addDqcRule
    | changeDqcRule
    | dropDqcRule
;


createDqcRule:
    KW_CREATE replace? KW_DQC_RULE ifNotExists? qualifiedName alias?
    KW_ON KW_TABLE tableName partitionSpec?
    (LPAREN  dqcColumnOrTableRuleList RPAREN)?
    comment?
    (KW_WITH setProperties)?
;


alterDqcRuleSuffix:
     qualifiedName
     | KW_ON KW_TABLE tableName partitionSpec?
;


addDqcRule:
    KW_ALTER  KW_DQC_RULE
    alterDqcRuleSuffix
    KW_ADD LPAREN dqcColumnOrTableRuleList RPAREN
;


changeDqcRule:
    KW_ALTER  KW_DQC_RULE
    alterDqcRuleSuffix
    KW_CHANGE LPAREN changeDqcRuleItem (COMMA changeDqcRuleItem)* RPAREN
;



changeDqcRuleItem:
    oldRule=identifier dqcColumnOrTableRule
;



dropDqcRule :
    KW_ALTER KW_DQC_RULE
    alterDqcRuleSuffix KW_DROP ruleName=identifier
;



dqcColumnOrTableRuleList
:
   dqcColumnOrTableRule (COMMA dqcColumnOrTableRule)*
;

dqcColumnOrTableRule
:
   KW_CONSTRAINT constraint=identifier (KW_CHECK expression)? enforce? KW_DISABLE? #dqcTableRule
;


enforce :
    KW_NOT? KW_ENFORCED
;



businessCategoryStatements:
    createBusinessCategoryStatement
    | renameBusinessCategory
    | setBusinessCategoryAliasedName
    | setBusinessCategoryComment
    | setBusinessCategoryProperties
    | unSetBusinessCategoryProperties
    | dropBusinessCategoryStatement
;


createBusinessCategoryStatement
    : KW_CREATE replace? categoryType
      ifNotExists?
      qualifiedName
      alias?
      comment?
      (KW_WITH setProperties)?
    ;

categoryType
    : KW_BUSINESS_CATEGORY
    | KW_MARKET
    | KW_SUBJECT
    ;

renameBusinessCategory
    : KW_ALTER categoryType qualifiedName alterStatementSuffixRename
    ;

setBusinessCategoryAliasedName
    : KW_ALTER categoryType qualifiedName setAliasedName
    ;

setBusinessCategoryComment
    : KW_ALTER categoryType qualifiedName alterStatementSuffixSetComment
    ;

setBusinessCategoryProperties
    : KW_ALTER categoryType  qualifiedName KW_SET setProperties
    ;

unSetBusinessCategoryProperties
    : KW_ALTER categoryType  qualifiedName KW_UNSET unSetProperties
    ;

dropBusinessCategoryStatement
    : KW_DROP categoryType qualifiedName
    ;

impExpStatements:
   exportStatement
;


exportStatement
: (KW_EXP | KW_EXPORT) exportBusinessUnit? exportOutput exportTarget? KW_WHERE expression
;


exportOutput
: KW_OUTPUT EQUAL string
;

exportBusinessUnit
: identifier
;

exportTarget
: KW_TARGET EQUAL string
;

scriptStatements:
     importEntityStatement | refEntityStatement
;

importEntityStatement
: KW_IMPORT qualifiedName (KW_AS identifier)?
;

refEntityStatement
: KW_REF leftTable=tableNameList leftTableComment=comment? refRelationType rightTable=tableNameList rightTableComent=comment? (COLON name=identifier)?
;


refRelationType:
    GREATERTHAN? LEFT_DIRECTION_RIGHT
    |  RIGHT_DIRECTON_LEFT LESSTHAN?
;



dimensionStatements:
    createDimension
    | renameDimension
    | addDimensionAttribute
    | changeDimensionAttribute
    | dropDimensionAttribute
    | setDimensionComment
    | setDimensionProperties
    | unSetDimensionProperties
    | dropDimension
    | setDimensionAliasedName
    ;

createDimension
    : KW_CREATE KW_DIMENSION
      ifNotExists?
      tableName
      alias?
      (LPAREN  dimensionAttributeList RPAREN)?
      comment?
      (KW_WITH setProperties)?
    ;

    dimensionAttributeList
        : dimensionAttribute (COMMA dimensionAttribute)*
        ;


    dimensionAttribute
            : identifier alias? attributeCategory? attributeConstraint? comment? (KW_WITH setProperties)?
            ;

    attributeCategory
            : KW_TIMEPERIOD
            ;
    attributeConstraint
            : KW_PRIMARY KW_KEY
            ;

    addDimensionAttribute
            : KW_ALTER  KW_DIMENSION tableName (KW_ADD | KW_REPLACE) KW_ATTRIBUTES LPAREN dimensionAttributeList RPAREN
            ;

    renameDimension
            : KW_ALTER KW_DIMENSION tableName alterStatementSuffixRename
            ;

   setDimensionProperties
            : KW_ALTER KW_DIMENSION tableName KW_SET setProperties
            ;

   unSetDimensionProperties
            : KW_ALTER KW_DIMENSION tableName KW_UNSET unSetProperties
            ;

   setDimensionComment
            : KW_ALTER  KW_DIMENSION tableName alterStatementSuffixSetComment
            ;

   dropDimensionAttribute
                : KW_ALTER KW_DIMENSION tableName KW_DROP KW_ATTRIBUTE identifier
                ;

   setDimensionAliasedName
                : KW_ALTER  KW_DIMENSION tableName setAliasedName
                ;

   changeDimensionAttribute
                : KW_ALTER KW_DIMENSION tableName KW_CHANGE KW_ATTRIBUTE old=identifier dimensionAttribute
                ;

   dropDimension
                : KW_DROP KW_DIMENSION ifExists? tableName
                ;



commandStatements :
      commonSqlCommandStatement
      | helpCommandStatement
      ;

commonSqlCommandStatement:
   commands dialectOptions commandOptions  (KW_WITH setProperties)?
;

helpCommandStatement :
   KW_HELP type=identifier (TEXT_OPTION string)? (KW_WITH setProperties)?
;



dialectOptions:
     MODE_OPTION identifier
    ;
commandOptions
    : TEXT_OPTION string
    | URI_OPTION string
    ;

commands :
    type=KW_IMPORT_SQL
    | type=KW_EXPORT_SQL
    | type=KW_RENDER
    | type=KW_FORMAT
;




referencesStatements :
        showReferences
      | moveReferences
      ;

moveReferences :
    KW_MOVE showType KW_REFERENCES KW_FROM from=qualifiedName KW_TO  to=qualifiedName (KW_WITH setProperties)?
    ;

showReferences :
    KW_SHOW showType KW_REFERENCES KW_FROM from=qualifiedName (KW_WITH setProperties)?
;
