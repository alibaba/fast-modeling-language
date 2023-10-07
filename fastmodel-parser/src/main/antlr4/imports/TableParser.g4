parser grammar TableParser;



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

