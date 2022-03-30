parser grammar DimensionParser;

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


