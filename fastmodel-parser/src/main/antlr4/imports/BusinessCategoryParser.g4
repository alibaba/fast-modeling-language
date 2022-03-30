parser grammar BusinessCategoryParser;
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