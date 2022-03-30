parser grammar BusinessUnitParser;


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