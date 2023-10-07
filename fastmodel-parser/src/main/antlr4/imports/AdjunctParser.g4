parser grammar AdjunctParser;

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