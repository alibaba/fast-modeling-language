parser grammar DictParser;
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