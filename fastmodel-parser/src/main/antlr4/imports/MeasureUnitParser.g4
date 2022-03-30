parser grammar MeasureUnitParser;
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