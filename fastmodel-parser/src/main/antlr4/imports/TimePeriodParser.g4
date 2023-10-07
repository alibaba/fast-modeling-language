parser grammar TimePeriodParser;

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
