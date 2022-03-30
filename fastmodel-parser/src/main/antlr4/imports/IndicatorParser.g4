parser grammar IndicatorParser;

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