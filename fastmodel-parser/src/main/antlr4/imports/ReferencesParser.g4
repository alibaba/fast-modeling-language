parser grammar ReferencesParser;

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