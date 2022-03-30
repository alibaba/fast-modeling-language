parser grammar BusinessProcessParser;
bpStatements:
    createBpStatement
    | renameBp
    | setBpComment
    | setBpAliasedName
    | setBpProperties
    | unSetBpProperties
    | dropBpStatement
;


createBpStatement
    : KW_CREATE replace? KW_PROCESS
      ifNotExists?
      qualifiedName
      alias?
      comment?
      (KW_WITH setProperties)?
    ;

  renameBp
        : KW_ALTER KW_PROCESS qualifiedName alterStatementSuffixRename
        ;

  setBpAliasedName
      : KW_ALTER KW_PROCESS qualifiedName setAliasedName
      ;

  setBpComment
        : KW_ALTER KW_PROCESS qualifiedName alterStatementSuffixSetComment
        ;

  setBpProperties
        : KW_ALTER KW_PROCESS  qualifiedName KW_SET setProperties
        ;

  unSetBpProperties
        : KW_ALTER KW_PROCESS  qualifiedName KW_UNSET unSetProperties
        ;

  dropBpStatement
            : KW_DROP KW_PROCESS qualifiedName
            ;