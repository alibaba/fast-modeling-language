parser grammar MaterializeParser;

materializeStatements
    : createMaterializeStatement
    | renameMaterialize
    | setMaterializeComment
    | setMaterializeProperties
    | dropMaterializeStatement
    | setMaterializeAlias
    ;
createMaterializeStatement
    : KW_CREATE replace? KW_MATERIALIZED KW_VIEW
      qualifiedName
      alias?
      KW_REFERENCES (LPAREN tableNameList RPAREN)
      comment?
      KW_ENGINE identifier
      (KW_WITH setProperties)?
    ;


renameMaterialize
        : KW_ALTER KW_MATERIALIZED KW_VIEW
                 qualifiedName alterStatementSuffixRename
        ;

 setMaterializeComment:
         KW_ALTER KW_MATERIALIZED KW_VIEW
         qualifiedName alterStatementSuffixSetComment
        ;

 setMaterializeAlias:
         KW_ALTER KW_MATERIALIZED KW_VIEW
         qualifiedName setAliasedName
 ;
setMaterializeProperties:
            KW_ALTER KW_MATERIALIZED KW_VIEW
            qualifiedName (KW_REFERENCES (LPAREN tableNameList RPAREN))?
                         (KW_ENGINE identifier)? alterMaterializeProperties
        ;

 alterMaterializeProperties
            : KW_SET KW_PROPERTIES tableProperties
            | KW_UNSET KW_PROPERTIES ifExists? tableProperties
            ;

 dropMaterializeStatement
            : KW_DROP KW_MATERIALIZED KW_VIEW  ifExists? qualifiedName
            ;