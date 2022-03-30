parser grammar GroupParser;

groupStatements:
    createGroup
    | setGroupComment
    | setGroupProperties
    | dropGroup
    | renameGroup
    | setGroupAlias
;
createGroup:
    KW_CREATE replace? KW_GROUP type=groupType
    ifNotExists? qualifiedName
    alias?
    comment? (KW_WITH setProperties)?
;
groupType :
      KW_MEASUREUNIT
    | KW_DICT
    | KW_CODE
;

setGroupAlias :
    KW_ALTER KW_GROUP type=groupType qualifiedName setAliasedName
;

setGroupComment:
   KW_ALTER KW_GROUP type=groupType qualifiedName alterStatementSuffixSetComment
;
setGroupProperties:
   KW_ALTER KW_GROUP type=groupType qualifiedName KW_SET setProperties
;
dropGroup:
   KW_DROP KW_GROUP type=groupType qualifiedName
;
 renameGroup
    : KW_ALTER KW_GROUP type=groupType qualifiedName alterStatementSuffixRename
    ;