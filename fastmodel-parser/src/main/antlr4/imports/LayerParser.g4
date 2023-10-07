parser grammar LayerParser;

layerStatements:
 createLayer
 | renameLayer
 | setLayerComment
 | setLayerProperties
 | addLayerChecker
 | dropLayerChecker
 | dropLayer
 | setLayerAlias

;

createLayer:
    KW_CREATE replace? KW_LAYER ifNotExists? qualifiedName
    alias?
    checkers?
    comment?
    (KW_WITH setProperties)?
;

checkers:
  LPAREN checker (COMMA checker)* RPAREN
;

checker:
   KW_CHECKER type=identifier name=identifier string comment?
;
renameLayer:
   KW_ALTER KW_LAYER qualifiedName alterStatementSuffixRename
;
setLayerComment:
   KW_ALTER KW_LAYER qualifiedName alterStatementSuffixSetComment
;
setLayerProperties:
   KW_ALTER KW_LAYER qualifiedName KW_SET setProperties
;

setLayerAlias:
    KW_ALTER KW_LAYER qualifiedName setAliasedName
;
addLayerChecker:
   KW_ALTER KW_LAYER qualifiedName KW_ADD checker
 ;

dropLayerChecker:
   KW_ALTER KW_LAYER qualifiedName KW_DROP KW_CHECKER identifier
;

dropLayer:
    KW_DROP KW_LAYER  ifExists?  qualifiedName
;