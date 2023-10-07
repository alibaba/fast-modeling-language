parser grammar ScriptParser;

scriptStatements:
     importEntityStatement | refEntityStatement
;

importEntityStatement
: KW_IMPORT qualifiedName (KW_AS identifier)?
;

refEntityStatement
: KW_REF leftTable=tableNameList leftTableComment=comment? refRelationType rightTable=tableNameList rightTableComent=comment? (COLON name=identifier)?
;


refRelationType:
    GREATERTHAN? LEFT_DIRECTION_RIGHT
    |  RIGHT_DIRECTON_LEFT LESSTHAN?
;


