parser grammar PipeParser;

pipeStatements :
    createPipe
;

createPipe :
    KW_CREATE replace? pipeType? KW_PIPE ifNotExists?
    qualifiedName alias? comment?
    (KW_WITH setProperties)?
    KW_AS
    copyIntoFrom
;


copyIntoFrom :
    KW_COPY KW_INTO
    targetType
    (KW_WITH copy=keyValuePairs)?
    KW_FROM tableName
    KW_WHERE
    expression
;



keyValuePairs :
    LPAREN
       keyValue (COMMA keyValue)*
    RPAREN
;
keyValue :
    pipeKey EQUAL constant
;

targetType:
    KW_TABLE
    | KW_DICT
;

pipeType:
    KW_SYNC
    | KW_ASYNC
;

pipeKey :
    KW_TARGET
    | KW_COPY_MODE
;