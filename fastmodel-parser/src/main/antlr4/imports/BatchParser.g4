parser grammar BatchParser;


batchStatements:
    createBatch
;

createBatch
   : createBatchIndicator
  | createBatchDomain
  | createBatchDict
  ;


createBatchIndicator:
    KW_CREATE  replace? KW_BATCH KW_INDICATOR?
        ifNotExists?
        qualifiedName
       LPAREN
           batchElement (COMMA batchElement)*
       RPAREN
      (KW_WITH setProperties)?
;



createBatchDomain:
    KW_CREATE replace? KW_BATCH KW_DOMAIN
           ifNotExists?
           qualifiedName
           LPAREN
               domainElement (COMMA domainElement)*
           RPAREN
          (KW_WITH setProperties)?
;

createBatchDict
   : KW_CREATE replace? KW_BATCH KW_NAMING? KW_DICT
     ifNotExists?
     qualifiedName
     LPAREN
        batchDictElement (COMMA batchDictElement)*
     RPAREN
     comment?
     (KW_WITH setProperties)?
   ;

batchDictElement :
    identifier alias? comment? (KW_WITH setProperties)?
;

batchElement
    : indicatorDefine
      | timePeriod
      | defaultAdjunct
      | fromTable
      | dimTable
      | dimPath
      | dateField
    ;

domainElement
    : identifier comment? (KW_WITH setProperties)?
    ;

indicatorDefine
    : define=identifier (comment)? (KW_ADJUNCT adjunct)? KW_REFERENCES atomic=identifier (KW_AS expression)?
    ;

 adjunct:
     LPAREN (identifier (COMMA identifier)*) RPAREN
 ;


timePeriod
    : KW_TIMEPERIOD identifier
    ;

defaultAdjunct
    : KW_ADJUNCT LPAREN identifier(COMMA identifier)* RPAREN
    ;

fromTable
    : KW_FROM KW_TABLE LPAREN identifier (COMMA identifier)* RPAREN
    ;

dimTable
    : KW_DIM KW_TABLE LPAREN identifier (COMMA identifier)* RPAREN
    ;

dateField
    : KW_DATEFIELD LPAREN tableOrColumn COMMA pattern=string RPAREN
    ;

dimPath
 :  KW_DIM KW_PATH LPAREN path (COMMA path)* RPAREN
 ;

 path
 :  LESSTHAN identifier (COMMA identifier)* GREATERTHAN
 ;

tableName
    : identifier (DOT identifier)*
    | identifier
    ;
