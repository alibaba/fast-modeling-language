parser grammar DqcRule;

dqcRuleStatements:
    createDqcRule
    | addDqcRule
    | changeDqcRule
    | dropDqcRule
;


createDqcRule:
    KW_CREATE replace? KW_DQC_RULE ifNotExists? qualifiedName alias?
    KW_ON KW_TABLE tableName partitionSpec?
    (LPAREN  dqcColumnOrTableRuleList RPAREN)?
    comment?
    (KW_WITH setProperties)?
;


alterDqcRuleSuffix:
     qualifiedName
     | KW_ON KW_TABLE tableName partitionSpec?
;


addDqcRule:
    KW_ALTER  KW_DQC_RULE
    alterDqcRuleSuffix
    KW_ADD LPAREN dqcColumnOrTableRuleList RPAREN
;


changeDqcRule:
    KW_ALTER  KW_DQC_RULE
    alterDqcRuleSuffix
    KW_CHANGE LPAREN changeDqcRuleItem (COMMA changeDqcRuleItem)* RPAREN
;



changeDqcRuleItem:
    oldRule=identifier dqcColumnOrTableRule
;



dropDqcRule :
    KW_ALTER KW_DQC_RULE
    alterDqcRuleSuffix KW_DROP ruleName=identifier
;



dqcColumnOrTableRuleList
:
   dqcColumnOrTableRule (COMMA dqcColumnOrTableRule)*
;

dqcColumnOrTableRule
:
   KW_CONSTRAINT constraint=identifier (KW_CHECK expression)? enforce? KW_DISABLE? #dqcTableRule
;


enforce :
    KW_NOT? KW_ENFORCED
;



