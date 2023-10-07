parser grammar Rules;

ruleStatements:
    createRules
    | addRule
    | changeRule
    | dropRule
;


createRules:
    KW_CREATE replace? ruleLevel? KW_RULES ifNotExists? qualifiedName alias?
    KW_REFERENCES tableName partitionSpec?
    (LPAREN  columnOrTableRuleList RPAREN)?
    comment?
    (KW_WITH setProperties)?
;


alterRuleSuffix:
     qualifiedName
     | KW_REFERENCES tableName partitionSpec?
;


addRule:
    KW_ALTER ruleLevel? KW_RULES
    alterRuleSuffix
    KW_ADD KW_RULE LPAREN columnOrTableRuleList RPAREN
;


changeRule:
    KW_ALTER ruleLevel? KW_RULES
    alterRuleSuffix
    KW_CHANGE KW_RULE LPAREN changeRuleItem (COMMA changeRuleItem)* RPAREN
;

changeRuleItem:
    oldRule=identifier columnOrTableRule
;



dropRule :
    KW_ALTER ruleLevel? KW_RULES
    alterRuleSuffix KW_DROP KW_RULE ruleName=identifier
;



ruleLevel:
    KW_SQL | KW_TASK
;

ruleGrade :
    KW_STRONG | KW_WEAK
;
columnOrTableRuleList
:
   columnOrTableRule (COMMA columnOrTableRule)*
;

columnOrTableRule
: ruleGrade? identifier alias? strategy comment? KW_DISABLE?
;

strategy
: functionExpression (KW_AS colType)? comparisonOperator numberLiteral #fixedStrategy
| functionExpression (KW_AS colType)? volOperator? intervalVol #volStrategy
| KW_DYNAMIC '(' functionExpression (KW_AS colType)? COMMA numberLiteral ')' #dynamicStrategy
;


volOperator
:
    KW_INCREASE | KW_DECREASE
;

intervalVol
: LSQUARE low=numberLiteral COMMA high=numberLiteral RSQUARE
;

dropRules:
    KW_DROP KW_RULES ifExists? qualifiedName
;


