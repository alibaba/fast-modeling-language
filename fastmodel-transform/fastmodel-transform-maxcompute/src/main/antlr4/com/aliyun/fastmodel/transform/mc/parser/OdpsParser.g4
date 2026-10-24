parser grammar OdpsParser;

@header {
import java.util.ArrayDeque;
import java.util.Deque;
}

@members {
/* This field stores the tags which are used to detect the end of a dollar-quoted string literal.
 */
private final Deque<String> _tags = new ArrayDeque<String>();
}

options
{
    tokenVocab=OdpsLexer;
    contextSuperClass=OdpsParserRuleContext;
}

// starting rule
script :
    (stmt+=statement)+ (cb+=userCodeBlock)* EOF;

userCodeBlock
    : (begin=KW_CODE_BEGIN) LPAREN tablePropertiesList RPAREN (c+=~KW_CODE_END)+? (end=KW_CODE_END) emptyStatement*
    ;

// add compound statement and change delimiter to ';'
statement
  : explainStatement SEMICOLON
  | costSqlStatement SEMICOLON
  | compoundStatement
  | execStatement SEMICOLON
  | ifStatement
  | loopStatement
  | setStatement SEMICOLON
  | setProjectStatement SEMICOLON
  | funDef = functionDefinition
  | emptyStatement
	;

// compound statement defnition
compoundStatement
    : KW_BEGIN statement* KW_END
    ;

emptyStatement
    :
    SEMICOLON
    ;

// add assign statement, function definition statement, if statement and loop statement
execStatement
    :
    queryStatement
    | dmlStatement
    | loadStatement
    | exportStatement
    | importStatement
    | readStatement
    | ddlStatement
    | assignStatement
    | authorizationStatement
    | statisticStatement
    | resourceManagement
    | instanceManagement
    | undoStatement
    | redoStatement
    | purgeStatement
    | dropTableVairableStatement
    | msckRepairTableStatement
    | createMachineLearningModelStatment
    | callStatement
    | windowClauseStatement
    ;

callStatement :
    KW_CALL name=functionName
    LPAREN
    (arg+=functionArgument (COMMA arg+=functionArgument)*)?
    RPAREN
    ;

cteStatement
    :
    id = identifier
    ( LPAREN cols=columnNameCommentList RPAREN
    | param=functionParameters (KW_RETURNS retvar=variableName retType=parameterTypeDeclaration)?)?
    KW_AS (LPAREN (exp=expression | queryExp=query) RPAREN | cpd=compoundStatement)
    ;

tableAliasWithCols
    :
    KW_AS? table=identifier (LPAREN col+=identifier (COMMA col+=identifier)* RPAREN)?
    ;

tableAliasWithoutCols
    :
    KW_AS? table=identifier
    ;

subQuerySource
    :
    subQuery=subQueryExpression alias = tableAliasWithCols?
    ;

explainStatement
	: KW_EXPLAIN (
	    option=explainOption* execStatement
        |
        KW_REWRITE query)
	;

costSqlStatement
	: KW_COST KW_SQL execStatement
	;

// if statement
ifStatement
    : KW_IF exp=expression trueStatement=statement (KW_ELSE falseStatement=statement)?
    ;

// loop statement
loopStatement
    : KW_LOOP var=variableName KW_FROM from=expression KW_TO to=expression stmt=statement
    ;

// function definition statement
functionDefinition
    :
    KW_FUNCTION
    name=functionIdentifier
    param=functionParameters
    (KW_RETURNS retvar=variableName retType=parameterTypeDeclaration)?
    KW_AS
    (funBody=compoundStatement | expr=expression SEMICOLON | queryExp=query SEMICOLON)
    ;

functionParameters:
    LPAREN (param+=parameterDefinition (COMMA param+=parameterDefinition)*)? RPAREN;

parameterDefinition:
    var=variableName decl=parameterTypeDeclaration (EQUAL init=expression)? commentClause?;

typeDeclaration:
    KW_TABLE LPAREN columnsType=columnNameTypeList RPAREN
    |
    singleType=colType
    ;

parameterTypeDeclaration:
    KW_TABLE LPAREN (var=varSizeParam | columnsType=parameterColumnNameTypeList (COMMA var=varSizeParam)?) RPAREN
    |
    funType = functionTypeDeclaration
    |
    singleType=anyType
    ;

functionTypeDeclaration
    : KW_FUNCTION LPAREN argType = parameterTypeDeclarationList? RPAREN KW_RETURNS ret=parameterTypeDeclaration;

parameterTypeDeclarationList
    : parameterTypeDeclaration (COMMA parameterTypeDeclaration)*
    ;

parameterColumnNameTypeList
    : parameterColumnNameType (COMMA parameterColumnNameType)*
    ;

parameterColumnNameType
    : colName=identifier t = anyType commentClause?
    ;

varSizeParam
    :
    STAR any=anyType
    ;

// assign statement definition
assignStatement
    :
    var=variableName
    decl=typeDeclaration?
    (ASSIGN (cache=KW_CACHE (KW_WITH KW_CACHEPROPERTIES cacheprops=tableProperties)? KW_ON)?
        (
        exp=expression
        |
        queryExp=query
        )
    )?
    ;

preSelectClauses
    :
    w = whereClause
    g = groupByClause?
    h = havingClause?
    q = qualifyClause?
    win = windowClause?
    |
    g = groupByClause
    h = havingClause?
    q = qualifyClause?
    win = windowClause?
    |
    h = havingClause
    q = qualifyClause?
    win = windowClause?
    |
    q = qualifyClause
    win = windowClause?
    |
    win = windowClause
    ;


queryOrganization
    :
    o=orderByClause?
    c=clusterByClause?
    d=distributeByClause?
    sort=sortByClause?
    zorder=zorderByClause?
    l=limitClause?
    ;

selectRest
    :
    (f=fromClause | lv=lateralView)? pre=preSelectClauses? post = queryOrganization
    ;

multiInsertFromRest
    :
    s = selectClause lv = lateralView? pre = preSelectClauses? post = queryOrganization
    |
    lv = lateralView? pre = preSelectClauses s = selectClause post = queryOrganization
    ;

fromRest
    :
    s = selectClause pre = preSelectClauses? post = queryOrganization
    |
    pre = preSelectClauses s = selectClause post = queryOrganization
    ;

simpleQueryExpression
    :
    s = selectQueryExpression
    |
    f = fromQueryExpression
    |
    subQueryExp = subQueryExpression
    ;

selectQueryExpression
    :
    s = selectClause rest = selectRest
    ;

fromQueryExpression
    :
    f = fromClause rest = fromRest
    ;

queryStatement
    :
    query
    ;

query
    :
    (w = withClause)?
    exp = queryWithoutCTE
    queryOrganization
    ;

queryWithoutCTE
    :
    left=queryWithoutCTE op=setOperator right=queryWithoutCTE
    | simpleQueryExpression
    ;

dmlStatement
    :
    withClause? dmlStatementWithoutCTE
    ;

dmlStatementWithoutCTE
    :
    insertStatement
    | fromClause multiInsertBranch+
    | deleteStatement
    | updateStatement
    | mergeStatement
    ;

multiInsertSetOperationFactor
    :
    f=fromRest
    |
    LPAREN m=multiInsertSelect RPAREN
    ;

multiInsertSelect
    :
    ( LPAREN m=multiInsertSelect RPAREN rhs+=multiInsertSetRHS
    | f=multiInsertFromRest
    ) (rhs+=multiInsertSetRHS)*
    ;

multiInsertSetRHS
    :
    op=setOperator operand=multiInsertSetOperationFactor
    ;

multiInsertBranch
    :
    i = insertClause
    m = multiInsertSelect
    ;

insertStatement
    :
    i=insertClause (v=valuesClause|q=query)
    ;

subQueryExpression
    :
    LPAREN subqueryJoinHint? q=query RPAREN
    ;

limitClause
    : KW_LIMIT offset=mathExpression COMMA exp=mathExpression
    | KW_LIMIT exp=mathExpression (KW_OFFSET offset=mathExpression)?
    ;

// add Variable to from source
atomFromSource
    :
    pt=partitionedTableFunction
    | t=tableSource
    | sq=subQuerySource
    | vt=virtualTableSource
    | tv=tableVariableSource
    | un=unnest
    | tf=tableFunctionSource
    | es=externalTableSchemaless
    | LPAREN js=joinSource RPAREN
    ;

unnest
    :
    KW_UNNEST LPAREN exp=expression RPAREN alias=tableAliasWithCols?
    ;

fromSourceSuffix
    : lateralView
    | unpivotClause
    | pivotClause
    ;

tableVariableSource
    : var = variableName alias=tableAliasWithCols?
    ;

tableFunctionSource
    : fun = function alias=tableAliasWithCols?
    ;

externalTableSchemaless
    :
    KW_LOCATION location=stringLiteral
    (props=tableProperties)?
    (KW_AS? (alias=identifierWithoutSql11 | l=doubleQuoteStringLiteral))?
    ;

createMachineLearningModelStatment
    :
    KW_CREATE KW_MODEL model=tableName
    KW_WITH KW_PROPERTIES LPAREN tablePropertiesList RPAREN
    KW_AS dataSource=query
    ;

createSqlMlModelStatment
    : KW_CREATE KW_MODEL ine=ifNotExists? model=tableName KW_WITH KW_VERSION version=identifier
    (KW_INPUT LPAREN columns=columnNameTypeConstraintList RPAREN)?
    (KW_OPTIONS modelOptions=optionItemWithEqualList)?
    (KW_INFERENCEPARAMETERS inferenceParameters=optionItemWithEqualList)?
    comment=commentClause?
    lifecycle=tableLifecycle?
    ;

alterSqlMlModelStatement
    : KW_ALTER KW_MODEL ifExists? model=tableName
    ((KW_ADD KW_VERSION ifNotExists? version=identifier
      (KW_INPUT LPAREN columns=columnNameTypeConstraintList RPAREN)?
      (KW_WITH KW_OPTIONS modelOptions=optionItemWithEqualList
        (KW_INFERENCEPARAMETERS inferenceParameters=optionItemWithEqualList)?)? comment=commentClause?)
    | (KW_DROP KW_VERSION version=identifier)
    | (KW_SET KW_DEFAULT_VERSION version=identifier))
    ;

dropSqlMlModelStatement
    : KW_DROP KW_MODEL ifExists? model=tableName
    ;

showSqlMlModelVersionsStatement
    : KW_SHOW KW_VERSIONS KW_FOR KW_MODEL model=tableName (KW_LIKE showStmtIdentifier)?
    ;

optionItemWithEqualList
    : LPAREN items+=optionItemWithEqual (COMMA items+=optionItemWithEqual)* RPAREN
    ;

optionItemWithEqual
    : identifier EQUAL stringLiteral
    ;

variableName
    : Variable
    ;

// add Varaiable and native type instialization to atom expressions
// priority of operator is defined in https://help.aliyun.com/zh/maxcompute/user-guide/operator
atomExpression
    :
    KW_NULL
    | con = constant      // literals
    | exists = existsExpression
    | castExp = castExpression
    | caseExp = caseExpression
    | whenExp = whenExpression
    | LPAREN exp = expression RPAREN
    | var = variableRef      // @xxx
    | varFun = variableCall      // @xxx
    | structExp = structExpression
    | fun = function
    | col = tableOrColumnRef    // maybe dbname, tablename, colname, funcName, className, package...
    | newExp = newExpression    // new xxxclass()
    | subQuery = scalarSubQueryExpression
    ;

structExpression
    :
    KW_STRUCT LPAREN arg+=structElementExpression (COMMA arg+=structElementExpression)* RPAREN
    ;

structElementExpression
    :
    namedExpression | wildcardExpression
    ;

wildcardExpression
    :
    (STAR | table=identifier DOT STAR)
    (KW_EXCEPT LPAREN exceptColNames+=identifier (COMMA exceptColNames+=identifier)* RPAREN)?
    ;

namedExpression
    :
    expression KW_AS? alias=identifier?
    ;

variableRef
    :
    variableName
    ;

variableCall
    :
    variableName LPAREN expressionList? RPAREN
    ;

funNameRef
    :
    db=identifier COLON fn=identifier
    ;

lambdaExpression
    :
    KW_FUNCTION LPAREN (p+=lambdaParameter (COMMA p+=lambdaParameter)*)? RPAREN
    (KW_RETURNS retvar=variableName retType=parameterTypeDeclaration)?
    KW_AS (c=compoundStatement | e=expression | q=query)
    | (p+=lambdaParameter | LPAREN (p+=lambdaParameter (COMMA p+=lambdaParameter)*)? RPAREN) LAMBDA_IMPLEMENT e=expression
    ;

lambdaParameter
    :
    (v=variableName| i=identifier) p=parameterTypeDeclaration?
    ;

tableOrColumnRef
    :
    identifier
    ;

// support function from other languages
newExpression
    : KW_NEW classNameWithPackage
    ( LPAREN args=expressionList? RPAREN
    | (arr+=LSQUARE RSQUARE)+ init=LCURLY elem=expressionList? RCURLY
    | (arr+=LSQUARE len+=expression RSQUARE)+ (arr+=LSQUARE RSQUARE)*
    );

existsExpression:
    KW_EXISTS q=subQueryExpression
    ;

scalarSubQueryExpression:
    subQuery = subQueryExpression
    ;

classNameWithPackage:
    (packages += identifier DOT)* className=identifier (LESSTHAN types=classNameList GREATERTHAN)?;

classNameOrArrayDecl:
    classNameWithPackage (arr+=LSQUARE RSQUARE)*;

classNameList:
    cn+=classNameOrArrayDecl (COMMA cn+=classNameOrArrayDecl)*;

// We have add some keywords. But those keywords are identifyable with Identifier during parsing
// So we can use them as identifier without quote to keep compatible.
odpsqlNonReserved:
    KW_RETURNS | KW_BEGIN | KW_LOOP | KW_NEW | KW_REMOVE | KW_GRANTS | KW_ACL | KW_TYPE | KW_LIST | KW_USERS | KW_WHOAMI
    | KW_TRUSTEDPROJECTS | KW_TRUSTEDPROJECT | KW_SECURITYCONFIGURATION | KW_PACKAGE | KW_PACKAGES | KW_INSTALL | KW_UNINSTALL
    | KW_PRIVILEGES | KW_PROJECT | KW_PROJECTS | KW_LABEL | KW_ALLOW | KW_DISALLOW | KW_P | KW_JOB | KW_JOBS | KW_ACCOUNTPROVIDERS
    | KW_RESOURCES | KW_FLAGS | KW_STATISTIC_LIST | KW_STATISTIC | KW_COUNT | KW_COST | KW_GET | KW_PUT | KW_POLICY | KW_PROJECTPROTECTION
    | KW_EXCEPTION | KW_CLEAR | KW_EXPIRED | KW_EXP | KW_ACCOUNTPROVIDER | KW_SUPER | KW_VOLUMEFILE | KW_VOLUMEARCHIVE | KW_OFFLINEMODEL | KW_PY
    | KW_RESOURCE | KW_STATUS | KW_KILL | KW_HUBLIFECYCLE | KW_SHARDS | KW_SETPROJECT | KW_MERGE | KW_SMALLFILES | KW_PARTITIONPROPERTIES
    | KW_EXSTORE | KW_CHANGELOGS | KW_REDO | KW_HUBTABLE | KW_CHANGEOWNER | KW_RECYCLEBIN | KW_PRIVILEGEPROPERTIES | relaxedKeywords | KW_NULL_VALUE
    | KW_DISTINCT_VALUE | KW_TABLE_COUNT | KW_COLUMN_SUM | KW_COLUMN_MAX | KW_COLUMN_MIN | KW_EXPRESSION_CONDITION | KW_GROUPS
    | KW_CACHE | ByteLengthLiteral | KW_VARIABLES | KW_EXCEPT | KW_SELECTIVITY | KW_LOCALTIMESTAMP | KW_EXTRACT | KW_SUBSTRING | KW_LAST
    | KW_NULLS | KW_DEFAULT | KW_ANY | KW_OFFSET | KW_CLONE | KW_CONSTRAINT
    | KW_UNIQUEJOIN | KW_TABLESAMPLE | KW_MACRO | KW_FILE | KW_DYNAMICFILTER | KW_DATABASE | KW_UDFPROPERTIES
    | KW_UNBOUNDED | KW_PRECEDING | KW_FOLLOWING | KW_MORE | KW_OVER | KW_PARTIALSCAN | KW_EXCHANGE| KW_CONF
    | KW_LIFECYCLE | KW_CACHEPROPERTIES
    | KW_ACCESS | KW_PERMISSIVE | KW_RESTRICTIVE
    | KW_TENANT
    | KW_DISTMAPJOIN
    | KW_CATALOG
    | KW_BLOOMFILTER
    | KW_BITMAP
    | KW_EVERY
    | KW_AT
    | KW_REFRESH
    | KW_SECURE
    | KW_SPLITSIZE
    | KW_UNIONTABLE
    | KW_HISTOGRAM
    | KW_TUNE
    | KW_START
    | KW_GENERATED | KW_ALWAYS
    ;
relaxedKeywords:
     KW_INTERVAL | KW_CONF | KW_EXCHANGE | KW_LESS | KW_MORE;

allIdentifiers
    :
    id = Identifier
    | nonReservedId = nonReserved
    | sq11KeywordAsId = sql11ReservedKeywordsUsedAsIdentifier
    | odpsNonReservedId = odpsqlNonReserved
    | reservedId = reserved
    ;

identifier
    :
    id = Identifier
    | nonReservedId = nonReserved
    | sq11KeywordAsId = sql11ReservedKeywordsUsedAsIdentifier
    | odpsNonReservedId = odpsqlNonReserved
    ;

aliasIdentifier
    :
    identifier
    | doubleQuoteStringLiteral
    ;

identifierWithoutSql11
    :
    id = Identifier
    | nonReservedId = nonReserved
    | odpsNonReservedId = odpsqlNonReserved
    ;

alterTableChangeOwner
    : KW_CHANGEOWNER KW_TO stringLiteral
    ;

alterViewChangeOwner
    : KW_CHANGEOWNER KW_TO stringLiteral
    ;

alterTableEnableHubTable
    : KW_ENABLE KW_HUBTABLE KW_WITH shardNum=Number KW_SHARDS KW_HUBLIFECYCLE hubLifeCycle=Number
    ;

tableLifecycle
    :
    KW_LIFECYCLE lifecycleDays=Number
    ;

setStatement
    : KW_SET KW_PROJECTPROTECTION EQUAL (KW_TRUE | KW_FALSE) (KW_WITH KW_EXCEPTION filePath)?
    | (KW_SET|unset=KW_UNSET) KW_LABEL (cat=identifier|min=MINUS? num=label) KW_TO (p=principalName | KW_TABLE t=tableName (LPAREN cols=columnNameList RPAREN)?)
    | KW_SET key=anythingButEqualOrSemi EQUAL (skewInfo=skewInfoVal|val+=anythingButSemi*?)
    ;

anythingButEqualOrSemi:
    (~(EQUAL|SEMICOLON|WS))+;

anythingButSemi:
    ~SEMICOLON;

setProjectStatement :
    KW_SETPROJECT key=anythingButEqualOrSemi EQUAL expression;

label:
    Number;

skewInfoVal
    :
    skewSource=tableName
    COLON
    LPAREN key+=allIdentifiers (COMMA key+=allIdentifiers)* RPAREN
    LSQUARE LPAREN expressionList RPAREN (COMMA? LPAREN expressionList RPAREN)* RSQUARE
    ;

memberAccessOperator
    :
    DOT field=identifier
    ;

methodAccessOperator
    :
    DOT (LESSTHAN types=classNameList GREATERTHAN)? field=identifier LPAREN arguments=expressionList? RPAREN
    ;

isNullOperator:
    KW_IS not=KW_NOT? KW_NULL
    ;

// priority of operator is defined in https://help.aliyun.com/zh/maxcompute/user-guide/operator
mathExpression
    :
    // priority 2
    lhs=mathExpression op=BITWISEXOR rhs=mathExpression
    // priority 3
    |lhs=mathExpression (op=STAR | op=DIVIDE | op=MOD | op=KW_DIV) rhs=mathExpression
    // priority 4
    |lhs=mathExpression (op=PLUS | op=MINUS) rhs=mathExpression
    // priority 5
    |lhs=mathExpression op=CONCATENATE rhs=mathExpression
    // priority 6
    |lhs=mathExpression op=AMPERSAND rhs=mathExpression
    // priority 7
    |lhs=mathExpression op=BITWISEOR rhs=mathExpression
    |exp=unarySuffixExpression
    ;

// priority 1, priority of operator is defined in https://help.aliyun.com/zh/maxcompute/user-guide/operator
unarySuffixExpression:
    operand=unarySuffixExpression op=isNullOperator
    |exp=unaryPrefixExpression
    ;

// second highest priority, not listed in document, priority of operator is defined in https://help.aliyun.com/zh/maxcompute/user-guide/operator
unaryPrefixExpression:
    (op=PLUS | op=MINUS | op=TILDE) operand=unaryPrefixExpression
    | exp=fieldExpression
    ;

// highest priority, not count in document, priority of operator is defined in https://help.aliyun.com/zh/maxcompute/user-guide/operator
fieldExpression:
    exp=atomExpression
    |
    operand=fieldExpression (member=memberAccessOperator|ls=LSQUARE index=expression RSQUARE|method=methodAccessOperator)
    ;

// priority of operator is defined in https://help.aliyun.com/zh/maxcompute/user-guide/operator
logicalExpression:
    // priority 12
    lhs=logicalExpression op=KW_AND rhs=logicalExpression
    // priority 13
    |lhs=logicalExpression op=KW_OR rhs=logicalExpression
    |exp=notExpression
    ;

// priority 11, priority of operator is defined in https://help.aliyun.com/zh/maxcompute/user-guide/operator
notExpression:
    op=KW_NOT operand=notExpression
    |exp=rangeExpression
    ;

inOperator:
    not=KW_NOT? KW_IN (exp=expressionsInParenthese | subQuery=subQueryExpression)
    ;

betweenOperator:
    not=KW_NOT? KW_BETWEEN (min=mathExpression) KW_AND (max=mathExpression)
    ;

// priority 10, priority of operator is defined in https://help.aliyun.com/zh/maxcompute/user-guide/operator
rangeExpression:
    lhs=rangeExpression (in=inOperator | between=betweenOperator)
    |exp=equalExpression
    ;

// priority 9, priority of operator is defined in https://help.aliyun.com/zh/maxcompute/user-guide/operator
equalExpression:
    lhs=equalExpression
        (
            not=KW_NOT? (op=KW_LIKE|op=KW_RLIKE|op=KW_REGEXP)
            |op=EQUAL
            |op=EQUAL_NS
            |KW_IS not=KW_NOT? op=KW_DISTINCT KW_FROM
            |op=NOTEQUAL
            |op=LESSTHANOREQUALTO
            |op=LESSTHAN
            |op=GREATERTHANOREQUALTO
            |op=GREATERTHAN
        )
    rhs=equalExpression
    |exp=likeExpression
    ;

// priority 8, priority of operator is defined in https://help.aliyun.com/zh/maxcompute/user-guide/operator
likeExpression:
    value=likeExpression not=KW_NOT? KW_LIKE (KW_ANY | KW_ALL) LPAREN valueList+=expression? (COMMA valueList+=expression)* RPAREN
    |exp=mathExpression
    |expIn=mathExpressionListInParentheses
    ;

mathExpressionListInParentheses
    : LPAREN mathExpressionList RPAREN
    ;

mathExpressionList
    : mathExpression (COMMA mathExpression)*
    ;

expression:
    logicalExpression
    ;

statisticStatement
    : addRemoveStatisticStatement
    | showStatisticStatement
    | showStatisticListStatement
    | analyzeStatement
    | countTableStatement;

// we cannot use a uniform way to parse the statement.
// for example, the statment " add statistic tableName statisticName c1 - 1 > c2;" could have two explaination:
//  1. when statisticName == 'COLUMN_SUM', it means "select sum(c1) from tableName where -1 > c2;"
//  2. when statisticName == 'EXPRESSION_CONDITION', it means select count(*) from tableName where c1-1 > c2;
addRemoveStatisticStatement:
    (KW_ADD|KW_REMOVE) KW_STATISTIC tab=tableName info=statisticInfo;

statisticInfo:
    sName=KW_NULL_VALUE identifier
    | sName=KW_DISTINCT_VALUE identifier (COMMA identifier)*
    | sName=KW_TABLE_COUNT
    | sName=KW_COLUMN_SUM identifier expression?
    | sName=KW_COLUMN_MAX identifier expression?
    | sName=KW_COLUMN_MIN identifier expression?
    | sName=KW_EXPRESSION_CONDITION expression?;

showStatisticStatement:
    KW_SHOW KW_STATISTIC tableName partitionSpec? (KW_COLUMNS (LPAREN columnNameList RPAREN)?)?;

showStatisticListStatement:
    KW_SHOW KW_STATISTIC_LIST tableName?;

countTableStatement:
    KW_COUNT tableName partitionSpec?;

/////////////////////////////
// Instance management statements
//////////////////////////////

instanceManagement:
    instanceStatus
    | killInstance;

instanceStatus:
    KW_STATUS instanceId;

killInstance:
    KW_KILL instanceId;

instanceId:
    Identifier;

/////////////////////////////
// Resource Management statements
//////////////////////////////

resourceManagement:
    addResource
    | dropResource
    | getResource
    | dropOfflineModel;

addResource:
    KW_ADD (KW_FILE | KW_ARCHIVE | KW_VOLUMEFILE | KW_VOLUMEARCHIVE) filePath (KW_AS identifier)? (KW_COMMENT stringLiteral)? options?
    | KW_ADD KW_TABLE tableName partitionSpec? (KW_AS identifier)? (KW_COMMENT stringLiteral)? options?
    | KW_ADD (KW_PY | KW_JAR) filePath (KW_COMMENT stringLiteral)? options?;

dropResource:
    KW_DROP KW_RESOURCE resourceId;

resourceId:
    identifier (DOT identifier)*;

dropOfflineModel:
    KW_DROP KW_OFFLINEMODEL ifExists? identifier;

getResource:
    KW_GET KW_RESOURCE (identifier COLON)? resourceId filePath;

options:
    MINUS identifier;

/////////////////////////////
// Authorization statements
//////////////////////////////

authorizationStatement:
    // not supported hive grammar: odps do not have current role
    setRole
    | showCurrentRole

    // odps only
    | addUserStatement
    | removeUserStatement
    | addGroupStatement
    | removeGroupStatement
    | addAccountProvider
    | removeAccountProvider
    | listUsers
    | listGroups
    | whoami
    | showAcl
//  | describeRole    // merge with describe statemenet
    | listRoles
    | listTrustedProjects
    | addTrustedProject
    | removeTrustedProject
    | showSecurityConfiguration
    | showPackages
    | showItems
//  | describePackage // move to describe statement
    | installPackage
    | uninstallPackage
    | createPackage
    | deletePackage
    | addToPackage
    | removeFromPackage
    | allowPackage
    | disallowPackage
    | putPolicy
    | getPolicy
    | clearExpiredGrants
    | grantLabel
    | revokeLabel
    | showLabel
    | grantSuperPrivilege
    | revokeSuperPrivilege
    | purgePrivileges

    // Odps privilege commands
    | createRoleStatement
    | dropRoleStatement
    | addRoleToProject
    | removeRoleFromProject
    | grantRole
    | revokeRole
    | grantPrivileges
    | revokePrivileges
    | showGrants        // odps: show grants
    | showRoleGrants    // odps show grants
    | showRoles     // odps: list roles
    | showRolePrincipals    // odps: describe role
    | createUserGroupStatement
    | alterUserGroupStatement
    | dropUserGroupStatement
    | listUserGroups

    // row access policy
    | createRowAccessPolicyStatement
    | dropRowAccessPolicyStatement
    | descRowAccessPolicyStatement
    | listRowAccessPolicyStatement
;

listUsers:
    KW_LIST KW_TENANT? KW_USERS;

listGroups:
    KW_LIST KW_TENANT? KW_GROUPS (KW_FOR KW_USER user)?;

addUserStatement:
    KW_ADD KW_TENANT? KW_USER name=user comment=userRoleComments?;

addGroupStatement:
    KW_ADD KW_TENANT? KW_GROUP name=principalIdentifier comment=userRoleComments?;

removeUserStatement:
    KW_REMOVE KW_TENANT? KW_USER user;

removeGroupStatement:
    KW_REMOVE KW_TENANT? KW_GROUP principalIdentifier;

addAccountProvider:
    KW_ADD KW_ACCOUNTPROVIDER accountProvider;

removeAccountProvider:
    KW_REMOVE KW_ACCOUNTPROVIDER accountProvider;

showAcl:
    KW_SHOW KW_ACL KW_FOR privilegeObjectName (KW_ON KW_TYPE privilegeObjectType)?;

//describeRole:
//    (KW_DESCRIBE|KW_DESC) KW_ROLE roleName;

listRoles:
    KW_LIST KW_TENANT? KW_ROLES;

whoami:
    KW_WHOAMI;

listTrustedProjects:
    KW_LIST KW_TRUSTEDPROJECTS;

addTrustedProject:
    KW_ADD KW_TRUSTEDPROJECT projectName;

removeTrustedProject:
    KW_REMOVE KW_TRUSTEDPROJECT projectName;

showSecurityConfiguration:
    KW_SHOW KW_SECURITYCONFIGURATION;

showPackages:
    KW_SHOW KW_PACKAGES (KW_WITH privilegeObject)? privilegeProperties?;

showItems:
    KW_SHOW KW_PACKAGE (pkg=packageName | KW_ITEMS (KW_FROM prj=projectName)? (KW_ON KW_TYPE tp=privilegeObjectType)?) privilegeProperties?;

installPackage:
    KW_INSTALL KW_PACKAGE pkg=packageNameWithProject privilegeProperties?;

uninstallPackage:
    KW_UNINSTALL KW_PACKAGE pkg=packageNameWithProject privilegeProperties?;

createPackage:
    KW_CREATE KW_PACKAGE packageName;

deletePackage:
    (KW_DELETE|KW_DROP) KW_PACKAGE packageName;

addToPackage:
    KW_ADD privilegeObject KW_TO KW_PACKAGE packageName (KW_WITH KW_PRIVILEGES privilege (COMMA privilege)*)? props=privilegeProperties?;

removeFromPackage:
    KW_REMOVE privilegeObject KW_FROM KW_PACKAGE packageName;

allowPackage:
    KW_ALLOW KW_PROJECT (pj=projectName|st=STAR) KW_TO KW_INSTALL KW_PACKAGE packageName
    (KW_WITH privilege (COMMA privilege)*)? (KW_USING KW_LABEL label)? (KW_EXP Number)?;

disallowPackage:
    KW_DISALLOW KW_PROJECT (pj=projectName|st=STAR) KW_TO KW_INSTALL KW_PACKAGE packageName;

putPolicy:
    KW_PUT KW_POLICY filePath (KW_ON KW_TENANT? KW_ROLE roleName)?;

getPolicy:
    KW_GET KW_POLICY (KW_ON KW_TENANT? KW_ROLE roleName)?;

clearExpiredGrants:
    KW_CLEAR KW_EXPIRED KW_GRANTS;

grantLabel:
    KW_GRANT KW_LABEL label KW_ON KW_TABLE tabName=tableName (LPAREN columnNameList RPAREN)? KW_TO (p=principalName KW_WITH KW_EXP Number | p=principalName) props=privilegeProperties?;

revokeLabel:
    KW_REVOKE KW_LABEL KW_ON KW_TABLE tabName=tableName (LPAREN columnNameList RPAREN)? KW_FROM p=principalName props=privilegeProperties?;

showLabel:
    KW_SHOW KW_LABEL label? KW_GRANTS
        ( KW_ON KW_TABLE tabName=tableName (KW_FOR p=principalName)?
        | KW_FOR p=principalName (KW_ON KW_ALL KW_OBJECTS KW_IN KW_SCHEMA s=identifier)?
        | forTable=KW_FOR KW_TABLE tabName=tableName)? props=privilegeProperties?;

grantSuperPrivilege:
    KW_GRANT KW_SUPER privilege (COMMA privilege)* KW_TO principalName;

revokeSuperPrivilege:
    KW_REVOKE KW_SUPER privilege (COMMA privilege)* KW_FROM principalName;

createRoleStatement
    : KW_CREATE KW_TENANT? KW_ROLE role=roleName comment=userRoleComments? props=privilegeProperties?;

dropRoleStatement
    : (KW_DELETE|KW_DROP) KW_TENANT? KW_ROLE roleName;

addRoleToProject
    : KW_ADD KW_TENANT KW_ROLE roleName (KW_TO KW_PROJECT projectName)?;

removeRoleFromProject
    : KW_REMOVE KW_TENANT KW_ROLE roleName (KW_FROM KW_PROJECT projectName)?;

grantRole
    : KW_GRANT (KW_TENANT? KW_ROLE)? roleName (COMMA roleName)* KW_TO principalSpecification withAdminOption?;

revokeRole
    : KW_REVOKE adminOptionFor? (KW_TENANT? KW_ROLE)? roleName (COMMA roleName)* KW_FROM principalSpecification;

grantPrivileges
    : KW_GRANT privilege (COMMA privilege)* KW_ON privilegeObject
    KW_TO principalSpecification withGrantOption? privilegeProperties?;

privilegeProperties:
    KW_PRIVILEGEPROPERTIES LPAREN keyValueProperty (COMMA keyValueProperty)* RPAREN;

privilegePropertieKeys:
    KW_PRIVILEGEPROPERTIES LPAREN keyPrivProperty (COMMA keyPrivProperty)* RPAREN;

revokePrivileges
    : KW_REVOKE grantOptionFor? privilege (COMMA privilege)* KW_ON privilegeObject KW_FROM principalSpecification privilegeProperties?;

purgePrivileges
    : KW_PURGE KW_PRIVILEGES KW_FROM principalSpecification
    ;

showGrants:
    KW_SHOW KW_TENANT? (KW_GRANT|KW_GRANTS) KW_FOR? principalName?
    (KW_ON KW_ALL KW_OBJECTS KW_IN KW_SCHEMA s=identifier | KW_ON KW_TYPE privilegeObjectType | KW_ON privilegeObject)?
    privilegeProperties?;

showRoleGrants:
    KW_SHOW KW_ROLE KW_GRANT KW_FOR? principalName;

showRoles:
    KW_SHOW KW_ROLES;

showRolePrincipals:
    KW_SHOW KW_PRINCIPALS KW_FOR? KW_TENANT? KW_ROLE? roleName;

user:
    (allIdentifiers DOLLAR)?    // account provider
    (allIdentifiers | Number) ((DOT | MINUS | UNDERLINE) (allIdentifiers | Number))*  // main body
    ((Variable |  AT (allIdentifiers | Number)) ((DOT | MINUS | UNDERLINE) (allIdentifiers | Number))*)?
    (COLON (identifier|Number|DOT|MINUS|AT)+)?;

rowAccessPolicyUserList:
    user (COMMA user)*;

rowAccessPolicyRoleList:
    roleName (COMMA roleName)*;

createRowAccessPolicyStatement:
    KW_CREATE orReplace? KW_ROW KW_ACCESS KW_POLICY ifNotExists? policyName
    KW_ON tableName
    KW_TO (dflt=KW_DEFAULT | KW_USER LPAREN userlist=rowAccessPolicyUserList RPAREN | KW_ROLE LPAREN rolelist=rowAccessPolicyRoleList RPAREN)
    KW_FILTER KW_USING LPAREN expression RPAREN
    (KW_AS (KW_PERMISSIVE | KW_RESTRICTIVE))?;

dropRowAccessPolicyStatement:
    KW_DROP KW_ALL? KW_ROW KW_ACCESS KW_POLICY policyName? KW_ON tableName;

descRowAccessPolicyStatement:
    KW_DESC KW_ROW KW_ACCESS KW_POLICY policyName KW_ON tableName;

listRowAccessPolicyStatement:
    KW_LIST KW_ROW KW_ACCESS KW_POLICY KW_ON tableName (KW_TO (KW_USER user |  KW_ROLE roleName))?;

createDataMaskingPolicyStatement:
    KW_CREATE KW_DATA KW_MASKING KW_POLICY ifNotExists? policyName
    KW_TO (dflt=KW_DEFAULT | KW_USER LPAREN userlist=rowAccessPolicyUserList RPAREN | KW_ROLE LPAREN rolelist=rowAccessPolicyRoleList RPAREN)
    KW_USING
    (
        policyfunction=function
        |
        policyItem=identifier
        |
        KW_USERDEFINE
        param=functionParameters (KW_RETURNS retvar=variableName retType=typeDeclaration)?
        KW_AS (funBody=compoundStatement|exp=expression)
    );

applyDataMaskingPolicyStatement:
    KW_APPLY KW_DATA KW_MASKING KW_POLICY policyName?
    (KW_BIND KW_TO | KW_UNBIND KW_ALL? KW_FROM) KW_TABLE tableName (KW_COLUMN colName=identifier)?;

dropDataMaskingPolicyStatement:
    KW_DROP KW_DATA KW_MASKING KW_POLICY policyName;

descDataMaskingPolicyStatement:
    KW_DESC KW_EXTENDED? KW_DATA KW_MASKING KW_POLICY policyName;

listDataMaskingPolicyStatement:
    KW_LIST KW_DATA KW_MASKING KW_POLICY
    (KW_ON tableName (KW_TO KW_COLUMN colName=identifier)?
    | KW_TO (KW_USER user| KW_ROLE roleName))?;

userRoleComments:
    (KW_COMMENT | options) stringLiteral;

createUserGroupStatement
    : KW_CREATE KW_TENANT? KW_USERGROUP userGroup=userGroupName comment=userGroupComments? props=privilegeProperties?;

alterUserGroupStatement
    : KW_ALTER KW_TENANT? KW_USERGROUP userGroup=userGroupName (KW_ADD | KW_DROP) KW_USER user props=privilegeProperties?;

dropUserGroupStatement
    : (KW_DELETE|KW_DROP) KW_TENANT? KW_USERGROUP userGroupName;

listUserGroups:
    KW_LIST KW_TENANT? KW_USERGROUPS;

userGroupName:
    identifier;

userGroupComments:
    KW_COMMENT stringLiteral;

accountProvider:
    identifier;

projectName:
    identifier;

privilegeObjectName:
    ((STAR? identifier) | STAR) ((Number|DOT|MINUS|AT|SHARP|DOLLAR|STAR) identifier?)* STAR? | STAR;

privilegeObjectType:
    identifier | KW_FUNCTION | KW_DATABASE;

roleName:
    identifier;

packageName:
    identifier;

policyName:
    identifier;

packageNameWithProject:
    (proj=identifier DOT)? name=identifier;

principalSpecification:
    principalName (COMMA principalName)*;

principalName:
    t=KW_USER? (principalIdentifier | user)
    | KW_TENANT? t=KW_USER (principalIdentifier | user)
    | KW_TENANT? t=KW_GROUP principalIdentifier
    | KW_TENANT? t=KW_ROLE identifier
    | KW_TENANT? t=KW_USERGROUP identifier
    ;

principalIdentifier
    : id = identifier
    ;

privilege:
    privilegeType (LPAREN cols=columnNameList RPAREN)?;

privilegeType:
    identifier | KW_SELECT;

privilegeObjectFullName:
    db=identifier DOT (n=identifier | STAR)
    |
    (n=identifier | STAR)
    |
    db=identifier DOT sch=identifier DOT (n=identifier | STAR)
    ;
// database or table type. Type is optional, default type is table
privilegeObject:
    KW_TABLE? tableName (LPAREN cols=columnNameList RPAREN)? parts=partitionSpec?
    | privilegeObjectType (n=privilegeObjectFullName | privilegeObjectName | stringLiteral)
    ;

filePath:
    stringLiteral | DIVIDE? identifier ((COLON | DIVIDE | ESCAPE | DOT | MINUS)+ identifier)*;

// root rule
policyCondition
    : lhs=policyCondition and=KW_AND rhs=policyConditionOp
    | rhs=policyConditionOp;

policyConditionOp
    : key=policyKey (eq=EQUAL | ne=NOTEQUAL | not=KW_NOT? like=KW_LIKE | lt=LESSTHAN | le=LESSTHANOREQUALTO | gt=GREATERTHAN | ge=GREATERTHANOREQUALTO | not=KW_NOT? in=KW_IN)
        (val+=policyValue | LPAREN val+=policyValue (COMMA val+=policyValue)* RPAREN)
    | op=identifier LPAREN key=policyKey (COMMA val+=policyValue)+ RPAREN;

policyKey
    : lower=identifier LPAREN (prefix=identifier COLON)? key=identifier RPAREN
    | (prefix=identifier COLON)? key=identifier;

policyValue: str=stringLiteral | neg=MINUS? num=Number;

/*********** not supported by odps *********/

showCurrentRole:
    KW_SHOW KW_CURRENT KW_ROLES;

setRole:
    KW_SET KW_ROLE (KW_ALL | identifier);

adminOptionFor:
    KW_ADMIN KW_OPTION KW_FOR;

withAdminOption:
    KW_WITH KW_ADMIN KW_OPTION;

withGrantOption:
    KW_WITH KW_GRANT KW_OPTION;

grantOptionFor:
    KW_GRANT KW_OPTION KW_FOR;

/******* end not supported *********/

///////////////////////////////////
// end authorization statements
///////////////////////////////////

// ----------------------- original hive parser rules start here ---------------------

/* overwritten by our extension -- commenting out the original here
explainStatement
	: KW_EXPLAIN (
	    explainOption* execStatement
        |
        KW_REWRITE queryStatementExpression[true])
	;
*/

explainOption:
        KW_EXTENDED
        |
        KW_FORMATTED
        |
        KW_DEPENDENCY
        |
        KW_LOGICAL (cmds+=simpleStringLiteral (COMMA cmds+=simpleStringLiteral)*)?
        |
        KW_AUTHORIZATION
        |
        KW_TUNE
        |
        KW_OUTPUT
        ;

loadStatement
    : KW_LOAD KW_DATA (islocal=KW_LOCAL)? KW_INPATH (path=stringLiteral) (isoverwrite=KW_OVERWRITE)? KW_INTO KW_TABLE (tab=tableOrPartition)
    |
    KW_LOAD (KW_INTO | KW_OVERWRITE) KW_TABLE (tab=tableOrPartition)
    KW_FROM dd = dataFormatDesc (KW_PROPERTIES p=tableProperties)?
    |
    KW_UNLOAD KW_FROM ((LPAREN q=query RPAREN) | t=tableOrPartition)
    KW_INTO dd = dataFormatDesc (KW_PROPERTIES p=tableProperties)?
    ;

replicationClause
    : KW_FOR (isMetadataOnly=KW_METADATA)? KW_REPLICATION LPAREN (replId=simpleStringLiteral) RPAREN
    ;

exportStatement
    : KW_EXPORT
      KW_TABLE (tab=tableOrPartition)
      (to=KW_TO (path=simpleStringLiteral)
      replicationClause?)?
    ;


importStatement
       : KW_IMPORT
         ((ext=KW_EXTERNAL)? KW_TABLE (tab=tableOrPartition))?
         KW_FROM (path=simpleStringLiteral)
         tableLocation?
    ;

readStatement:
    KW_READ tableName (LPAREN columnNameList RPAREN)? partitionSpec? Number?;

undoStatement:
    KW_UNDO KW_TABLE tableName partitionSpec? KW_TO Number;

redoStatement:
    KW_REDO KW_TABLE tableName partitionSpec? KW_TO Number;

purgeStatement:
    KW_PURGE KW_TABLE t = tableName (hours=Number)? KW_SAFE?
    |
    KW_PURGE KW_ALL (hours=Number)? KW_SAFE?
    |
    KW_PURGE KW_TEMPORARY KW_OUTPUT KW_ALL
    |
    KW_PURGE KW_TEMPORARY KW_OUTPUT i = instanceId
    ;

// useless grammar
dropTableVairableStatement:
    KW_DROP KW_TABLE KW_VARIABLES variableName
    ;

msckRepairTableStatement:
    KW_MSCK KW_REPAIR? KW_TABLE tableName ((KW_ADD|KW_DROP) KW_PARTITIONS)? (KW_WITH KW_PROPERTIES LPAREN tablePropertiesList RPAREN)?;

ddlStatement
    : createDatabaseStatement
    | switchDatabaseStatement
    | dropDatabaseStatement
    | createSchemaStatement
    | dropSchemaStatement
    | createTableStatement
    | dropTableStatement
    | truncateTableStatement
    | xcopySnapshotTableStatement
    | createConnectionStatement
    | alterConnectionStatement
    | dropConnectionStatement
    | describeConnectionStatement
    | showConnectionsStatement
    | alterStatement
    | descStatement
    | showStatement
    | listStatement
    | createViewStatement
    | createMaterializedViewStatement
    | dropViewStatement
    | dropMaterializedViewStatement
    | createFunctionStatement
    | createSqlFunctionStatement
    | createEmbeddedFunctionStatement
    | cloneTableStatement
    | createMacroStatement
    | createIndexStatement
    | dropIndexStatement
    | dropFunctionStatement
    | reloadFunctionStatement
    | alterFunctionResourceStatement
    | alterFunctionPropertiesStatement
    | dropMacroStatement
    | lockStatement
    | unlockStatement
    | lockDatabase
    | unlockDatabase
    | tableHistoryStatement
    | setExstore
    | createServerStatement
    | dropServerStatement
    | createStreamStatement
    | dropStreamStatement
    | createTaskStatement
    | alterTaskStatement
    | dropTaskStatement
    | showTaskHistoryStatement
    | createDataMaskingPolicyStatement
    | applyDataMaskingPolicyStatement
    | dropDataMaskingPolicyStatement
    | descDataMaskingPolicyStatement
    | listDataMaskingPolicyStatement
    | createSnapshotTableStatement
    | alterSnapshotTableStatement
    | dropSnapshotTableStatement
    | createSqlMlModelStatment
    | alterSqlMlModelStatement
    | dropSqlMlModelStatement
    | showSqlMlModelVersionsStatement
    ;

createServerStatement :
    KW_CREATE orReplace? KW_SERVER ifNotExists? name=identifier KW_FOREIGN KW_DATA KW_WRAPPER fdw=identifier
    (KW_OPTIONS optionItemsList)?
    (KW_WITH KW_PROPERTIES tableProperties)?
    commentClause?
    ;

optionItemsList:
    LPAREN items+=optionItem (COMMA items+=optionItem)* RPAREN
    ;

optionItem :
    identifier stringLiteral?
    ;

dropServerStatement:
  KW_DROP KW_SERVER ifExists? name=identifier restrictOrCascade?
  ;

partitionSpecOrPartitionId : partitionSpec | KW_PARTITION LPAREN tablePropertiesList RPAREN;

tableOrTableId
    : KW_TABLE table=tableName (LPAREN tablePropertiesList RPAREN)?
    ;
tableHistoryStatement
    : KW_RESTORE tableOrTableId part+=partitionSpecOrPartitionId* (KW_TO KW_LSN v=stringLiteral)? (KW_AS as=tableName)?
    | KW_SHOW KW_HISTORY KW_FOR tableOrTableId part+=partitionSpecOrPartitionId* lsnOffsetLimit?
    | KW_SHOW KW_HISTORY KW_FOR KW_TABLES (KW_LIKE showStmtIdentifier)?
    ;

lsnOffsetLimit:
    (KW_LIMIT limit=mathExpression)?
    ((KW_LSN v=stringLiteral) | (KW_OFFSET offset=mathExpression))
    ;

setExstore:
    KW_EXSTORE tableName partitionSpec;

ifExists
    : KW_IF KW_EXISTS
    ;

restrictOrCascade
    : KW_RESTRICT
    | KW_CASCADE
    ;

ifNotExists
    : KW_IF KW_NOT KW_EXISTS
    ;

rewriteEnabled
    : KW_ENABLE KW_REWRITE
    ;

rewriteDisabled
    : KW_DISABLE KW_REWRITE
    ;

storedAsDirs
    : KW_STORED KW_AS KW_DIRECTORIES
    ;

orReplace
    : KW_OR KW_REPLACE
    ;

ignoreProtection
        : KW_IGNORE KW_PROTECTION
        ;

createDatabaseStatement
    : KW_CREATE KW_DATABASE
        ifNotExists?
        name=identifier
        commentClause?
        dbLocation?
        (KW_WITH KW_DBPROPERTIES dbprops=dbProperties)?
    ;

schemaName
    :
    db=identifier DOT s=identifier
    |
    s=identifier
    ;

createSchemaStatement
    :
    (
      KW_CREATE KW_SCHEMA ifNotExists? name=schemaName
      |
      KW_CREATE KW_EXTERNAL KW_SCHEMA ifNotExists? name=schemaName (withForeignServer | withConnection) KW_ON externalQualifiedParts=stringLiteral
    )
    commentClause?
    ;

withConnection
    :
    KW_WITH KW_CONNECTION identifier
    ;

createConnectionStatement
    :
    KW_CREATE orReplace? KW_CONNECTION ifNotExists? connection_name=identifier KW_OPTIONS connectionOptionItemsList
    ;

connectionOptionItemsList
    :
    LPAREN items+=connectionOptionItem (COMMA items+=connectionOptionItem)* RPAREN
    ;

connectionOptionItem
    :
    identifier EQUAL constant
    ;

alterConnectionStatement
    :
    KW_ALTER KW_CONNECTION ifExists? connection_name=identifier KW_SET KW_OPTIONS connectionOptionItemsList
    ;

dropConnectionStatement
    :
    KW_DROP KW_CONNECTION ifExists? connection_name=identifier
    ;

describeConnectionStatement
    :
    (KW_DESCRIBE|KW_DESC) KW_CONNECTION connection_name=identifier
    ;

showConnectionsStatement
    :
    KW_SHOW KW_CONNECTIONS (KW_LIKE showStmtIdentifier)?
    ;

withForeignServer
    :
    KW_WITH identifier
    ;

dbLocation
    :
      KW_LOCATION locn=simpleStringLiteral
    ;

dbProperties
    :
      LPAREN dbPropertiesList RPAREN
    ;

dbPropertiesList
    :
      keyValueProperty (COMMA keyValueProperty)*
    ;


switchDatabaseStatement
    : KW_USE db=identifier (DOT s=identifier)?
    ;

dropDatabaseStatement
    : KW_DROP KW_DATABASE ifExists? identifier restrictOrCascade?
    ;

dropSchemaStatement
    : KW_DROP KW_SCHEMA ifExists? schemaName KW_PURGE? restrictOrCascade?
    ;

commentClause
    : KW_COMMENT comment=stringLiteral
    ;

dataFormatDesc
    :
    rf=tableRowFormat? ff=tableFileFormat? loc=tableLocation? res=externalTableResource?
    |
    loc=tableLocation? rf=tableRowFormat? ff=tableFileFormat? res=externalTableResource?
    ;
createTableStatement
    : KW_CREATE (orReplace)? (temp=KW_TEMPORARY)? (ext=KW_EXTERNAL)? KW_TABLE ine = ifNotExists? name=tableName
      (  like=KW_LIKE likeName=tableName
         dd = dataFormatDesc
         prop=tablePropertiesPrefixed?
         lifecycle=tableLifecycle?
       | (LPAREN columns=columnNameTypeConstraintList (COMMA outOfLineConstraints)? RPAREN)?
         comment=commentClause?
         (partitions = tablePartition | autoPartition = autoTablePartition)?
         buckets=tableBuckets?
         skewed=tableSkewed?
         connection=withConnection?
         dd = dataFormatDesc
         prop=tablePropertiesPrefixed?
         lifecycle=tableLifecycle?
         (KW_INTO shards=Number KW_SHARDS)?
         (KW_HUBLIFECYCLE hubLifeCycle=Number)?
         (KW_CHANGELOGS changeLogs=Number)?
         (KW_AS dataSource=query)?
      )
    | KW_CREATE (orReplace)? KW_TABLE ine = ifNotExists? name=tableName KW_CLONE snapshotName=tableName
    | KW_CREATE KW_OBJECT KW_TABLE ine = ifNotExists? name=tableName
      comment=commentClause?
      partitions=tablePartition?
      (KW_WITH serdeProp=serdeProperties)?
      loc=tableLocation
      prop=tablePropertiesPrefixed?
      lifecycle=tableLifecycle?
    ;

createSnapshotTableStatement
    : KW_CREATE (orReplace)? KW_SNAPSHOT KW_TABLE ine = ifNotExists? name=tableName KW_CLONE srcName=tableName
      singleExpPartSpec?
      (KW_FOR KW_SYSTEMTIME KW_AS KW_OF time=expression)?
      (snapshotOptionList)?
    ;

singleExpPartSpec
    :
    KW_PARTITION
     LPAREN partVal=expression RPAREN
    ;

alterSnapshotTableStatement
    : KW_ALTER KW_SNAPSHOT KW_TABLE ifExists? name=tableName KW_SET snapshotOptionList
    ;

dropSnapshotTableStatement
    : KW_DROP KW_SNAPSHOT KW_TABLE ifExists? name=tableName
    ;

snapshotOptionList
    : KW_OPTIONS LPAREN (kv+=idExpProperty (COMMA kv+=idExpProperty)*) RPAREN
    ;

idExpProperty
    : key=identifier EQUAL value=expression
    ;

truncateTableStatement
    : KW_TRUNCATE KW_TABLE tablePartitionPrefix (KW_COLUMNS LPAREN columnNameList RPAREN)?
    ;

xcopySnapshotTableStatement
    : KW_XCOPY KW_SNAPSHOT srcName=tableName KW_TO KW_SNAPSHOT dstName=tableName
    (KW_IF KW_EXISTS (KW_OVERWRITE | KW_IGNORE))?
    snapshotOptionList
    ;

createIndexStatement
    : KW_CREATE (idxType=identifier)? KW_INDEX indexName=identifier
      KW_ON KW_TABLE? tab=tableName (KW_FOR KW_COLUMNS)? LPAREN part+=multipartIdentifier (COMMA part+=multipartIdentifier)* RPAREN
      (KW_AS typeNameHive=simpleStringLiteral)?
      autoRebuild?
      prop=indexPropertiesPrefixed?
      indexTblName?
      tableRowFormat?
      tableFileFormat?
      tableLocation?
      tablePropertiesPrefixed?
      commentClause?
    ;

autoRebuild
    : KW_WITH KW_DEFERRED KW_REBUILD
    ;

indexTblName
    : KW_IN KW_TABLE indexTbl=tableName
    ;

indexPropertiesPrefixed
    :
        KW_IDXPROPERTIES indexProperties
    ;

indexProperties
    :
      LPAREN indexPropertiesList RPAREN
    ;

indexPropertiesList
    :
      kv+=keyValueProperty (COMMA kv+=keyValueProperty)*
    ;

dropIndexStatement
    : KW_DROP KW_INDEX ifExists? indexName=identifier KW_ON (KW_TABLE)? tab=tableName
    ;

dropTableStatement
    : KW_DROP KW_TABLE ifExists? tableName KW_PURGE? replicationClause?
    ;

dropTaskStatement
    : KW_DROP KW_TASK ifExists? taskIdentifier=identifier
    ;

alterStatement
    : KW_ALTER KW_TABLE tableName tableSuffix=alterTableStatementSuffix
    | KW_ALTER KW_VIEW tableName KW_AS? viewSuffix=alterViewStatementSuffix
    | KW_ALTER KW_STREAM streamName streamSuffix=alterStreamStatementSuffix
    | KW_ALTER KW_MATERIALIZED KW_VIEW (tableName materializedViewSuffix=alterMaterializedViewStatementSuffix | tableFunction=tableFunctionSource KW_REBUILD)
    | KW_ALTER KW_INDEX idxSuffix=alterIndexStatementSuffix
    | KW_ALTER KW_DATABASE dbSuffix=alterDatabaseStatementSuffix
    | KW_ALTER KW_TENANT? (u=KW_USER | KW_ROLE) uname=user (KW_SET p=privilegeProperties | KW_UNSET k=privilegePropertieKeys)
    | KW_ALTER KW_SCHEMA s=schemaName schemaSuffix=alterSchemaStatementSuffix
    | KW_ALTER KW_SERVER serverName=identifier serverSuffix=alterServerStatementSuffix
    ;

alterServerStatementSuffix
    : elements+=alterServerElements (COMMA elements+=alterServerElements)*
    | alterServerChangeOwner
    | alterServerSetComment
    ;

alterServerElements
    : KW_SET KW_OPTIONS optionItemsList
    | KW_UNSET KW_OPTIONS LPAREN optionKeys+=identifier (COMMA optionKeys+=identifier)* RPAREN
    | KW_SET KW_PROPERTIES tableProperties
    | KW_UNSET KW_PROPERTIES LPAREN keys+=simpleStringLiteral (COMMA keys+=simpleStringLiteral)* RPAREN
    ;

alterServerSetComment
    : KW_SET KW_COMMENT comment=stringLiteral
    ;

alterServerChangeOwner
    : KW_CHANGEOWNER KW_TO stringLiteral
    ;

alterSchemaStatementSuffix
    : alterStatementSuffixChangeOwner
    | alterStatementSuffixRename[false]
    | KW_SET commentClause
    ;

alterTableStatementSuffix
    : rename=alterStatementSuffixRename[true]
    | dropPartition=alterStatementSuffixDropPartitions[true]
    | addPartition=alterStatementSuffixAddPartitions[true]
    | rebuildIndex=alterStatementSuffixRebuildIndex
    | alterStatementSuffixTouch
    | alterStatementSuffixArchive
    | alterStatementSuffixUnArchive
    | alterStatementSuffixProperties
    | alterStatementSuffixRewriteProperties
    | alterStatementSuffixSkewedby
    | alterStatementSuffixExchangePartition
    | alterStatementPartitionKeyType
    | partition=partitionSpec? tblPartition=alterTblPartitionStatementSuffix
    | alterTableChangeOwner
    | alterTableEnableHubTable
    | alterTableMergePartitionSuffix
    | alterStatementSuffixAddConstraint
    | alterStatementSuffixClearColumn
    | alterStatementSuffixRefreshMetadata
    | alterStatementSuffixRecluster
    | KW_BUILD KW_CDC
    ;

alterTableMergePartitionSuffix
    : KW_MERGE (c=KW_IF KW_EXISTS)? src+=partitionSpec (COMMA src+=partitionSpec)* KW_OVERWRITE dest=partitionSpec p=KW_PURGE?;

alterStatementSuffixAddConstraint
    : KW_DROP ( KW_CONSTRAINT n=identifier | KW_PRIMARY KW_KEY?)
    | KW_ADD oolc=outOfLineConstraints
    | KW_RENAME KW_CONSTRAINT n=identifier KW_TO nn=identifier
    ;

alterTblPartitionStatementSuffix
  : alterStatementSuffixFileFormat
  | alterStatementSuffixLocation
  | alterStatementSuffixProtectMode
  | alterStatementSuffixMergeFiles
  | alterStatementSuffixSerdeProperties
  | alterStatementSuffixRenamePart
  | alterStatementSuffixBucketNum
  | alterTblPartitionStatementSuffixSkewedLocation
  | alterStatementSuffixClusterbySortby
  | alterStatementSuffixCompact
  | alterStatementSuffixUpdateStatsCol
  | renameCol=alterStatementSuffixRenameCol
  | addCol=alterStatementSuffixAddCol
  | changeCols=alterStatementSuffixChangeCols
  | alterStatementSuffixDropCol
  | properties=alterTblPartitionStatementSuffixProperties
  | alterStatementSuffixPartitionLifecycle
  ;

alterStatementSuffixPartitionLifecycle:
    (KW_ENABLE | KW_DISABLE) KW_LIFECYCLE;

alterTblPartitionStatementSuffixProperties:
    KW_SET KW_PARTITIONPROPERTIES tableProperties;

alterStatementPartitionKeyType
	: KW_PARTITION KW_COLUMN LPAREN columnNameType RPAREN
	;

alterViewStatementSuffix
    : alterViewSuffixProperties
    | alterStatementSuffixRename[false]
    | alterStatementSuffixAddPartitions[false]
    | alterStatementSuffixDropPartitions[false]
    | alterViewChangeOwner
    | q=query
    | alterViewColumnCommentSuffix
    ;

alterMaterializedViewStatementSuffix
    : alterMaterializedViewSuffixRewrite
    | alterMaterializedViewSuffixRebuild
    | dropPartition=alterStatementSuffixDropPartitions[true]
    | alterStatementSuffixTouch
    | alterStatementSuffixArchive
    | alterStatementSuffixUnArchive
    | alterStatementSuffixProperties
    | partition=partitionSpec? tblPartition=alterMvPartitionStatementSuffix
    | alterTableChangeOwner
    ;

alterMaterializedViewSuffixRewrite
    : (rewriteEnabled | rewriteDisabled)
    ;

partitionSpecExpression
    :
    KW_PARTITION
     LPAREN expr+=expression (COMMA expr+=expression )* RPAREN
    ;

alterMaterializedViewSuffixRebuild
    : KW_REBUILD partitionSpecExpression?
    ;

alterMvPartitionStatementSuffix
  : alterStatementSuffixFileFormat
  | alterStatementSuffixLocation
  | alterStatementSuffixProtectMode
  | alterStatementSuffixMergeFiles
  | alterStatementSuffixSerdeProperties
  | alterStatementSuffixBucketNum
  | alterStatementSuffixClusterbySortby
  | alterStatementSuffixCompact
  | properties=alterTblPartitionStatementSuffixProperties
  | alterStatementSuffixPartitionLifecycle
  ;

alterIndexStatementSuffix
    : indexName=identifier KW_ON KW_TABLE? tableName partitionSpec?
    (
      KW_REBUILD
    |
      KW_SET KW_IDXPROPERTIES
      indexProperties
    )
    ;

alterDatabaseStatementSuffix
    : alterDatabaseSuffixProperties
    | alterDatabaseSuffixSetOwner
    ;

alterDatabaseSuffixProperties
    : name=identifier KW_SET KW_DBPROPERTIES dbProperties
    ;

alterDatabaseSuffixSetOwner
    : dbName=identifier KW_SET KW_OWNER principalName
    ;

alterStatementSuffixRename[boolean table]
    : KW_RENAME KW_TO identifier
    ;

alterStatementSuffixAddCol
    : (add=KW_ADD | replace=KW_REPLACE)(KW_COLUMNS | KW_COLUMN) ifNotExists?
      (
        LPAREN cols=columnNameTypeConstraintWithPosList RPAREN
        |
        cols=columnNameTypeConstraintWithPosList
      )
     restrictOrCascade?
    ;

alterStatementSuffixChangeCols
    : KW_CHANGE KW_COLUMNS LPAREN cols=changeColumnNameTypeList RPAREN
    ;

alterStatementSuffixRenameCol
    : KW_CHANGE KW_COLUMN? oldName=multipartIdentifier
    ( KW_RENAME KW_TO newName=identifier
    | commentClause
    | n+=constraints+
    | columnNameTypeConstraintWithPos restrictOrCascade?
    )
    | KW_RENAME KW_COLUMN oldName=multipartIdentifier KW_TO newName=identifier
    ;

alterStatementSuffixDropCol
    : KW_DROP (KW_COLUMNS | KW_COLUMN)
    (
        part+=multipartIdentifier (COMMA part+=multipartIdentifier)*
        |
        LPAREN part+=multipartIdentifier (COMMA part+=multipartIdentifier)* RPAREN
    )
    ;
alterStatementSuffixUpdateStatsCol
    : KW_UPDATE KW_STATISTICS KW_FOR KW_COLUMN? colName=identifier KW_SET tableProperties (KW_COMMENT comment=simpleStringLiteral)?
    ;

alterStatementChangeColPosition
    : first=KW_FIRST|KW_AFTER afterCol=identifier
    ;

alterStatementSuffixAddPartitions[boolean table]
    : KW_ADD ifNotExists? elem+=alterStatementSuffixAddPartitionsElement+
    ;

alterStatementSuffixAddPartitionsElement
    : spec=partitionSpec location=partitionLocation?
    ;

alterStatementSuffixTouch
    : KW_TOUCH (partitionSpec)*
    ;

alterStatementSuffixArchive
    : KW_ARCHIVE (partitionSpec)*
    ;

alterStatementSuffixUnArchive
    : KW_UNARCHIVE (partitionSpec)*
    ;

alterStatementSuffixChangeOwner
    : KW_CHANGEOWNER KW_TO stringLiteral
    ;

partitionLocation
    :
      KW_LOCATION locn=stringLiteral
    ;

alterStatementSuffixDropPartitions[boolean table]
    : KW_DROP ifExists? elem+=dropPartitionSpec (COMMA elem+=dropPartitionSpec)* ignore=ignoreProtection? purge=KW_PURGE? replication=replicationClause?
    ;

alterStatementSuffixProperties
    : KW_SET KW_TBLPROPERTIES tableProperties
    | KW_UNSET KW_TBLPROPERTIES ifExists? tableProperties
    | KW_SET tableLifecycle
    | KW_SET commentClause
    | KW_SET KW_CHANGELOGS Number
    | KW_SET KW_HUBLIFECYCLE Number
    ;

alterStatementSuffixRewriteProperties
    : KW_REWRITE KW_TBLPROPERTIES tableProperties
    ;

alterViewSuffixProperties
    : KW_SET KW_TBLPROPERTIES prop=tableProperties
    | KW_UNSET KW_TBLPROPERTIES ifExists? tableProperties
    ;

alterViewColumnCommentSuffix
    : KW_CHANGE KW_COLUMN col=identifier KW_COMMENT cmt=stringLiteral;

alterStatementSuffixSerdeProperties
    : KW_SET KW_SERDE serdeName=simpleStringLiteral (KW_WITH serdeProperties)?
    | KW_SET serdeProperties
    ;

tablePartitionPrefix
  : tableName (part+=dropPartitionSpec (COMMA part+=dropPartitionSpec)*)?
  ;

alterStatementSuffixFileFormat
	: KW_SET KW_FILEFORMAT fileFormat
	;

alterStatementSuffixClusterbySortby
  : notClustered = KW_NOT KW_CLUSTERED
  | notSorted = KW_NOT KW_SORTED
  | shards = tableShards
  | buckets = tableBuckets
  ;

alterTblPartitionStatementSuffixSkewedLocation
  : KW_SET KW_SKEWED KW_LOCATION skewedLocations
  ;

skewedLocations
    :
      LPAREN skewedLocationsList RPAREN
    ;

skewedLocationsList
    :
      skewedLocationMap (COMMA skewedLocationMap)*
    ;

skewedLocationMap
    :
      key=skewedValueLocationElement EQUAL value=simpleStringLiteral
    ;

alterStatementSuffixLocation
  : KW_SET KW_LOCATION newLoc=simpleStringLiteral
  ;


alterStatementSuffixSkewedby
	: tableSkewed
	|
	 KW_NOT KW_SKEWED
	|
	 KW_NOT storedAsDirs
	;

alterStatementSuffixExchangePartition
    : KW_EXCHANGE partitionSpec KW_WITH KW_TABLE exchangename=tableName
    ;

alterStatementSuffixClearColumn
    : (par+=partitionSpec (COMMA par+=partitionSpec)*)?
    KW_CLEAR KW_COLUMN
    (
        part+=multipartIdentifier (COMMA part+=multipartIdentifier)*
        |
        LPAREN part+=multipartIdentifier (COMMA part+=multipartIdentifier)* RPAREN
    )
    (KW_WITHOUT KW_TOUCH)?
    ;

alterStatementSuffixRefreshMetadata
    : (par+=partitionSpec (COMMA par+=partitionSpec)*
      |
      KW_PARTITION LPAREN partitionExpr=expression RPAREN)?
    KW_REFRESH KW_METADATA;

alterStatementSuffixRecluster
    : (par+=partitionSpec (COMMA par+=partitionSpec)*)?
    KW_RECLUSTER (KW_FULL | KW_PARTIAL);

alterStatementSuffixProtectMode
    : alterProtectMode
    ;

alterStatementSuffixRenamePart
    : KW_RENAME KW_TO partitionSpec
    ;

alterStatementSuffixMergeFiles
    : KW_CONCATENATE | KW_MERGE KW_SMALLFILES
    ;

alterProtectMode
    : KW_ENABLE alterProtectModeMode
    | KW_DISABLE alterProtectModeMode
    ;

alterProtectModeMode
    : KW_OFFLINE
    | KW_NO_DROP KW_CASCADE?
    | KW_READONLY
    ;

alterStatementSuffixBucketNum
    : KW_INTO num=Number (KW_BUCKETS | KW_SHARDS) (KW_HUBLIFECYCLE hubLifeCycle=Number)?
    ;

alterStatementSuffixCompact
    : KW_COMPACT compactType=simpleStringLiteral
    ;

alterStatementSuffixRebuildIndex
    : partitionSpecExpression? KW_REBUILD (KW_BLOOMFILTER | KW_BITMAP) KW_INDEX
    ;

alterTaskStatement
    : KW_ALTER KW_TASK ifExists? taskIdentifier=identifier
    ( status=identifier
    | (KW_SET (KW_SCHEDULE EQUAL simpleStringLiteral)? prop=taskPropertiesPrefixed? comment=commentClause?)
    | (KW_UNSET KW_SCHEDULE? comment=commentClause?)
    | (KW_MODIFY KW_WHEN whenExp=expression))
    ;

fileFormat
    : KW_INPUTFORMAT inFmt=simpleStringLiteral KW_OUTPUTFORMAT outFmt=simpleStringLiteral KW_SERDE serdeCls=simpleStringLiteral (KW_INPUTDRIVER inDriver=simpleStringLiteral KW_OUTPUTDRIVER outDriver=simpleStringLiteral)?
    | genericSpec=identifier
    ;

tabTypeExpr
   : allIdentifiers (DOT(KW_ELEM_TYPE|KW_KEY_TYPE|KW_VALUE_TYPE| allIdentifiers))* identifier?;

partTypeExpr
    :  tabTypeExpr partitionSpec?
    ;


descStatement:
    (KW_DESCRIBE|KW_DESC)
    ( KW_DATABASE KW_EXTENDED? identifier
    | KW_PACKAGE (proj=projectName DOT)? pkgName=packageName privilegeProperties?
    | KW_PACKAGE KW_ITEMS obj=privilegeObject (KW_FROM proj=projectName)?
    | KW_TENANT? KW_ROLE roleName
    | KW_TENANT? KW_USERGROUP userGroupName
    | KW_FUNCTION KW_EXTENDED? functionIdentifier
    | KW_CHANGELOGS KW_FOR KW_TABLE? tableName partitionSpec? Number?
    | KW_EXTENDED? variableName // useless grammar
    | KW_STREAM KW_EXTENDED? streamName
    | KW_SCHEMA KW_EXTENDED? schemaName
    | KW_SERVER KW_EXTENDED? identifier
    | KW_TASK KW_EXTENDED? taskIdentifier=identifier
    | KW_REFRESH KW_TASK KW_EXTENDED? KW_FOR KW_OBJECT KW_TABLE tabname=tableName
    | KW_MODEL KW_EXTENDED? model=tableName (KW_WITH KW_VERSION version=identifier)?
    | (KW_FORMATTED| KW_EXTENDED |KW_PRETTY)? partTypeExpr);

analyzeStatement
    :
    KW_ANALYZE KW_TABLE (parttype=tableOrPartition)
    (
      KW_COMPUTE KW_STATISTICS
      (
        noscan=KW_NOSCAN
        | partialscan=KW_PARTIALSCAN
        | KW_FOR isForColumns=KW_COLUMNS forColumns = analyzedColumns? (KW_WITH KW_HISTOGRAM (bucketNum=Number KW_BUCKETS)? (KW_COLUMNS histogramColumns=analyzedColumns)?)?
      )?
      | del=KW_DELETE KW_STATISTICS KW_FOR KW_COLUMNS forColumns = analyzedColumns
    )
    ;

analyzedColumns:
    LPAREN cols+=columnNameOrList (COMMA cols+=columnNameOrList)* RPAREN
    ;

columnNameOrList
    : columnName
    | LPAREN columnNameList RPAREN
    ;

showStatement
    : KW_SHOW KW_DATABASES (KW_LIKE showStmtIdentifier)?   // odps: list projects
    | KW_SHOW KW_TABLES ((KW_FROM|KW_IN) db_name=identifier)? (KW_LIKE showStmtIdentifier|showStmtIdentifier)?
    | KW_SHOW KW_SCHEMAS ((KW_FROM|KW_IN) db_name=identifier)? (KW_LIKE showStmtIdentifier|showStmtIdentifier)?
    | KW_SHOW KW_SERVERS
    | KW_SHOW KW_STREAMS
    | KW_SHOW KW_COLUMNS (KW_FROM|KW_IN) tableName ((KW_FROM|KW_IN) db_name=identifier)?
    | KW_SHOW KW_BUILTIN? KW_FUNCTIONS (KW_LIKE showFunctionIdentifier|showFunctionIdentifier)?
    | KW_SHOW KW_PARTITIONS tabName=tableName partitionSpec?
    | KW_SHOW KW_CREATE KW_TABLE tabName=tableName
    | KW_SHOW KW_CREATE KW_MATERIALIZED KW_VIEW tabName=tableName
    | KW_SHOW KW_CREATE KW_VIEW tabName=tableName
    | KW_SHOW KW_TABLE KW_EXTENDED ((KW_FROM|KW_IN) db_name=identifier)? KW_LIKE showStmtIdentifier partitionSpec?
    | KW_SHOW KW_TBLPROPERTIES tableName (LPAREN prptyName=simpleStringLiteral RPAREN)?
    | KW_SHOW KW_LOCKS
      (
      (KW_DATABASE|KW_SCHEMA) (dbName=Identifier) (isExtended=KW_EXTENDED)?
      |
      (parttype=partTypeExpr)? (isExtended=KW_EXTENDED)?
      )
    | KW_SHOW (showOptions=KW_FORMATTED)? (KW_INDEX|KW_INDEXES) KW_ON (KW_TABLE)? tabName=tableName ((KW_FROM|KW_IN) db_name=identifier)?
    | KW_SHOW KW_COMPACTIONS
    | KW_SHOW KW_TRANSACTIONS
    | KW_SHOW KW_CONF simpleStringLiteral
    | KW_SHOW KW_P (KW_FROM bareDate KW_TO bareDate)? (Number)? // show instances
    | KW_SHOW KW_JOB stringLiteral (KW_FROM bareDate KW_TO bareDate)?
    | KW_SHOW KW_FLAGS
    | KW_SHOW KW_CHANGELOGS KW_FOR KW_TABLE? tableName partitionSpec? Number?
    | KW_SHOW KW_RECYCLEBIN ((KW_FROM|KW_IN) db_name=identifier)?
    | KW_SHOW KW_VARIABLES (KW_LIKE showStmtIdentifier)? // useless grammar
    | KW_SHOW KW_TASKS
    | KW_SHOW KW_MODELS
    | KW_SHOW KW_CREATE KW_TASK tabName=tableName
    ;

listStatement:
    KW_LIST (KW_PROJECTS | KW_JOBS | KW_RESOURCES | KW_FUNCTIONS | KW_ACCOUNTPROVIDERS | KW_TEMPORARY KW_OUTPUT);

bareDate:
    stringLiteral | dateWithoutQuote;

lockStatement
    : KW_LOCK KW_TABLE tableName partitionSpec? lockMode
    ;

lockDatabase
    : KW_LOCK (KW_DATABASE|KW_SCHEMA) (dbName=Identifier) lockMode
    ;

lockMode
    : KW_SHARED | KW_EXCLUSIVE
    ;

unlockStatement
    : KW_UNLOCK KW_TABLE tableName partitionSpec?
    ;

unlockDatabase
    : KW_UNLOCK (KW_DATABASE|KW_SCHEMA) (dbName=Identifier)
    ;

resourceList
  :
  resource (COMMA resource)*
  ;

resource
  :
  resType=resourceType? resPath=simpleStringLiteral
  ;

resourceType
  :
  KW_JAR
  |
  KW_FILE
  |
  KW_ARCHIVE
  ;

createFunctionStatement
    :
    KW_CREATE (temp=KW_TEMPORARY)? KW_SECURE? KW_FUNCTION functionIdentifier KW_AS simpleStringLiteral
    (KW_USING (rList=resourceList | codeBlock=userCodeBlock))?
    ;

dropFunctionStatement
    : KW_DROP (temp=KW_TEMPORARY)? KW_FUNCTION ifExists? functionIdentifier
    ;

reloadFunctionStatement
    : KW_RELOAD KW_FUNCTION
    ;

alterFunctionResourceStatement
    :
    KW_ALTER KW_FUNCTION functionIdentifier
    (
      (KW_SET KW_RESOURCES setArch=simpleStringLiteral rlist=resourceList)
      |
      (KW_UNSET KW_RESOURCES unsetArch=simpleStringLiteral)
    )
    ;

alterFunctionPropertiesStatement
    :
    KW_ALTER KW_FUNCTION functionIdentifier
    KW_SET KW_PROPERTIES tableProperties
    ;

createMacroStatement
    : KW_CREATE KW_TEMPORARY KW_MACRO Identifier
      LPAREN columnNameTypeList? RPAREN expression
    ;

dropMacroStatement
    : KW_DROP KW_TEMPORARY KW_MACRO ifExists? Identifier
    ;

createSqlFunctionStatement
    :
    KW_CREATE (orReplace)? KW_SECURE? KW_SQL? KW_FUNCTION (ifNotExists)? name=functionIdentifier
    param=functionParameters (KW_RETURNS retvar=variableName retType=typeDeclaration)?
    comment=commentClause? res=viewResource?
    KW_AS (funBody=compoundStatement|exp=expression|q=query)
    ;

createEmbeddedFunctionStatement
    :
    KW_CREATE (orReplace)? KW_EMBEDDED KW_FUNCTION (ifNotExists)? functionIdentifier KW_AS simpleStringLiteral
    (KW_USING (rList=resourceList | codeBlock=userCodeBlock))?
    ;

cloneTableStatement
    :
    KW_CLONE KW_TABLE src=tableName (par+=partitionSpec (COMMA par+=partitionSpec)*)?
    KW_TO dest=tableName (dstpar+=partitionSpec (COMMA dstpar+=partitionSpec)*)?
    (KW_IF KW_EXISTS ( o=KW_OVERWRITE | i=KW_IGNORE ))?;

createViewStatement
    :
    KW_CREATE (orReplace)? KW_SECURE? KW_VIEW (ifNotExists)? name=tableName
    (
    (LPAREN columnNameCommentList RPAREN)?
    |
    param=functionParameters
    (KW_RETURNS retvar=variableName retType=typeDeclaration)?
    )
    comment=commentClause? viewPartition? res=viewResource? prop=tablePropertiesPrefixed?
    KW_AS (funBody=compoundStatement|q=query)
    ;

createStreamStatement
    :
    KW_CREATE KW_STREAM ifNotExists? stm=streamName KW_ON KW_TABLE tbl=tableName
    tr=timeTravel?
    prop=streamPropertiesPrefixed
    comment=commentClause?
    ;

streamPropertiesPrefixed
    :
    KW_STRMPROPERTIES streamProperties
    ;

streamProperties
    :
    LPAREN streamPropertiesList RPAREN
    ;

streamPropertiesList
    :
    kv+=keyValueProperty (COMMA kv+=keyValueProperty)*
    ;

alterStreamStatementSuffix
    :
    KW_SET KW_STRMPROPERTIES streamProperties comment=commentClause?
    |
    KW_ON KW_TABLE tbl=tableName tr=timeTravel
    ;

streamName
    :
    db=identifier DOT stm=identifier
    |
    stm=identifier
    |
    db=identifier DOT sch=identifier DOT stm=identifier
    ;

dropStreamStatement
    :
    KW_DROP KW_STREAM ifExists? stm=streamName
    ;

viewPartition
    : KW_PARTITIONED (KW_ON|KW_BY) LPAREN columnNameList RPAREN
    ;

dropViewStatement
    : KW_DROP KW_VIEW ifExists? name=tableName
    ;

scheduleSpec
    : KW_REFRESH (KW_START startTime=simpleStringLiteral)? KW_EVERY value=Number? qualifier=intervalQualifiersUnit ((KW_AT|KW_OFFSET KW_BY) offsetTs=simpleStringLiteral)?
    ;

createMaterializedViewStatement
    :
    KW_CREATE KW_SECURE? KW_MATERIALIZED KW_VIEW (ifNotExists)? name=tableName
    outOfLineConstraints?
    lifecycle=tableLifecycle? (buildImmediate|buildDeferred)?
    (
    (LPAREN columnNameCommentList RPAREN)?
    |
    param=functionParameters
    (KW_RETURNS retvar=variableName retType=typeDeclaration)?
    )
    rewriteDisabled? comment=commentClause?
    partitionCols=viewPartition? buckets=tableBuckets?
    schedule=scheduleSpec?
    prop=tablePropertiesPrefixed?
    KW_AS (funBody=compoundStatement|q=query)
    ;

taskStatement
    : queryStatement
    | dmlStatement
    | createTableStatement
    | alterStatement
    ;

createTaskStatement
    : KW_CREATE (orReplace)? KW_TASK (ifNotExists)? taskIdentifier=identifier
    KW_SCHEDULE EQUAL simpleStringLiteral prop=taskPropertiesPrefixed? comment=commentClause?
    (KW_WHEN whenExp=expression)?
    KW_AS taskBody=taskStatement
    ;

taskPropertiesPrefixed:
    KW_TASKPROPERTIES taskProperties;

taskProperties:
    LPAREN taskPropertiesList RPAREN;

taskPropertiesList:
    kv+=keyValueProperty (COMMA kv+=keyValueProperty)*;

showTaskHistoryStatement
    : KW_SHOW KW_HISTORY KW_FOR KW_TASK taskIdentifier=identifier lsnOffsetLimit?
    | KW_SHOW KW_HISTORY KW_FOR KW_TASKS (KW_LIKE showStmtIdentifier)?
    | KW_SHOW KW_REFRESH KW_TASK KW_HISTORY KW_FOR KW_OBJECT KW_TABLE tabname=tableName lsnOffsetLimit?;

buildImmediate
    : KW_BUILD KW_IMMEDIATE
    ;

buildDeferred
    : KW_BUILD KW_DEFERRED
    ;

dropMaterializedViewStatement
    : KW_DROP KW_MATERIALIZED KW_VIEW ifExists? name=tableName KW_PURGE?
    ;

showFunctionIdentifier
    : functionIdentifier
    | simpleStringLiteral
    ;

showStmtIdentifier
    : identifier
    | simpleStringLiteral
    ;

tablePartition
    : KW_PARTITIONED KW_BY LPAREN partitionColumnNameTypeList RPAREN
    ;

autoTablePartition
    : KW_AUTO KW_PARTITIONED KW_BY LPAREN namedExpression RPAREN
    ;

tableBuckets
    :
    KW_CLUSTERED KW_BY LPAREN bucketCols=clusterColumnNameOrderList RPAREN
    (KW_SORTED KW_BY LPAREN sortCols=columnNameOrderList RPAREN)?
    (KW_INTO num=Number KW_BUCKETS)?
    |
    range=KW_RANGE KW_CLUSTERED KW_BY LPAREN bucketCols=clusterColumnNameOrderList RPAREN
    (KW_SORTED KW_BY LPAREN sortCols=columnNameOrderList RPAREN)?
    (KW_INTO num=Number KW_BUCKETS)?
    ;

tableShards
    :
    KW_CLUSTERED KW_BY LPAREN bucketCols=columnNameList RPAREN
    (KW_SORTED KW_BY LPAREN sortCols=columnNameOrderList RPAREN)?
    KW_INTO num=Number shard=KW_SHARDS
    |
    KW_DISTRIBUTE KW_BY LPAREN bucketCols=columnNameList RPAREN
    (KW_SORT KW_BY LPAREN sortCols=columnNameOrderList RPAREN)?
    KW_INTO num=Number shard=KW_SHARDS
    ;

tableSkewed
    :
     KW_SKEWED KW_BY LPAREN skewedCols=columnNameList RPAREN KW_ON LPAREN (skewedValues=skewedValueElement) RPAREN storedAsDirs?
    ;

recordReader
    : KW_RECORDREADER reader=stringLiteral
    |
    ;

recordWriter
    : KW_RECORDWRITER writer=stringLiteral
    |
    ;

serdeProperties
    : KW_SERDEPROPERTIES serdeprops=tableProperties
    ;

rowFormatSerde
    : KW_ROW KW_FORMAT KW_SERDE name=stringLiteral (KW_WITH serdeProperties)?
    ;

rowFormatDelimited
    :
      KW_ROW KW_FORMAT KW_DELIMITED fd=tableRowFormatFieldIdentifier? cd=tableRowFormatCollItemsIdentifier? md=tableRowFormatMapKeysIdentifier? ld=tableRowFormatLinesIdentifier? nul=tableRowNullFormat?
    ;

tableRowFormat
    :
      rfd=rowFormatDelimited
    | rfs=rowFormatSerde
    ;

tablePropertiesPrefixed
    :
        KW_TBLPROPERTIES tableProperties
    ;

tableProperties
    :
      LPAREN tablePropertiesList RPAREN
    ;

tablePropertiesList
    :
      kv+=keyValueProperty (COMMA kv+=keyValueProperty)*
    ;

keyValueProperty
    :
      key=simpleStringLiteral EQUAL value=simpleStringLiteral
    ;

userDefinedJoinPropertiesList
    :
    kv+=keyValueProperty (COMMA kv+=keyValueProperty)*
    ;

keyPrivProperty
    :
    key = stringLiteral
    ;

tableRowFormatFieldIdentifier
    :
      KW_FIELDS KW_TERMINATED KW_BY fldIdnt=stringLiteral (KW_ESCAPED KW_BY fldEscape=stringLiteral)?
    ;

tableRowFormatCollItemsIdentifier
    :
      KW_COLLECTION KW_ITEMS KW_TERMINATED KW_BY collIdnt=stringLiteral
    ;

tableRowFormatMapKeysIdentifier
    :
      KW_MAP KW_KEYS KW_TERMINATED KW_BY mapKeysIdnt=stringLiteral
    ;

tableRowFormatLinesIdentifier
    :
      KW_LINES (KW_TERMINATED|KW_SEPARATED) KW_BY linesIdnt=stringLiteral
    ;

tableRowNullFormat
    :
      KW_NULL KW_DEFINED KW_AS nullIdnt=stringLiteral
    ;
tableFileFormat
    :
      KW_STORED KW_AS KW_INPUTFORMAT inFmt=stringLiteral KW_OUTPUTFORMAT outFmt=stringLiteral (KW_INPUTDRIVER inDriver=simpleStringLiteral KW_OUTPUTDRIVER outDriver=simpleStringLiteral)?
      | KW_STORED KW_BY storageHandler=stringLiteral (KW_WITH serdeProperties)?
      | KW_STORED KW_AS genericSpec=identifier
    ;

tableLocation
    :
      KW_LOCATION locn=stringLiteral
    ;

externalTableResource
    :
      KW_USING res=stringLiteral
    ;

viewResource
    :
      KW_RESOURCES res+=stringLiteral (COMMA res+=stringLiteral)*
    ;

outOfLineConstraints
    :
    (KW_CONSTRAINT n=identifier)?
    KW_PRIMARY KW_KEY? LPAREN c=columnNameList RPAREN e=enableSpec? v=validateSpec? r=relySpec?
    ;

enableSpec
    : KW_ENABLE | KW_DISABLE
    ;

validateSpec
    : KW_VALIDATE | KW_NOVALIDATE
    ;

relySpec
    : KW_RELY | KW_NORELY
    ;

columnNameTypeConstraintList
    : columnNameTypeConstraint (COMMA columnNameTypeConstraint)*
    ;

columnNameTypeList
    : columnNameType (COMMA columnNameType)*
    ;

partitionColumnNameTypeList
    : partitionColumnNameType (COMMA partitionColumnNameType)*
    ;

columnNameTypeConstraintWithPosList
    : columnNameTypeConstraintWithPos (COMMA columnNameTypeConstraintWithPos)*
    ;

changeColumnNameTypeList
    : changeColumnNameType (COMMA changeColumnNameType)*
    ;

columnNameColonTypeList
    : t+=columnNameColonType (COMMA t+=columnNameColonType)*
    ;

columnNameList
    : columnName (COMMA columnName)*
    ;

columnNameListInParentheses
    : LPAREN columnNameList RPAREN
    ;

columnName
    :
      identifier
    ;

columnNameOrderList
    : columnNameOrder (COMMA columnNameOrder)*
    ;

clusterColumnNameOrderList
    : columnNameOrder (COMMA columnNameOrder)*
    ;

skewedValueElement
    :
      skewedColumnValues
     | skewedColumnValuePairList
    ;

skewedColumnValuePairList
    : skewedColumnValuePair (COMMA skewedColumnValuePair)*
    ;

skewedColumnValuePair
    :
      LPAREN colValues=skewedColumnValues RPAREN
    ;

skewedColumnValues
    : skewedColumnValue (COMMA skewedColumnValue)*
    ;

skewedColumnValue
    :
      constant
    ;

skewedValueLocationElement
    :
      skewedColumnValue
     | skewedColumnValuePair
    ;

columnNameOrder
    : identifier (asc=KW_ASC | desc=KW_DESC)? (KW_NULLS (first=KW_FIRST | last=KW_LAST))?
    ;

columnNameCommentList
    : columnNameComment (COMMA columnNameComment)*
    ;

columnNameComment
    : colName=identifier commentClause?
    ;

columnRefOrder
    : expression (asc=KW_ASC | desc=KW_DESC)? (KW_NULLS (first=KW_FIRST | last=KW_LAST))?
    ;

columnNameTypeConstraint
    : colName=identifier t = colType n+=constraints* commentClause?
    ;

columnNameType
    : colName=identifier t = colType commentClause?
    ;

partitionColumnNameType
    : colName=identifier t = colType n=nullableSpec? commentClause?
    ;

multipartIdentifier
    : parts+=identifier ('.' parts+=identifier)*
    ;

columnNameTypeConstraintWithPos
    : colName=multipartIdentifier t = colType n+=constraints* commentClause? alterStatementChangeColPosition?
    ;

changeColumnNameType
    : oldName=multipartIdentifier colName=multipartIdentifier t = colType n+=constraints* commentClause? alterStatementChangeColPosition?
    ;

constraints
    : n = nullableSpec
    | d = defaultValue
    | g = generatedColumnDesc
    | p = primaryKey
    ;

primaryKey
    : KW_PRIMARY KW_KEY?
    ;

nullableSpec
    :
    not=KW_NOT? KW_NULL
    ;

defaultValue
    :
    KW_DEFAULT constant
    ;

generatedColumnDesc
    :
    KW_GENERATED KW_ALWAYS KW_AS LPAREN expr=expression RPAREN KW_STORED
    ;


columnNameColonType
    : n=identifier COLON t=builtinTypeOrUdt (KW_COMMENT c=stringLiteral)?
    ;

colType
    : t=type
    ;

colTypeList
    : colType (COMMA colType)*
    ;

anyType
    : t=type | KW_ANY
    ;

// root grammar
anyTypeList
    : anyType (COMMA anyType)*
    ;

// root grammar
tableTypeInfo
    : table=KW_TABLE LPAREN cols=columnNameTypeConstraintList RPAREN (KW_PARTITIONED KW_BY LPAREN pars=columnNameTypeConstraintList RPAREN)?
    | view=KW_VIEW LPAREN cols=columnNameTypeConstraintList RPAREN KW_AS stmt=statement;

type
    : pt=primitiveType
    | blobty=blobType
    | geoty=geoType
    | lt=listType
    | st=structType
    | mt=mapType
    | ut=unionType
    | vt=vectorType
    | jt=jsonType;

primitiveType
    : name=KW_TINYINT
    | name=KW_SMALLINT
    | name=KW_INT
    | name=KW_BIGINT
    | name=KW_BOOLEAN
    | name=KW_FLOAT
    | name=KW_DOUBLE
    | name=KW_DATE
    | name=KW_DATETIME
    | name=KW_TIMESTAMP
    | name=KW_TIMESTAMP_NTZ
    | name=KW_INTERVAL_DAY_TIME
    | name=KW_INTERVAL_YEAR_MONTH
    | name=KW_STRING
    | name=KW_BINARY
    | name=KW_DECIMAL (LPAREN prec=Number (COMMA scale=Number)? RPAREN)?
    | name=KW_VARCHAR LPAREN length=Number RPAREN
    | name=KW_CHAR LPAREN length=Number RPAREN
    ;

builtinTypeOrUdt
    : t=type | cn=classNameOrArrayDecl
    ;

primitiveTypeOrUdt
    : t=primitiveType | cn=classNameOrArrayDecl
    ;

listType
    : KW_ARRAY LESSTHAN elemType=builtinTypeOrUdt GREATERTHAN
    ;

structType
    : KW_STRUCT LESSTHAN tl=columnNameColonTypeList GREATERTHAN
    ;

mapType
    : KW_MAP LESSTHAN left=primitiveTypeOrUdt COMMA right=builtinTypeOrUdt GREATERTHAN
    ;

jsonType
    : KW_JSON |
    KW_JSON LESSTHAN tl=columnNameColonTypeList GREATERTHAN
    ;

blobType
    : KW_BLOB
    ;

geoType
    : KW_GEOGRPHY
    ;

vectorType
    : KW_VECTOR LPAREN elemType=primitiveType COMMA dim=Number RPAREN
    ;

unionType
    : KW_UNIONTYPE LESSTHAN colTypeList GREATERTHAN
    ;

setOperator
    : union = KW_UNION all = KW_ALL
    | union = KW_UNION KW_DISTINCT?
    | intersect = KW_INTERSECT all = KW_ALL
    | intersect = KW_INTERSECT KW_DISTINCT?
    | minus = (KW_MINUS|KW_EXCEPT) all = KW_ALL
    | minus = (KW_MINUS|KW_EXCEPT) KW_DISTINCT?
    ;

withClause
  :
  KW_WITH KW_RECURSIVE? branches+=cteStatement (COMMA branches+=cteStatement)*
;

insertClause
   :
     KW_INSERT KW_OVERWRITE dest=destination ine=ifNotExists?
   | KW_INSERT KW_INTO KW_TABLE? intoTable=tableOrPartition (LPAREN targetCols=columnNameList RPAREN)?
   ;

destination
   :
     (local = KW_LOCAL)? KW_DIRECTORY simpleStringLiteral tableRowFormat? tableFileFormat?
   | KW_TABLE? table=tableOrPartition
   ;

//DELETE FROM <tableName> WHERE ...;
deleteStatement
   :
   KW_DELETE KW_FROM tp=tableOrPartition alias=tableAliasWithoutCols? (w=whereClause)?
   ;

/*SET <columName> = (3 + col2)*/
columnAssignmentClause
   :
   | tableAndColumnRef EQUAL expression //precedencePlusExpression
   | tableOrColumnRef EQUAL expression
   | (tableAndColumnRefListInParentheses | tableOrColumnRefListInParentheses) EQUAL (exp=expressionsInParenthese | subQuery=scalarSubQueryExpression)
   ;

tableAndColumnRefListInParentheses
    : LPAREN tableAndColumnRefList RPAREN
    ;

tableAndColumnRefList
    : tableAndColumnRef (COMMA tableAndColumnRef)*
    ;

tableOrColumnRefListInParentheses
    : LPAREN tableOrColumnRefList RPAREN
    ;

tableOrColumnRefList
    : tableOrColumnRef (COMMA tableOrColumnRef)*
    ;

/*SET col1 = 5, col2 = (4 + col4), ...*/
setColumnsClause
   :
   KW_SET columnAssignmentClause (COMMA columnAssignmentClause)*
   ;

/*
  UPDATE <table>
  SET col1 = val1, col2 = val2... WHERE ...
*/
updateStatement
   :
   KW_UPDATE tp=tableOrPartition alias=tableAliasWithoutCols? s=setColumnsClause f=fromClause? w=whereClause?
   ;

/*
  MERGE INTO <target table> AS T USING <source expression/table> AS S
  ON <boolean expression1>
  WHEN MATCHED [AND <boolean expression2>] THEN UPDATE SET [<set clause list> | *]
  WHEN MATCHED [AND <boolean expression3>] THEN DELETE
  WHEN NOT MATCHED [AND <boolean expression4>] THEN INSERT [(col1,col2 ...)] [VALUES<value list> | *]
*/
mergeStatement
    : KW_MERGE KW_INTO target=mergeTargetTable KW_USING source=mergeSourceTable KW_ON mergeOn=expression mergeAction+
    ;

mergeTargetTable
    : t=tableName (KW_AS? alias=identifier)?
    ;

mergeSourceTable
    : js=joinSource
    ;

mergeAction
    : KW_WHEN KW_NOT? KW_MATCHED (KW_BY (target=KW_TARGET | source=KW_SOURCE))? (KW_AND mergeAnd=expression)? KW_THEN
    (KW_INSERT values=mergeValuesCaluse
    | KW_UPDATE s=mergeSetColumnsClause
    | KW_DELETE)
    ;

mergeValuesCaluse
    : (LPAREN targetCols=mergeColumnRefList RPAREN)? KW_VALUES LPAREN cols+=mathExpression (COMMA cols+=mathExpression)* RPAREN
    | wildcardColumn
    | KW_ROW
    ;

mergeSetColumnsClause
   :
   KW_SET mergeColumnAssignmentClause (COMMA mergeColumnAssignmentClause)*
   ;

mergeColumnAssignmentClause
    : tableAndColumnRef EQUAL expression
    | tableOrColumnRef EQUAL expression
    | wildcardColumn
    ;

mergeColumnRef
    : tableAndColumnRef
    | tableOrColumnRef
    ;

mergeColumnRefList
    : mergeColumnRef (COMMA mergeColumnRef)*
    ;

temporalClause
    :
    tr=timeTravel
    |
    inc=incremental
    ;

/*
  SELECT * FROM t VERSION AS OF 1;
  SELECT * FROM t VERSION AS OF get_latest_version('projectName.schemaName.tableName', 3);
  SELECT * FROM t VERSION AS OF get_latest_version('projectName.schemaName.tableName');

  SELECT * FROM t TIMESTAMP AS OF '2019-01-29 00:37:58';
  SELECT * FROM t TIMESTAMP AS OF current_timestamp();
  SELECT * FROM t TIMESTAMP AS OF getdate();
  SELECT * FROM t TIMESTAMP AS OF get_latest_timestamp('projectName.schemaName.tableName');
   SELECT * FROM t TIMESTAMP AS OF get_latest_timestamp('projectName.schemaName.tableName', 3);
*/
timeTravel
    :
    (KW_VERSION | KW_TIMESTAMP) KW_AS KW_OF (v=expression)
    ;

/*
  SELECT * FROM t VERSION between 0 and 1;
  SELECT * FROM t VERSION between get_latest_version('projectName.schemaName.tableName', 2) and
  get_latest_version('projectName.schemaName.tableName');
  SELECT * FROM t VERSION between get_latest_version('projectName.schemaName.tableName', 3) and
  get_latest_version('projectName.schemaName.tableName', 2);

  SELECT * FROM t TIMESTAMP between '2019-01-29 00:37:58' and '2019-01-30 00:37:58';
  SELECT * FROM t TIMESTAMP between '2019-01-29 00:37:58' and current_timestamp();
  SELECT * FROM t TIMESTAMP between '2019-01-29 00:37:58' and getdate();
  SELECT * FROM t VERSION  TIMESTAMP between get_latest_timestamp('projectName.schemaName.tableName', 2) and
  get_latest_timestamp('projectName.schemaName.tableName');
  SELECT * FROM t VERSION  TIMESTAMP between get_latest_timestamp('projectName.schemaName.tableName', 2) and
  get_latest_timestamp('projectName.schemaName.tableName', 1);
*/
incremental
    :
    (KW_VERSION | KW_TIMESTAMP) KW_BETWEEN (min=expression) KW_AND (max=expression)
    ;

selectClause
    :
    KW_SELECT hintClause? (((KW_ALL | dist=KW_DISTINCT)? selectList)
                          | (transform=KW_TRANSFORM trfm=selectTrfmClause))
    | trfmc=trfmClause
    ;

selectList
    :
    selectItem ( COMMA  selectItem )*
    ;

selectTrfmClause
    :
    LPAREN exprs=selectExpressionList RPAREN
    inSerde=tableRowFormat? inRec=recordWriter
    KW_USING using=stringLiteral (KW_RESOURCES res+=stringLiteral (COMMA res+=stringLiteral)*)?
    ( KW_AS ((LPAREN (aliases=aliasList | cols=columnNameTypeList) RPAREN) | (aliases=aliasList | cols=columnNameTypeList)))?
    outSerde=tableRowFormat? outRec=recordReader
    ;

hintClause
    :
    HintStart hintList HintEnd
    ;

hintList
    :
    hintItem (COMMA hintItem)*
    ;

hintItem
    :
    mapjoin=mapJoinHint
    |
    conditionaljoin=conditionalJoinHint
    |
    skewjoin=skewJoinHint
    |
    rangejoin=rangeJoinHint
    |
    selectivity=selectivityHint
    |
    dynamicfilter=dynamicfilterHint
    |
    dist=distMapJoin
    |
    unionTable=unionTableHint
    |
    materializeHint
    |
    hintName (LPAREN hintArgs RPAREN)?
    ;

subqueryJoinHint :
    HintStart
    (KW_SUBQMAPJOIN | KW_MATERIALIZE)
    HintEnd;

unionTableHint :
  KW_UNIONTABLE;

materializeHint :
  KW_MATERIALIZE;

distMapJoin :
    KW_DISTMAPJOIN LPAREN distMapJoinHintArgs RPAREN
    ;

distMapJoinHintArgs
    :
    distMapJoinHintArgName (COMMA distMapJoinHintArgName)*
    ;

distMapJoinHintArgName
    :
    identifier (LPAREN distMapjoinPropertiesList RPAREN)?
    ;

distMapjoinPropertiesList
    :
      kv+=distMapjoinProperty (COMMA kv+=distMapjoinProperty)*
    ;

distMapjoinProperty
    :
      key=identifier EQUAL value=Number
    ;

dynamicfilterHint:
    KW_DYNAMICFILTER (LPAREN hintArgs RPAREN);

mapJoinHint:
    KW_MAPJOIN (LPAREN hintArgs RPAREN)?;

conditionalJoinHint:
    KW_CONDITIONALJOIN (LPAREN hintArgs RPAREN)?;

skewJoinHint:
    KW_SKEWJOIN (LPAREN multipleSkewHintArgs RPAREN)?;

selectivityHint:
    name=KW_SELECTIVITY (LPAREN num=Number RPAREN)?;

// /*+ rangejoin(t1,10) */
// /*+ rangejoin((t1,10),(t2,8)) */

rangeJoinHint:
    KW_RANGEJOIN (LPAREN? multipleRangeHintArg RPAREN?)?;

multipleRangeHintArg
    :
    multipleRangeHint+=rangeJoinHintArg ( COMMA multipleRangeHint+=rangeJoinHintArg )*;

rangeJoinHintArg
    :
    LPAREN table=identifier COMMA value=constant RPAREN;

//tableName1((123, 456), (789, 56)), tableName2((22, 33), (55, 66)),
multipleSkewHintArgs
    :
    multipleSkewHints+=skewJoinHintArgs ( COMMA multipleSkewHints+=skewJoinHintArgs )*;

//tableName(c1, c2)((123, 456), (789, 56))
// or   tableName(c1, c2)
skewJoinHintArgs
    :
    table=identifier ( columns=skewColumns ( LPAREN keyValues+=skewJoinHintKeyValues ( COMMA keyValues+=skewJoinHintKeyValues )* RPAREN )* )?;

// (c1, c2)
skewColumns
    : LPAREN skewKeys+=identifier ( COMMA skewKeys+=identifier )* RPAREN;

// (123, 456)
skewJoinHintKeyValues
    : LPAREN keyValue+=constant ( COMMA keyValue+=constant )* RPAREN;

hintName
    :
    KW_STREAMTABLE
    | KW_HOLD_DDLTIME
    | id=identifier
    ;

hintArgs
    :
    hintArgName (COMMA hintArgName)*
    ;

hintArgName
    :
    identifier
    ;

selectItem
    :
    wildcardCol = wildcardColumn
    |
    ( expression
      ((KW_AS? alias+=aliasIdentifier) | (KW_AS LPAREN alias+=aliasIdentifier (COMMA alias+=aliasIdentifier)* RPAREN))?
    )
    ;

trfmClause
    :
    (   KW_MAP    exprs=selectExpressionList
      | KW_REDUCE exprs=selectExpressionList )
    inSerde=tableRowFormat? inRec=recordWriter
    KW_USING using=stringLiteral (KW_RESOURCES res+=stringLiteral (COMMA res+=stringLiteral)*)?
    ( KW_AS ((LPAREN (aliases=aliasList | cols=columnNameTypeList) RPAREN) | (aliases=aliasList | cols=columnNameTypeList)))?
    outSerde=tableRowFormat? outRec=recordReader
    ;

selectExpression
    :
    wildcardCol=wildcardColumn
    |
    exp=expression
    ;

selectExpressionList
    :
    selectExpression (COMMA selectExpression)*
    ;

//---------------------- Rules for windowing clauses -------------------------------
windowClauseStatement
    : windowClause;

windowClause
    :
    KW_WINDOW winDef+=windowDefinition (COMMA winDef+=windowDefinition)*
    ;

windowDefinition
    :
    name=windowVariable KW_AS spec=windowSpecification
    ;

windowSpecification
    :
    ref=windowReference |
    LPAREN (
      ref=windowReference? p=partitioningSpec? w=window_frame?
    ) RPAREN
    ;

windowReference
    :
    windowVariable
    ;

windowVariable
    :
    identifier
    ;

window_frame
    :
    frameType=(KW_ROWS | KW_RANGE | KW_GROUPS) b=window_frame_boundary ex=frame_exclusion?
    |
    frameType=(KW_ROWS | KW_RANGE | KW_GROUPS) KW_BETWEEN
    s=window_frame_boundary KW_AND end=window_frame_boundary ex=frame_exclusion?
    ;

frame_exclusion
    :
    KW_EXCLUDE KW_CURRENT KW_ROW
    | KW_EXCLUDE KW_GROUP
    | KW_EXCLUDE KW_TIES
    | KW_EXCLUDE KW_NO KW_OTHERS
    ;

window_frame_boundary
    :
    KW_UNBOUNDED (d=KW_PRECEDING|d=KW_FOLLOWING) |
    KW_CURRENT KW_ROW  |
//  Number (d=KW_PRECEDING | d=KW_FOLLOWING )
    value=mathExpression/*precedenceBitwiseOrExpression*/ (d=KW_PRECEDING | d=KW_FOLLOWING )
    ;

wildcardColumn
    : (STAR | table=identifier DOT STAR)
    (KW_EXCEPT LPAREN exceptColNames+=aliasIdentifier (COMMA exceptColNames+=aliasIdentifier)* RPAREN)?
    (KW_REPLACE LPAREN
      exps+=expression KW_AS? replaceColNames+=aliasIdentifier (COMMA exps+=expression KW_AS? replaceColNames+=aliasIdentifier)*
      RPAREN)?
    ;

// (table|column)
tableOrColumn
    :
    identifier
    ;

tableAndColumnRef
    :
    t=identifier DOT c=identifier
    ;

expressionList
    :
    expression (COMMA expression)*
    ;

aliasList
    :
    ids+=aliasIdentifier (COMMA ids+=aliasIdentifier)*
    ;

//----------------------- Rules for parsing fromClause ------------------------------
// from [col1, col2, col3] table1, [col4, col5] table2
fromClause
    :
    KW_FROM hintClause? joinSource
    ;


// In MC sql, all the "join" clause, "lateral view" clause, "table sample" clause,
// even "on" clause will share the same precedence. Compiler will resolve it from left to right.
// so sql like "select * from a join b join c on a.x = b.x on a.x = c.x" will not work in MC sql.
// because "b join c on a.x = b.x" will not be resolved first.
// should change it to "select * from a join b on a.x = b.x join c on a.x = c.x".
joinSource
    : lhs=atomFromSource ( rhs += joinRHS )*
    ;

// As you can see, "using" clause and "on" clause in join is not relatively equal in grammar.
// In MC sql "a join b lateral view xxx(x) x as x on a.xx = b.xx" is legal,
// but "a join b lateral view xxx(x) x as x using xx" is illegal.
// "using" clause must be used near by join table, same constraint is appeared in "udj" clause
// udj can only used near by "on" clause which is near by join table.
joinRHS
    : joinType=joinToken KW_LATERAL? joinTable=atomFromSource
    (
        (KW_USING LPAREN commonCols+=identifier ( COMMA commonCols+=identifier)* RPAREN)
        |
        (KW_ON expression)
        // udj grammar
        (
            KW_USING cbn=functionIdentifier
            (LPAREN exprs=selectExpressionList RPAREN)?
            (tableAlias)?
            (
                (KW_AS? alias+=aliasIdentifier)
                |
                (KW_AS LPAREN alias+=aliasIdentifier (COMMA alias+=aliasIdentifier)* RPAREN)
            )?
            (KW_WITH KW_UDFPROPERTIES LPAREN userDefinedJoinPropertiesList RPAREN)?
            (sort=sortByClause)?
        )?
    )?
    | natural=KW_NATURAL joinType=joinToken joinTable=atomFromSource
    | (fss += fromSourceSuffix)+ (KW_ON expression)?
    | ts = tableSample (KW_ON expression)?
    ;

joinToken
    :
      KW_JOIN
    | KW_INNER KW_JOIN
    | COMMA
    | KW_CROSS KW_JOIN
    | KW_LEFT  (outer = KW_OUTER)? KW_JOIN
    | KW_RIGHT (outer = KW_OUTER)? KW_JOIN
    | KW_FULL  (outer = KW_OUTER)? KW_JOIN
    | KW_LEFT semi = KW_SEMI KW_JOIN
    | KW_LEFT anti = KW_ANTI KW_JOIN
    ;

lateralView
	:
	KW_LATERAL KW_VIEW outer=KW_OUTER function tableAlias (KW_AS identifier (COMMA identifier)*)?
	|
	KW_LATERAL KW_VIEW function tableAlias KW_AS identifier (COMMA identifier)*
	;

tableAlias
    :
    aliasIdentifier
    ;

tableBucketSample
    :
    KW_TABLESAMPLE LPAREN KW_BUCKET (numerator=Number) KW_OUT KW_OF (denominator=Number) (KW_ON expr+=expression (COMMA expr+=expression)*)? RPAREN (KW_AS? aliasIdentifier)?
    ;

splitSample
    :
    (
      KW_TABLESAMPLE LPAREN  (numerator=Number) (percent=KW_PERCENT|KW_ROWS) RPAREN
      |
      KW_TABLESAMPLE LPAREN  (numerator=ByteLengthLiteral) RPAREN
    ) (KW_AS? aliasIdentifier)?
    ;

tableSample
    :
    tableBucketSample |
    splitSample
    ;

pivotUnpivotColumnRef
    : tableAndColumnRef
    | tableOrColumnRef
    ;

pivotClause
    : KW_PIVOT LPAREN
        pivotAggregates
        KW_FOR
            pivotAxisColumns
        KW_IN
            LPAREN pivotValues RPAREN
    RPAREN
    (KW_AS? alias=aliasIdentifier)?
    ;

pivotAggregates
    : pai += pivotAggregateItem (COMMA pai+=pivotAggregateItem)*
    ;

pivotAggregateItem
    : expression (KW_AS? aliasIdentifier)?
    ;

pivotAxisColumns
    : pf += pivotUnpivotColumnRef
    | LPAREN pf += pivotUnpivotColumnRef (COMMA pf += pivotUnpivotColumnRef)* RPAREN
    ;

pivotValues
    : pv += pivotValue (COMMA pv += pivotValue)*
    ;

pivotValue
    : (
      exp += mathExpression
      | LPAREN exp += mathExpression (COMMA exp += mathExpression) RPAREN
    ) (KW_AS? aliasIdentifier)?
    ;

unpivotClause
    : KW_UNPIVOT (KW_INCLUDE KW_NULLS | KW_EXCLUDE KW_NULLS)?
    LPAREN
        unpivotMeasureColumns
        KW_FOR
            unpivotAxisColumns
        KW_IN
            LPAREN unpivotValues RPAREN
    RPAREN
    (KW_AS? alias=aliasIdentifier)?
    ;

unpivotMeasureColumns
    : umc += identifier
    | LPAREN umc += identifier (COMMA umc += identifier)* RPAREN
    ;

unpivotAxisColumns
    : uac += identifier
    | LPAREN uac += identifier (COMMA uac += identifier)* RPAREN
    ;

unpivotValues
    : uv += unpivotValue (COMMA uv += unpivotValue)*
    ;

unpivotValue
    : unpivotValueColumns (KW_AS? unpivotValueExpressions)?
    ;

unpivotValueColumns
    : pc += pivotUnpivotColumnRef
    | LPAREN pc += pivotUnpivotColumnRef (COMMA pc += pivotUnpivotColumnRef)* RPAREN
    ;

unpivotValueExpressions
    : exp += mathExpression
    | LPAREN exp += mathExpression (COMMA exp += mathExpression)* RPAREN
    ;

tableSource
    : tabname=tableName
    (props=tableProperties)?
    (KW_AS? (alias=identifierWithoutSql11 | extra=availableSql11KeywordsForOdpsTableAlias | l=doubleQuoteStringLiteral)
        (LPAREN col+=identifier (COMMA col+=identifier)* RPAREN)?)?
    (HintStart KW_SPLITSIZE LPAREN splitSize=Number RPAREN HintEnd)?
    (tc=temporalClause)?
    ;

// for compatibility, 'inner' and 'user' is avaliable as a table alias
availableSql11KeywordsForOdpsTableAlias
    :
    KW_USER
    | KW_INNER
    ;

tableName
    :
    // for compatibility, parse to db.table first, if resolve failed, try schema.table
    db=identifier DOT tab=identifier
    |
    tab=identifier
    |
    db=identifier DOT sch=identifier DOT tab=identifier
    ;

//---------------------- Rules for parsing PTF clauses -----------------------------
partitioningSpec
   :
   p = partitionByClause o = orderByClause? |
   o = orderByClause |
   d = distributeByClause s = sortByClause? |
   s = sortByClause |
   c = clusterByClause
   ;

partitionTableFunctionSource
   :
   subQuerySource |
   tableSource |
   partitionedTableFunction
   ;

partitionedTableFunction
   :
   name=Identifier LPAREN KW_ON
   (ptfsrc=partitionTableFunctionSource spec=partitioningSpec?)
   (Identifier LPAREN expression RPAREN ( COMMA Identifier LPAREN expression RPAREN)*)?
   RPAREN alias=Identifier?
   ;

//----------------------- Rules for parsing whereClause -----------------------------
// where a=b and ...
whereClause
    :
    KW_WHERE expression
    ;


//-----------------------------------------------------------------------------------

//-------- Row Constructor ----------------------------------------------------------
//in support of SELECT * FROM (VALUES(1,2,3),(4,5,6),...) as FOO(a,b,c) and
// INSERT INTO <table> (col1,col2,...) VALUES(...),(...),...
// INSERT INTO <table> (col1,col2,...) SELECT * FROM (VALUES(1,2,3),(4,5,6),...) as Foo(a,b,c)
valueRowConstructor
    :
    //LPAREN precedenceUnaryPrefixExpression (COMMA precedenceUnaryPrefixExpression)* RPAREN
    LPAREN cols+=mathExpression (COMMA cols+=mathExpression)* RPAREN
    ;

valuesTableConstructor
    :
    rows+=valueRowConstructor (COMMA rows+=valueRowConstructor)*
    ;

/*
VALUES(1),(2) means 2 rows, 1 column each.
VALUES(1,2),(3,4) means 2 rows, 2 columns each.
VALUES(1,2,3) means 1 row, 3 columns
*/
valuesClause
    :
    KW_VALUES values=valuesTableConstructor
    ;

/*
This represents a clause like this:
(VALUES(1,2),(2,3)) as VirtTable(col1,col2)
*/
virtualTableSource
    :
    LPAREN values=valuesClause RPAREN tableDecl=tableNameColList
    |
    values=valuesClause tableDecl=tableNameColList
    ;
/*
e.g. as VirtTable(col1,col2)
Note that we only want literals as column names
*/
tableNameColList
    :
    KW_AS? table=identifier LPAREN col+=identifier (COMMA col+=identifier)* RPAREN
    ;

functionTypeCubeOrRollup
    : (c=KW_CUBE | r=KW_ROLLUP) LPAREN gs+=groupingSetExpression ( COMMA gs+=groupingSetExpression)*  RPAREN
    ;

groupingSetsItem
    :
    functionTypeCubeOrRollup | groupingSetExpression
    ;

groupingSetsClause
    :
    sets = KW_GROUPING KW_SETS
    LPAREN groupingSetsItem ( COMMA groupingSetsItem)*  RPAREN
    ;


groupByKey
    :
    cr=functionTypeCubeOrRollup | gp=groupingSetsClause | exp=expression
    ;
// group by a,b
groupByClause
    :
    KW_GROUP KW_BY
    groupByKey (COMMA groupByKey)*
    ((rollup=KW_WITH KW_ROLLUP) | (cube=KW_WITH KW_CUBE) | (groupingset=groupingSetsClause)) ?
    ;

groupingSetExpression
   :
   groupingSetExpressionMultiple
   |
   groupingExpressionSingle
   ;

groupingSetExpressionMultiple
   :
   LPAREN
   expression? (COMMA expression)*
   RPAREN
   ;

groupingExpressionSingle
    :
    expression
    ;

havingClause
    :
    KW_HAVING havingCondition
    ;

havingCondition
    :
    expression
    ;

// ----qualify clause----
qualifyClause
    :
    KW_QUALIFY qualifyCondition
    ;

qualifyCondition
    :
    expression
    ;
// ----qualify clause----

expressionsInParenthese
    :
    LPAREN expressionsNotInParenthese RPAREN
    ;

expressionsNotInParenthese
    :
    expression (COMMA expression)*
    ;

columnRefOrderInParenthese
    :
    LPAREN columnRefOrderNotInParenthese RPAREN
    ;

columnRefOrderNotInParenthese
    :
    columnRefOrder (COMMA columnRefOrder)*
    ;

// order by a,b
orderByClause
    :
    KW_ORDER KW_BY exp+=columnRefOrder ( COMMA exp+=columnRefOrder)*
    ;

columnNameOrIndexInParenthese
    :
    LPAREN columnNameOrIndexNotInParenthese RPAREN
    ;

columnNameOrIndexNotInParenthese
    :
    col += columnNameOrIndex (COMMA col += columnNameOrIndex)*
    ;

columnNameOrIndex
    :
    tableAndColumnRef
    | tableOrColumnRef
    | index = constant
    ;

// zorder by a,b
zorderByClause
    :
    (isLocal=KW_LOCAL)? KW_ZORDER KW_BY
    (
    expsParen = columnNameOrIndexInParenthese
    |
    expsNoParen = columnNameOrIndexNotInParenthese
    )
    (sort=sortByClause)?
    ;

clusterByClause
    :
    KW_CLUSTER KW_BY
    (
    expsParen = expressionsInParenthese
    |
    expsNoParen = expressionsNotInParenthese
    )
    ;

partitionByClause
    :
    KW_PARTITION KW_BY
    (
    expsParen=expressionsInParenthese
    |
    expsNoParen=expressionsNotInParenthese
    )
    ;

distributeByClause
    :
    KW_DISTRIBUTE KW_BY
    (
    expsParen = expressionsInParenthese
    |
    expsNoParen = expressionsNotInParenthese
    )
    ;

sortByClause
    :
    KW_SORT KW_BY
    (
    expsParen=columnRefOrderInParenthese
    |
    expsNoParen=columnRefOrderNotInParenthese
    )
    ;

// fun(par1, par2, par3)
function
    :
    name=functionName
    lp = LPAREN
      (
        star = STAR
        | distinct = KW_DISTINCT? (arg+=functionArgument (COMMA arg+=functionArgument)*)?
      )
    RPAREN
    (
        (KW_WITHIN KW_GROUP LPAREN  obc = orderByClause (KW_LIMIT limit=mathExpression)? RPAREN)?
        (KW_FILTER LPAREN wc = whereClause RPAREN)?
        |
        (KW_FILTER LPAREN wc = whereClause RPAREN)?
        (KW_OVER ws=windowSpecification)?
    )
    |
    bfs=builtinFunctionStructure
    ;

functionArgument
    :
    s=expression
    |
    f=funNameRef
    |
    l=lambdaExpression
    ;

builtinFunctionStructure
    : KW_EXTRACT LPAREN u=intervalQualifiersUnit KW_FROM arg=expression RPAREN
    | KW_SUBSTRING LPAREN arg=expression KW_FROM st=mathExpression (KW_FOR end=mathExpression)? RPAREN
    | KW_TRIM LPAREN trimOption=(KW_BOTH | KW_LEADING | KW_TRAILING)? (trimStr=equalExpression)?
             KW_FROM srcStr=equalExpression RPAREN
    ;

functionName
    : // Keyword IF is also a function name
    kwIf = KW_IF
    | kwArray = KW_ARRAY
    | KW_MAP
    | KW_STRUCT
    | KW_UNIONTYPE
    | id = functionIdentifier
    | sql11ReservedId = sql11ReservedKeywordsUsedAsCastFunctionName
    ;

castExpression
    :
    KW_CAST
    LPAREN
          exp = expression
          KW_AS typeDecl=builtinTypeOrUdt
    RPAREN
    ;

caseExpression
    :
    KW_CASE caseExp = expression
    (KW_WHEN whenExp += expression KW_THEN thenExp += expression)+
    (KW_ELSE elseExp = expression)?
    KW_END
    ;

whenExpression
    :
    KW_CASE
     ( KW_WHEN whenExp += expression KW_THEN thenExp += expression)+
    (KW_ELSE elseExp = expression)?
    KW_END
    ;

constant
    :
    n=Number
    | date=dateLiteral
    | timestamp=timestampLiteral
    | datetime=dateTimeLiteral
    | json=jsonLiteral
    | i=intervalLiteral
    | s=stringLiteral
    | bi=BigintLiteral
    | si=SmallintLiteral
    | ti=TinyintLiteral
    | df=DecimalLiteral
    | doubleLiteral=DoubleLiteral
    | floatLiteral=FloatLiteral
    | hexLiteral=HexLiteral
    | cs=charSetStringLiteral
    | b=booleanValue
    ;

simpleStringLiteral
    :
    (StringLiteral | DoubleQuoteStringLiteral)
    ;
rawStringLiteral
  :
  BeginRawStringConstant Text* EndRawStringConstant ;


stringLiteral
    :
    simpleStringLiteral+
    |
    rawStringLiteral
    ;

doubleQuoteStringLiteral
    :
    DoubleQuoteStringLiteral
    ;

charSetStringLiteral
    :
    CharSetStringLiteral
    ;

dateLiteral
    :
    KW_DATE s=simpleStringLiteral
    |
    KW_CURRENT_DATE   // for compatible, CURRENT_DATE should be recognized as identifier
    ;

dateTimeLiteral
    :
    KW_DATETIME s = simpleStringLiteral
    ;

jsonLiteral
   :
   ty=jsonType s = simpleStringLiteral
   ;

timestampLiteral
    :
    KW_TIMESTAMP s=simpleStringLiteral
    |
    KW_TIMESTAMP_NTZ ntzstr=simpleStringLiteral
    |
    KW_CURRENT_TIMESTAMP
    |
    KW_LOCALTIMESTAMP
    ;

intervalLiteral
    :
    KW_INTERVAL v=stringLiteral q=intervalQualifiers
    | KW_INTERVAL e=mathExpression u=intervalQualifiersUnit
    ;

intervalQualifiers
    : y2m=KW_YEAR intervalQualifierPrecision? KW_TO KW_MONTH intervalQualifierPrecision?
    | d2h=KW_DAY intervalQualifierPrecision? KW_TO KW_HOUR intervalQualifierPrecision?
    | d2m=KW_DAY intervalQualifierPrecision? KW_TO KW_MINUTE intervalQualifierPrecision?
    | d2s=KW_DAY intervalQualifierPrecision? KW_TO KW_SECOND intervalQualifierPrecision?
    | h2m=KW_HOUR intervalQualifierPrecision? KW_TO KW_MINUTE intervalQualifierPrecision?
    | h2s=KW_HOUR intervalQualifierPrecision? KW_TO KW_SECOND intervalQualifierPrecision?
    | m2s=KW_MINUTE intervalQualifierPrecision? KW_TO KW_SECOND intervalQualifierPrecision?
    | u=intervalQualifiersUnit intervalQualifierPrecision?
    ;

intervalQualifiersUnit
    : y=KW_YEAR | y=KW_YEARS
    | M=KW_MONTH | M=KW_MONTHS
    | d=KW_DAY | d=KW_DAYS
    | h=KW_HOUR | h=KW_HOURS
    | m=KW_MINUTE | m=KW_MINUTES
    | s=KW_SECOND | s=KW_SECONDS
    | milli = KW_MILLISECOND
    | micro = KW_MICROSECOND
    ;

intervalQualifierPrecision
    : LPAREN Number RPAREN;

booleanValue
    :
    KW_TRUE | KW_FALSE
    ;

tableOrPartition
   :
   table=tableName partitions=partitionSpec?
   ;

partitionSpec
    :
    KW_PARTITION
     LPAREN partitionVal (COMMA  partitionVal )* RPAREN
    ;

partitionVal
    :
    identifier (EQUAL (constant | variableRef))?
    ;

// for compatibility
dateWithoutQuote:
    Number MINUS Number MINUS Number;

dropPartitionSpec
    :
    KW_PARTITION
     LPAREN dropVal+=expression (COMMA dropVal+=expression )* RPAREN
    ;

sysFuncNames
    :
      KW_AND
    | KW_OR
    | KW_NOT
    | KW_LIKE
    | KW_IF
    | KW_CASE
    | KW_WHEN
    | KW_TINYINT
    | KW_SMALLINT
    | KW_INT
    | KW_BIGINT
    | KW_FLOAT
    | KW_DOUBLE
    | KW_BOOLEAN
    | KW_STRING
    | KW_BINARY
    | KW_ARRAY
    | KW_MAP
    | KW_STRUCT
    | KW_UNIONTYPE
    | EQUAL
    | EQUAL_NS
    | NOTEQUAL
    | LESSTHANOREQUALTO
    | LESSTHAN
    | GREATERTHANOREQUALTO
    | GREATERTHAN
    | DIVIDE
    | PLUS
    | MINUS
    | STAR
    | MOD
    | KW_DIV
    | AMPERSAND
    | TILDE
    | BITWISEOR
    | BITWISEXOR
    | KW_RLIKE
    | KW_REGEXP
    | KW_IN
    | KW_BETWEEN
    ;

functionIdentifier
    : db=identifier (DOT|COLON) fn=identifier
    | db=identifier COLON s=identifier COLON fn=identifier
    | db=identifier DOT s=identifier DOT fn=identifier
    | builtin=COLON COLON fn=identifier
    | fn = identifier
    ;

reserved
    :
    KW_AND | KW_OR | KW_NOT | KW_LIKE | KW_IF | KW_HAVING | KW_FROM | KW_SELECT | KW_DISTINCT | KW_UNIQUEJOIN |
    KW_PRESERVE | KW_JOIN | KW_ON | KW_COLUMN  | KW_CHAR | KW_VARCHAR | KW_TABLESAMPLE |
    KW_CAST | KW_MACRO | KW_EXTENDED | KW_CASE | KW_WHEN | KW_THEN | KW_ELSE | KW_END | KW_CROSS | KW_UNBOUNDED | KW_PRECEDING |
    KW_FOLLOWING | KW_CURRENT | KW_PARTIALSCAN | KW_OVER | KW_WHERE
    ;

//the new version of nonReserved + sql11ReservedKeywordsUsedAsIdentifier = old version of nonReserved
nonReserved
    :
    KW_ADD | KW_ADMIN | KW_AFTER | KW_ANALYZE | KW_ARCHIVE | KW_ASC | KW_BEFORE | KW_BUCKET | KW_BUCKETS | KW_BUILD
    | KW_CASCADE | KW_CHANGE | KW_CLUSTER | KW_CLUSTERED | KW_CLUSTERSTATUS | KW_COLLECTION | KW_COLUMNS
    | KW_COMMENT | KW_COMPACT | KW_COMPACTIONS | KW_COMPUTE | KW_CONCATENATE | KW_CONTINUE | KW_DATA | KW_DAY
    | KW_DATABASES | KW_DATETIME | KW_DBPROPERTIES | KW_DEFERRED | KW_DEFINED | KW_DELIMITED | KW_DEPENDENCY
    | KW_DESC | KW_DIRECTORIES | KW_DIRECTORY | KW_DISABLE | KW_DISTRIBUTE | KW_ELEM_TYPE
    | KW_ENABLE | KW_ESCAPED | KW_EXCLUSIVE | KW_EXPLAIN | KW_EXPORT | KW_FIELDS | KW_FILE | KW_FILEFORMAT
    | KW_FIRST | KW_FORMAT | KW_FORMATTED | KW_FUNCTIONS | KW_HOLD_DDLTIME | KW_HOUR | KW_IDXPROPERTIES | KW_IGNORE
    | KW_INDEX | KW_INDEXES | KW_INPATH | KW_INPUTDRIVER | KW_INPUTFORMAT | KW_ITEMS | KW_IMMEDIATE | KW_JAR
    | KW_KEYS | KW_KEY_TYPE | KW_LIMIT | KW_LINES | KW_LOAD | KW_LOCATION | KW_LOCK | KW_LOCKS | KW_LOGICAL | KW_LONG
    | KW_MAPJOIN | KW_CONDITIONALJOIN | KW_SKEWJOIN | KW_RANGEJOIN | KW_MATERIALIZED | KW_METADATA | KW_MINUS | KW_MINUTE | KW_MONTH | KW_MSCK | KW_NOSCAN | KW_NO_DROP | KW_OFFLINE
    | KW_OPTION | KW_OUTPUTDRIVER | KW_OUTPUTFORMAT | KW_OVERWRITE | KW_OWNER | KW_PARTITIONED | KW_PARTITIONS | KW_PLUS | KW_PRETTY
    | KW_PRINCIPALS | KW_PROTECTION | KW_PURGE | KW_READ | KW_READONLY | KW_REBUILD | KW_RECORDREADER | KW_RECORDWRITER
    | KW_REGEXP | KW_RELOAD | KW_RENAME | KW_REPAIR | KW_REPLACE | KW_REPLICATION | KW_RESTRICT | KW_REWRITE | KW_RLIKE
    | KW_ROLE | KW_ROLES | KW_SCHEMA | KW_SCHEMAS | KW_SECOND | KW_SEMI | KW_SERDE | KW_SERDEPROPERTIES | KW_SERVER | KW_SETS | KW_SHARED
    | KW_SHOW | KW_SHOW_DATABASE | KW_SKEWED | KW_SORT | KW_SORTED | KW_SSL | KW_STATISTICS | KW_STORED | KW_BUILTIN
    | KW_STREAMTABLE | KW_STRING | KW_STRUCT | KW_TABLES | KW_TBLPROPERTIES | KW_TEMPORARY | KW_TERMINATED | KW_SEPARATED
    | KW_TINYINT | KW_TOUCH | KW_TRANSACTIONS | KW_UNARCHIVE | KW_UNDO | KW_UNIONTYPE | KW_UNLOCK | KW_UNSET
    | KW_UNSIGNED | KW_URI | KW_USE | KW_UTC | KW_UTCTIMESTAMP | KW_VALUE_TYPE | KW_VIEW | KW_WHILE | KW_YEAR
    | KW_MAP | KW_REDUCE | KW_SEMI | KW_ANTI | KW_CUBE | KW_PRIMARY | KW_KEY | KW_VALIDATE | KW_NOVALIDATE | KW_RELY | KW_NORELY
    | KW_OUTPUT | KW_YEARS | KW_MONTHS | KW_DAYS | KW_HOURS | KW_MINUTES | KW_SECONDS | KW_MATCHED
    | KW_CODE_BEGIN | KW_CODE_END
    | KW_HISTORY | KW_RESTORE | KW_LSN
    | KW_ZORDER
    | KW_SQL
    | KW_MODEL | KW_PROPERTIES
    | KW_OPTIONS | KW_SERVERS | KW_FOREIGN | KW_WRAPPER | KW_CALL
    | KW_UNLOAD
    | KW_WITHIN | KW_FILTER | KW_WITHOUT
    | KW_EXCLUDE | KW_TIES | KW_NO | KW_OTHERS
    | KW_TRANSFORM
    | KW_TRIM | KW_LEADING | KW_TRAILING
    | KW_OBJECTS | KW_OBJECT
    | KW_JSON
    | KW_BLOB
    | KW_GEOGRPHY
    | KW_UNPIVOT | KW_INCLUDE | KW_PIVOT | KW_QUALIFY
    | KW_SAFE
    | KW_EMBEDDED
    | KW_VERSION
    | KW_RECURSIVE
    | KW_TIMESTAMP_NTZ
    | KW_INTERVAL_DAY_TIME
    | KW_INTERVAL_YEAR_MONTH
    | KW_TASK | KW_TASKS | KW_EXECUTE | KW_MODIFY | KW_SCHEDULE | KW_TASKPROPERTIES | KW_CDC
    | KW_STREAM | KW_STREAMS | KW_STRMPROPERTIES
    | KW_MASKING | KW_APPLY | KW_BIND | KW_UNBIND
    | KW_START
    | KW_SUBQMAPJOIN
    | KW_MATERIALIZE
    | KW_AUTO
    | KW_RECLUSTER | KW_PARTIAL
    | KW_USERDEFINE
    | KW_MILLISECOND | KW_MICROSECOND
    | KW_USERGROUP | KW_USERGROUPS
    | KW_SNAPSHOT | KW_SYSTEMTIME
    | KW_UNNEST
    | KW_TARGET | KW_SOURCE
    | KW_VERSIONS | KW_MODELS | KW_DEFAULT_VERSION | KW_INPUT | KW_INFERENCEPARAMETERS
    | KW_CONNECTION | KW_CONNECTIONS
    | KW_XCOPY | KW_CURRENT
    | KW_VECTOR
    ;

//The following SQL2011 reserved keywords are used as cast function name only, it is a subset of the sql11ReservedKeywordsUsedAsIdentifier.
sql11ReservedKeywordsUsedAsCastFunctionName
    :
    KW_BIGINT | KW_BINARY | KW_BOOLEAN | KW_CURRENT_DATE | KW_CURRENT_TIMESTAMP | KW_DATE | KW_DOUBLE | KW_FLOAT | KW_INT | KW_SMALLINT | KW_TIMESTAMP
    ;

//The following SQL2011 reserved keywords are used as identifiers in many q tests, they may be added back due to backward compatibility.
sql11ReservedKeywordsUsedAsIdentifier
    :
    KW_ALL | KW_ALTER | KW_ARRAY | KW_AS | KW_AUTHORIZATION | KW_BETWEEN | KW_BIGINT | KW_BINARY | KW_BOOLEAN
    | KW_BOTH | KW_BY | KW_CREATE | KW_CURRENT_DATE | KW_CURRENT_TIMESTAMP | KW_CURSOR | KW_DATE | KW_DECIMAL | KW_DELETE | KW_DESCRIBE
    | KW_DOUBLE | KW_DROP | KW_EXISTS | KW_EXTERNAL | KW_FALSE | KW_FETCH | KW_FLOAT | KW_FOR | KW_FULL | KW_GRANT
    | KW_GROUP | KW_GROUPING | KW_IMPORT | KW_IN | KW_INNER | KW_INSERT | KW_INT | KW_INTERSECT | KW_INTO | KW_IS | KW_LATERAL
    | KW_LEFT | KW_LIKE | KW_LOCAL | KW_NONE | KW_NULL | KW_OF | KW_ORDER | KW_OUT | KW_OUTER | KW_PARTITION
    | KW_PERCENT | KW_PROCEDURE | KW_RANGE | KW_READS | KW_REVOKE | KW_RIGHT
    | KW_ROLLUP | KW_ROW | KW_ROWS | KW_SET | KW_SMALLINT | KW_TABLE | KW_TIMESTAMP | KW_TO | KW_TRIGGER | KW_TRUE
    | KW_TRUNCATE | KW_UNION | KW_UPDATE | KW_USER | KW_USING | KW_VALUES | KW_WITH | KW_WINDOW | KW_NATURAL
    ;
