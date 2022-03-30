parser grammar QueryParser;

queryOrInsertStatements:
    query
    | insertInto
    | delete
;

query
    :  with? queryNoWith
    ;


insertInto
    : KW_INSERT (KW_INTO | KW_OVERWRITE) tableName
    partitionSpec? columnParenthesesList? query
    ;


delete:
        KW_DELETE deleteType? KW_FROM qualifiedName KW_WHERE expression
    ;

deleteType :
        KW_TABLE
        | indicatorType? KW_INDICATOR
    ;


with
    : KW_WITH KW_RECURSIVE? namedQuery (COMMA namedQuery)*
    ;

queryNoWith:
      queryTerm
      (KW_ORDER KW_BY sortItem (COMMA sortItem)*)?
      (KW_OFFSET offset=rowCount (KW_ROW | KW_ROWS)?)?
      ((KW_LIMIT limit=limitRowCount) | (KW_FETCH (KW_FIRST | KW_NEXT) (fetchFirst=rowCount)? (KW_ROW | KW_ROWS) (KW_ONLY | KW_WITH KW_TIES)))?
      ;
limitRowCount
    : KW_ALL
    | rowCount
    ;

rowCount
    : INTEGER_VALUE
    | '?'
    ;

queryTerm
    : queryPrimary                                                             #queryTermDefault
    | left=queryTerm operator=KW_INTERSECT setQuantifier? right=queryTerm         #setOperation
    | left=queryTerm operator=(KW_UNION | KW_EXCEPT) setQuantifier? right=queryTerm  #setOperation
    ;

queryPrimary
    : querySpecification                   #queryPrimaryDefault
    | KW_TABLE qualifiedName                  #table
    | KW_VALUES expression (COMMA expression)*  #inlineTable
    | LPAREN queryNoWith  RPAREN                 #subquery
    ;

sortItem
    : expression ordering=(KW_ASC | KW_DESC)? (KW_NULLS nullOrdering=(KW_FIRST | KW_LAST))?
    ;

querySpecification
    : KW_SELECT (hints=hint)? setQuantifier? selectItem (',' selectItem)*
      (KW_FROM relation (COMMA relation)*)?
      (KW_WHERE where=expression)?
      (KW_GROUP KW_BY groupBy)?
      (KW_HAVING having=expression)?
    ;
hint
    : HINT_START hintStatements+=hintStatement (COMMA? hintStatements+=hintStatement)* HINT_END
    ;

hintStatement
    : hintName=identifier
    | hintName=identifier LPAREN parameters+=expression (COMMA parameters+=expression)* RPAREN
    ;

groupBy
    : setQuantifier? groupingElement (COMMA groupingElement)*
    ;

groupingElement
    : groupingSet                                            #singleGroupingSet
    | KW_ROLLUP LPAREN (expression (COMMA expression)*)? RPAREN         #rollup
    | KW_CUBE LPAREN (expression (COMMA expression)*)? RPAREN           #cube
    | KW_GROUPING KW_SETS LPAREN groupingSet (COMMA groupingSet)* RPAREN   #multipleGroupingSets
    ;

groupingSet
    : LPAREN (expression (COMMA expression)*)? RPAREN
    | expression
    ;

namedQuery
    : name=identifier (columnAliases)? KW_AS LPAREN query RPAREN
    ;

setQuantifier
    : KW_DISTINCT
    | KW_ALL
    ;

selectItem
    : expression (KW_AS? identifier)?                          #selectSingle
    | atomExpression DOT STAR (KW_AS columnAliases)?       #selectAll
    | STAR                                              #selectAll
    ;

relation
    : left=relation
      ( KW_CROSS KW_JOIN right=sampledRelation
      | joinType KW_JOIN rightRelation=relation joinCriteria
      | KW_NATURAL joinType KW_JOIN right=sampledRelation
      )                                           #joinRelation
    | sampledRelation                             #relationDefault
    ;

joinType
    : KW_INNER?
    | KW_LEFT KW_OUTER?
    | KW_RIGHT KW_OUTER?
    | KW_FULL KW_OUTER?
    ;

joinCriteria
    : KW_ON expression
    | KW_USING LPAREN identifier (COMMA identifier)* RPAREN
    ;

sampledRelation
    : aliasedRelation (
        KW_TABLESAMPLE sampleType LPAREN percentage=expression RPAREN
      )?
    ;

sampleType
    : KW_BERNOULLI
    | KW_SYSTEM
    ;

aliasedRelation
    : relationPrimary (KW_AS? identifierWithoutSql11 columnAliases?)?
    ;

columnAliases
    : LPAREN identifier (COMMA identifier)* RPAREN
    ;

relationPrimary
    : qualifiedName                                                   #tableQualifiedName
    | LPAREN query RPAREN                                                   #subqueryRelation
    | KW_UNNEST LPAREN expression (COMMA expression)* RPAREN (KW_WITH KW_ORDINALITY)?  #unnest
    | KW_LATERAL LPAREN query RPAREN                                           #lateral
    | LPAREN relation RPAREN                                                #parenthesizedRelation
    ;

