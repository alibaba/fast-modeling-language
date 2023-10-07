parser grammar DomainParser;


domainStatements:
    createDomainStatement
    | setDomainComment
    | setDomainProperties
    | unSetDomainProperties
    | renameDomain
    | dropDomainStatement
    | setDomainAliasedName
    ;

createDomainStatement
    :KW_CREATE replace? KW_DOMAIN ifNotExists?
     qualifiedName alias? comment?
     (KW_WITH setProperties)?
    ;


setDomainComment
    :KW_ALTER KW_DOMAIN qualifiedName alterStatementSuffixSetComment
    ;


 setDomainAliasedName
    : KW_ALTER KW_DOMAIN qualifiedName setAliasedName
    ;

setDomainProperties
    :KW_ALTER KW_DOMAIN qualifiedName KW_SET setProperties
    ;

unSetDomainProperties
    : KW_ALTER KW_DOMAIN qualifiedName KW_UNSET unSetProperties
    ;

renameDomain
        : KW_ALTER KW_DOMAIN qualifiedName alterStatementSuffixRename
        ;

dropDomainStatement
    : KW_DROP KW_DOMAIN qualifiedName
    ;
