parser grammar CommandParser;

commandStatements :
      commonSqlCommandStatement
      | helpCommandStatement
      ;

commonSqlCommandStatement:
   commands dialectOptions commandOptions  (KW_WITH setProperties)?
;

helpCommandStatement :
   KW_HELP type=identifier (TEXT_OPTION string)? (KW_WITH setProperties)?
;



dialectOptions:
     MODE_OPTION identifier
    ;
commandOptions
    : TEXT_OPTION string
    | URI_OPTION string
    ;

commands :
    type=KW_IMPORT_SQL
    | type=KW_EXPORT_SQL
    | type=KW_RENDER
    | type=KW_FORMAT
;



