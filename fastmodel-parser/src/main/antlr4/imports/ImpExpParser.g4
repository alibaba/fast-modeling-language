parser grammar ImpExpParser;

impExpStatements:
   exportStatement
;


exportStatement
: (KW_EXP | KW_EXPORT) exportBusinessUnit? exportOutput exportTarget? KW_WHERE expression
;


exportOutput
: KW_OUTPUT EQUAL string
;

exportBusinessUnit
: identifier
;

exportTarget
: KW_TARGET EQUAL string
;
