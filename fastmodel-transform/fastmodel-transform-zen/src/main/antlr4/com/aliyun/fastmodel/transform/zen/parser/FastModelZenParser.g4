parser grammar FastModelZenParser;

tokens {
    DELIMITER
}

options
{
  tokenVocab= FastModelZenLexer;
}

zencoding
   : expression
   ;

expression
   : atomicExpression  #atomic
   | left=expression splitChar right=expression #brother
   ;

splitChar
   : NEW_LINE | PLUS | COMMA | BITWISEOR
   ;

atomicExpression
   : identifier
   | identifier STAR INTEGER_VALUE
   | atomicExpression splitAttrChar (identifier | string)
   ;
splitAttrChar
    : DOT
    | TAB*
    | BLANK*
    ;
identifier
    : Identifier
    ;

string
    : StringLiteral
    ;
