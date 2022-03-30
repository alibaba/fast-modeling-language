/**
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

   @author wyruweso
   based on the Hive 3.1.2 grammar by Canwei He
*/
parser grammar HiveParser;


options
{
  tokenVocab=HiveLexer;
}

// starting rule
statements
   : (statement statementSeparator | empty_)* EOF
   ;

statementSeparator
   : SEMICOLON
   ;

empty_
   : statementSeparator
   ;

statement
	: explainStatement
	| execStatement
	;

explainStatement
	: KW_EXPLAIN (
	    explainOption* execStatement
        | KW_REWRITE queryStatementExpression
      )
	;

explainOption
    : KW_EXTENDED
    | KW_FORMATTED
    | KW_DEPENDENCY
    | KW_LOGICAL
    | KW_AUTHORIZATION
    | KW_ANALYZE
    | KW_REOPTIMIZATION
    | (KW_VECTORIZATION vectorizationOnly? vectorizatonDetail?)
    ;

vectorizationOnly
    : KW_ONLY
    ;

vectorizatonDetail
    : KW_SUMMARY
    | KW_OPERATOR
    | KW_EXPRESSION
    | KW_DETAIL
    ;

execStatement
    : queryStatementExpression
    | loadStatement
    | exportStatement
    | importStatement
    | replDumpStatement
    | replLoadStatement
    | replStatusStatement
    | ddlStatement
    | deleteStatement
    | updateStatement
    | sqlTransactionStatement
    | mergeStatement
    ;

loadStatement
    : KW_LOAD KW_DATA KW_LOCAL? KW_INPATH StringLiteral KW_OVERWRITE? KW_INTO KW_TABLE tableOrPartition inputFileFormat?
    ;

replicationClause
    : KW_FOR KW_METADATA? KW_REPLICATION LPAREN StringLiteral RPAREN
    ;

exportStatement
    : KW_EXPORT
      KW_TABLE tableOrPartition
      KW_TO StringLiteral
      replicationClause?
    ;

importStatement
       : KW_IMPORT
         (KW_EXTERNAL? KW_TABLE tableOrPartition)?
         KW_FROM (path=StringLiteral)
         tableLocation?
    ;

replDumpStatement
      : KW_REPL KW_DUMP
        identifier (DOT identifier)?
        (KW_FROM Number
          (KW_TO Number)?
          (KW_LIMIT Number)?
        )?
        (KW_WITH replConfigs)?
    ;

replLoadStatement
      : KW_REPL KW_LOAD
        (identifier (DOT identifier)?)?
        KW_FROM StringLiteral
        (KW_WITH replConfigs)?
      ;

replConfigs
    : LPAREN replConfigsList RPAREN
    ;

replConfigsList
    : keyValueProperty (COMMA keyValueProperty)*
    ;

replStatusStatement
      : KW_REPL KW_STATUS
        identifier (DOT identifier)?
        (KW_WITH replConfigs)?
      ;

ddlStatement
    : createDatabaseStatement
    | switchDatabaseStatement
    | dropDatabaseStatement
    | createTableStatement
    | dropTableStatement
    | truncateTableStatement
    | alterStatement
    | descStatement
    | showStatement
    | metastoreCheck
    | createViewStatement
    | createMaterializedViewStatement
    | dropViewStatement
    | dropMaterializedViewStatement
    | createFunctionStatement
    | createMacroStatement
    | dropFunctionStatement
    | reloadFunctionStatement
    | dropMacroStatement
    | analyzeStatement
    | lockStatement
    | unlockStatement
    | lockDatabase
    | unlockDatabase
    | createRoleStatement
    | dropRoleStatement
    | grantPrivileges
    | revokePrivileges
    | showGrants
    | showRoleGrants
    | showRolePrincipals
    | showRoles
    | grantRole
    | revokeRole
    | setRole
    | showCurrentRole
    | abortTransactionStatement
    | killQueryStatement
    | createIndexStatement
    | dropIndexStatement
    ;

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

createDatabaseStatement
    : KW_CREATE (KW_DATABASE|KW_SCHEMA)
        ifNotExists?
        identifier
        databaseComment?
        dbLocation?
        (KW_WITH KW_DBPROPERTIES dbProperties)?
    ;

dbLocation
    :
      KW_LOCATION StringLiteral
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
    : KW_USE identifier
    ;

dropDatabaseStatement
    : KW_DROP (KW_DATABASE|KW_SCHEMA) ifExists? identifier restrictOrCascade?
    ;

databaseComment

    : KW_COMMENT StringLiteral
    ;

createTableStatement
    : KW_CREATE KW_TEMPORARY? KW_EXTERNAL? KW_TABLE ifNotExists? tableName
      (  KW_LIKE tableName
         tableRowFormat?
         tableFileFormat?
         tableLocation?
         tablePropertiesPrefixed?
       | (LPAREN columnNameTypeOrConstraintList RPAREN)?
         tableComment?
         tablePartition?
         tableBuckets?
         tableSkewed?
         tableRowFormat?
         tableFileFormat?
         tableLocation?
         tablePropertiesPrefixed?
         (KW_AS selectStatementWithCTE)?
      )
    ;

truncateTableStatement
    : KW_TRUNCATE KW_TABLE tablePartitionPrefix (KW_COLUMNS LPAREN columnNameList RPAREN)?;

dropTableStatement
    : KW_DROP KW_TABLE ifExists? tableName KW_PURGE? replicationClause?
    ;

alterStatement
    : KW_ALTER KW_TABLE tableName alterTableStatementSuffix
    | KW_ALTER KW_VIEW tableName KW_AS? alterViewStatementSuffix
    | KW_ALTER KW_MATERIALIZED KW_VIEW tableName alterMaterializedViewStatementSuffix
    | KW_ALTER (KW_DATABASE|KW_SCHEMA) alterDatabaseStatementSuffix
    | KW_ALTER KW_INDEX alterIndexStatementSuffix
    ;

alterTableStatementSuffix
    : alterStatementSuffixRename
    | alterStatementSuffixDropPartitions
    | alterStatementSuffixAddPartitions
    | alterStatementSuffixTouch
    | alterStatementSuffixArchive
    | alterStatementSuffixUnArchive
    | alterStatementSuffixProperties
    | alterStatementSuffixSkewedby
    | alterStatementSuffixExchangePartition
    | alterStatementPartitionKeyType
    | alterStatementSuffixDropConstraint
    | alterStatementSuffixAddConstraint
    | partitionSpec? alterTblPartitionStatementSuffix
    | alterStatementSuffixSetOwner
    ;

alterTblPartitionStatementSuffix
  : alterStatementSuffixFileFormat
  | alterStatementSuffixLocation
  | alterStatementSuffixMergeFiles
  | alterStatementSuffixSerdeProperties
  | alterStatementSuffixRenamePart
  | alterStatementSuffixBucketNum
  | alterTblPartitionStatementSuffixSkewedLocation
  | alterStatementSuffixClusterbySortby
  | alterStatementSuffixCompact
  | alterStatementSuffixUpdateStatsCol
  | alterStatementSuffixUpdateStats
  | alterStatementSuffixRenameCol
  | alterStatementSuffixAddCol
  ;

alterStatementPartitionKeyType
	: KW_PARTITION KW_COLUMN LPAREN columnNameType RPAREN
	;

alterViewStatementSuffix
    : alterViewSuffixProperties
    | alterStatementSuffixRename
    | alterStatementSuffixAddPartitions
    | alterStatementSuffixDropPartitions
    | selectStatementWithCTE
    ;

alterMaterializedViewStatementSuffix
    : alterMaterializedViewSuffixRewrite
    | alterMaterializedViewSuffixRebuild
    ;

alterDatabaseStatementSuffix
    : alterDatabaseSuffixProperties
    | alterDatabaseSuffixSetOwner
    | alterDatabaseSuffixSetLocation
    ;

alterDatabaseSuffixProperties
    : identifier KW_SET KW_DBPROPERTIES dbProperties
    ;

alterDatabaseSuffixSetOwner
    : identifier KW_SET KW_OWNER principalName
    ;

alterDatabaseSuffixSetLocation
    : identifier KW_SET KW_LOCATION StringLiteral
    ;

alterStatementSuffixRename
    : KW_RENAME KW_TO tableName
    ;

alterStatementSuffixAddCol
    : (KW_ADD | KW_REPLACE) KW_COLUMNS LPAREN columnNameTypeList RPAREN restrictOrCascade?
    ;

alterStatementSuffixAddConstraint
   :  KW_ADD (alterForeignKeyWithName | alterConstraintWithName)
   ;

alterStatementSuffixDropConstraint
   : KW_DROP KW_CONSTRAINT identifier
   ;

alterStatementSuffixRenameCol
    : KW_CHANGE KW_COLUMN? identifier identifier colType alterColumnConstraint? (KW_COMMENT StringLiteral)? alterStatementChangeColPosition? restrictOrCascade?
    ;

alterStatementSuffixUpdateStatsCol
    : KW_UPDATE KW_STATISTICS KW_FOR KW_COLUMN? identifier KW_SET tableProperties (KW_COMMENT StringLiteral)?
    ;

alterStatementSuffixUpdateStats
    : KW_UPDATE KW_STATISTICS KW_SET tableProperties
    ;

alterStatementChangeColPosition
    : first=KW_FIRST|KW_AFTER identifier
    ;

alterStatementSuffixAddPartitions
    : KW_ADD ifNotExists? alterStatementSuffixAddPartitionsElement+
    ;

alterStatementSuffixAddPartitionsElement
    : partitionSpec partitionLocation?
    ;

alterStatementSuffixTouch
    : KW_TOUCH partitionSpec*
    ;

alterStatementSuffixArchive
    : KW_ARCHIVE partitionSpec*
    ;

alterStatementSuffixUnArchive
    : KW_UNARCHIVE partitionSpec*
    ;

partitionLocation
    : KW_LOCATION StringLiteral
    ;

alterStatementSuffixDropPartitions
    : KW_DROP ifExists? dropPartitionSpec (COMMA dropPartitionSpec)* KW_PURGE? replicationClause?
    ;

alterStatementSuffixProperties
    : KW_SET KW_TBLPROPERTIES tableProperties
    | KW_UNSET KW_TBLPROPERTIES ifExists? tableProperties
    ;

alterViewSuffixProperties
    : KW_SET KW_TBLPROPERTIES tableProperties
    | KW_UNSET KW_TBLPROPERTIES ifExists? tableProperties
    ;

alterMaterializedViewSuffixRewrite
    : (rewriteEnabled | rewriteDisabled)
    ;

alterMaterializedViewSuffixRebuild
    : KW_REBUILD
    ;

alterStatementSuffixSerdeProperties
    : KW_SET KW_SERDE StringLiteral (KW_WITH KW_SERDEPROPERTIES tableProperties)?
    | KW_SET KW_SERDEPROPERTIES tableProperties
    ;

alterIndexStatementSuffix
    : identifier KW_ON tableName
    partitionSpec?
    KW_REBUILD;

alterStatementSuffixFileFormat
	: KW_SET KW_FILEFORMAT fileFormat
	;

alterStatementSuffixClusterbySortby
  : KW_NOT KW_CLUSTERED
  | KW_NOT KW_SORTED
  | tableBuckets
  ;

alterTblPartitionStatementSuffixSkewedLocation
  : KW_SET KW_SKEWED KW_LOCATION skewedLocations
  ;

skewedLocations
    : LPAREN skewedLocationsList RPAREN
    ;

skewedLocationsList
    : skewedLocationMap (COMMA skewedLocationMap)*
    ;

skewedLocationMap
    : skewedValueLocationElement EQUAL StringLiteral
    ;

alterStatementSuffixLocation
  : KW_SET KW_LOCATION StringLiteral
  ;


alterStatementSuffixSkewedby
	: tableSkewed
	| KW_NOT KW_SKEWED
	| KW_NOT storedAsDirs
	;

alterStatementSuffixExchangePartition
    : KW_EXCHANGE partitionSpec KW_WITH KW_TABLE tableName
    ;

alterStatementSuffixRenamePart
    : KW_RENAME KW_TO partitionSpec
    ;

alterStatementSuffixStatsPart
    : KW_UPDATE KW_STATISTICS KW_FOR KW_COLUMN? identifier KW_SET tableProperties (KW_COMMENT StringLiteral)?
    ;

alterStatementSuffixMergeFiles
    : KW_CONCATENATE
    ;

alterStatementSuffixBucketNum
    : KW_INTO Number KW_BUCKETS
    ;

createIndexStatement
    : KW_CREATE KW_INDEX identifier KW_ON KW_TABLE tableName columnParenthesesList KW_AS StringLiteral
    (KW_WITH KW_DEFERRED KW_REBUILD)?
    (KW_IDXPROPERTIES tableProperties)?
    (KW_IN KW_TABLE tableName)?
    (KW_PARTITIONED KW_BY columnParenthesesList)?
    (tableRowFormat? tableFileFormat)?
    (KW_LOCATION locationPath)?
    tablePropertiesPrefixed?
    tableComment?;

locationPath
    : identifier (DOT identifier)*
    ;

dropIndexStatement
    : KW_DROP KW_INDEX identifier KW_ON tableName;

tablePartitionPrefix
  : tableName partitionSpec?
  ;

blocking
  : KW_AND KW_WAIT
  ;

alterStatementSuffixCompact
    : KW_COMPACT StringLiteral blocking? (KW_WITH KW_OVERWRITE KW_TBLPROPERTIES tableProperties)?
    ;

alterStatementSuffixSetOwner
    : KW_SET KW_OWNER principalName
    ;

fileFormat
    : KW_INPUTFORMAT StringLiteral KW_OUTPUTFORMAT StringLiteral KW_SERDE StringLiteral (KW_INPUTDRIVER StringLiteral KW_OUTPUTDRIVER StringLiteral)?
    | identifier
    ;

inputFileFormat
    : KW_INPUTFORMAT StringLiteral KW_SERDE StringLiteral
    ;

tabTypeExpr
   : identifier (DOT identifier)?
   ( identifier (DOT
   ( KW_ELEM_TYPE
   | KW_KEY_TYPE
   | KW_VALUE_TYPE
   | identifier )
   )*
   )?
   ;

partTypeExpr
    : tabTypeExpr partitionSpec?
    ;

tabPartColTypeExpr
    : tableName partitionSpec? extColumnName?
    ;

descStatement
    : (KW_DESCRIBE|KW_DESC)
    (
    (KW_DATABASE|KW_SCHEMA) KW_EXTENDED? identifier
    | KW_FUNCTION KW_EXTENDED? descFuncNames
    | ((KW_FORMATTED|KW_EXTENDED) tabPartColTypeExpr)
    | tabPartColTypeExpr
    )
    ;

analyzeStatement
    : KW_ANALYZE KW_TABLE (tableOrPartition)
      ( KW_COMPUTE KW_STATISTICS (KW_NOSCAN | (KW_FOR KW_COLUMNS columnNameList?))?
      | KW_CACHE KW_METADATA
      )
    ;

showStatement
    : KW_SHOW (KW_DATABASES|KW_SCHEMAS) (KW_LIKE showStmtIdentifier)?
    | KW_SHOW KW_TABLES ((KW_FROM|KW_IN) identifier)? (KW_LIKE showStmtIdentifier|showStmtIdentifier)?
    | KW_SHOW KW_VIEWS ((KW_FROM|KW_IN) identifier)? (KW_LIKE showStmtIdentifier|showStmtIdentifier)?
    | KW_SHOW KW_MATERIALIZED KW_VIEWS ((KW_FROM|KW_IN) identifier)? (KW_LIKE showStmtIdentifier|showStmtIdentifier)?
    | KW_SHOW KW_COLUMNS (KW_FROM|KW_IN) tableName ((KW_FROM|KW_IN) identifier)? (KW_LIKE showStmtIdentifier|showStmtIdentifier)?
    | KW_SHOW KW_FUNCTIONS (KW_LIKE showFunctionIdentifier|showFunctionIdentifier)?
    | KW_SHOW KW_PARTITIONS tableName partitionSpec?
    | KW_SHOW KW_CREATE (
        (KW_DATABASE|KW_SCHEMA) identifier
        |
        KW_TABLE tableName
      )
    | KW_SHOW KW_TABLE KW_EXTENDED ((KW_FROM|KW_IN) identifier)? KW_LIKE showStmtIdentifier partitionSpec?
    | KW_SHOW KW_TBLPROPERTIES tableName (LPAREN StringLiteral RPAREN)?
    | KW_SHOW KW_LOCKS
      (
      (KW_DATABASE|KW_SCHEMA) identifier KW_EXTENDED?
      |
      partTypeExpr? KW_EXTENDED?
      )
    | KW_SHOW KW_COMPACTIONS
    | KW_SHOW KW_TRANSACTIONS
    | KW_SHOW KW_CONF StringLiteral
    | KW_SHOW KW_RESOURCE
      (
        (KW_PLAN identifier)
        | KW_PLANS
      )
    ;

lockStatement
    : KW_LOCK KW_TABLE tableName partitionSpec? lockMode
    ;

lockDatabase
    : KW_LOCK (KW_DATABASE|KW_SCHEMA) identifier lockMode
    ;

lockMode
    : KW_SHARED | KW_EXCLUSIVE
    ;

unlockStatement
    : KW_UNLOCK KW_TABLE tableName partitionSpec?
    ;

unlockDatabase
    : KW_UNLOCK (KW_DATABASE|KW_SCHEMA) identifier
    ;

createRoleStatement
    : KW_CREATE KW_ROLE identifier
    ;

dropRoleStatement
    : KW_DROP KW_ROLE identifier
    ;

grantPrivileges
    : KW_GRANT privilegeList
      privilegeObject?
      KW_TO principalSpecification
      withGrantOption?
    ;

revokePrivileges
    : KW_REVOKE grantOptionFor? privilegeList privilegeObject? KW_FROM principalSpecification
    ;

grantRole
    : KW_GRANT KW_ROLE? identifier (COMMA identifier)* KW_TO principalSpecification withAdminOption?
    ;

revokeRole
    : KW_REVOKE adminOptionFor? KW_ROLE? identifier (COMMA identifier)* KW_FROM principalSpecification
    ;

showRoleGrants
    : KW_SHOW KW_ROLE KW_GRANT principalName
    ;


showRoles
    : KW_SHOW KW_ROLES
    ;

showCurrentRole
    : KW_SHOW KW_CURRENT KW_ROLES
    ;

setRole
    : KW_SET KW_ROLE
    (
    KW_ALL
    |
    KW_NONE
    |
    identifier
    )
    ;

showGrants
    : KW_SHOW KW_GRANT principalName? (KW_ON privilegeIncludeColObject)?
    ;

showRolePrincipals
    : KW_SHOW KW_PRINCIPALS identifier
    ;


privilegeIncludeColObject
    : KW_ALL
    | privObjectCols
    ;

privilegeObject
    : KW_ON privObject
    ;

// database or table type. Type is optional, default type is table
privObject
    : (KW_DATABASE|KW_SCHEMA) identifier
    | KW_TABLE? tableName partitionSpec?
    | KW_URI StringLiteral
    | KW_SERVER identifier
    ;

privObjectCols
    : (KW_DATABASE|KW_SCHEMA) identifier
    | KW_TABLE? tableName (LPAREN columnNameList RPAREN)? partitionSpec?
    | KW_URI StringLiteral
    | KW_SERVER identifier
    ;

privilegeList
    : privlegeDef (COMMA privlegeDef)*
    ;

privlegeDef
    : privilegeType (LPAREN columnNameList RPAREN)?
    ;

privilegeType
    : KW_ALL
    | KW_ALTER
    | KW_UPDATE
    | KW_CREATE
    | KW_DROP
    | KW_LOCK
    | KW_SELECT
    | KW_SHOW_DATABASE
    | KW_INSERT
    | KW_DELETE
    ;

principalSpecification
    : principalName (COMMA principalName)*
    ;

principalName
    : KW_USER principalIdentifier
    | KW_GROUP principalIdentifier
    | KW_ROLE identifier
    ;

withGrantOption
    : KW_WITH KW_GRANT KW_OPTION
    ;

grantOptionFor
    : KW_GRANT KW_OPTION KW_FOR
;

adminOptionFor
    : KW_ADMIN KW_OPTION KW_FOR
;

withAdminOption
    : KW_WITH KW_ADMIN KW_OPTION
    ;

metastoreCheck
    : KW_MSCK KW_REPAIR?
      (KW_TABLE tableName
        ((KW_ADD | KW_DROP | KW_SYNC) KW_PARTITIONS)? |
        partitionSpec?)
    ;

resourceList
  :
  resource (COMMA resource)*
  ;

resource
  : resourceType StringLiteral
  ;

resourceType
  : KW_JAR
  | KW_FILE
  | KW_ARCHIVE
  ;

createFunctionStatement
    : KW_CREATE KW_TEMPORARY? KW_FUNCTION functionIdentifier KW_AS StringLiteral
      (KW_USING resourceList)?
    ;

dropFunctionStatement
    : KW_DROP KW_TEMPORARY? KW_FUNCTION ifExists? functionIdentifier
    ;

reloadFunctionStatement
    : KW_RELOAD KW_FUNCTION
    ;

createMacroStatement
    : KW_CREATE KW_TEMPORARY KW_MACRO Identifier
      LPAREN columnNameTypeList? RPAREN expression
    ;

dropMacroStatement
    : KW_DROP KW_TEMPORARY KW_MACRO ifExists? Identifier
    ;

createViewStatement
    : KW_CREATE orReplace? KW_VIEW ifNotExists? tableName
        (LPAREN columnNameCommentList RPAREN)? tableComment? viewPartition?
        tablePropertiesPrefixed?
        KW_AS
        selectStatementWithCTE
    ;

createMaterializedViewStatement
    : KW_CREATE KW_MATERIALIZED KW_VIEW ifNotExists? tableName
        rewriteDisabled? tableComment? tableRowFormat? tableFileFormat? tableLocation?
        tablePropertiesPrefixed? KW_AS selectStatementWithCTE
    ;

viewPartition
    : KW_PARTITIONED KW_ON LPAREN columnNameList RPAREN
    ;

dropViewStatement
    : KW_DROP KW_VIEW ifExists? viewName
    ;

dropMaterializedViewStatement
    : KW_DROP KW_MATERIALIZED KW_VIEW ifExists? viewName
    ;

showFunctionIdentifier
    : functionIdentifier
    | StringLiteral
    ;

showStmtIdentifier
    : identifier
    | StringLiteral
    ;

tableComment
    : KW_COMMENT StringLiteral
    ;

tablePartition
    : KW_PARTITIONED KW_BY LPAREN columnNameTypeConstraint (COMMA columnNameTypeConstraint)* RPAREN
    ;

tableBuckets
    : KW_CLUSTERED KW_BY LPAREN columnNameList RPAREN (KW_SORTED KW_BY LPAREN columnNameOrderList RPAREN)? KW_INTO Number KW_BUCKETS
    ;

tableSkewed
    : KW_SKEWED KW_BY LPAREN columnNameList RPAREN KW_ON LPAREN skewedValueElement RPAREN storedAsDirs?
    ;

rowFormat
    : rowFormatSerde
    | rowFormatDelimited
    ;

recordReader
    : KW_RECORDREADER StringLiteral
    ;

recordWriter
    : KW_RECORDWRITER StringLiteral
    ;

rowFormatSerde
    : KW_ROW KW_FORMAT KW_SERDE StringLiteral (KW_WITH KW_SERDEPROPERTIES tableProperties)?
    ;

rowFormatDelimited
    : KW_ROW KW_FORMAT KW_DELIMITED tableRowFormatFieldIdentifier? tableRowFormatCollItemsIdentifier? tableRowFormatMapKeysIdentifier? tableRowFormatLinesIdentifier? tableRowNullFormat?
    ;

tableRowFormat
    : rowFormatDelimited
    | rowFormatSerde
    ;

tablePropertiesPrefixed
    : KW_TBLPROPERTIES tableProperties
    ;

tableProperties
    : LPAREN tablePropertiesList RPAREN
    ;

tablePropertiesList
    : keyValueProperty (COMMA keyValueProperty)*
    | keyProperty (COMMA keyProperty)*
    ;

keyValueProperty
    : StringLiteral EQUAL StringLiteral
    ;

keyProperty
    : StringLiteral
    ;

tableRowFormatFieldIdentifier
    : KW_FIELDS KW_TERMINATED KW_BY StringLiteral (KW_ESCAPED KW_BY StringLiteral)?
    ;

tableRowFormatCollItemsIdentifier
    : KW_COLLECTION KW_ITEMS KW_TERMINATED KW_BY StringLiteral
    ;

tableRowFormatMapKeysIdentifier
    : KW_MAP KW_KEYS KW_TERMINATED KW_BY StringLiteral
    ;

tableRowFormatLinesIdentifier
    : KW_LINES KW_TERMINATED KW_BY StringLiteral
    ;

tableRowNullFormat
    : KW_NULL KW_DEFINED KW_AS StringLiteral
    ;
tableFileFormat
    : KW_STORED KW_AS KW_INPUTFORMAT StringLiteral KW_OUTPUTFORMAT StringLiteral (KW_INPUTDRIVER StringLiteral KW_OUTPUTDRIVER StringLiteral)?
      | KW_STORED KW_BY StringLiteral
         (KW_WITH KW_SERDEPROPERTIES tableProperties)?
      | KW_STORED KW_AS identifier
    ;

tableLocation
    : KW_LOCATION StringLiteral
    ;

columnNameTypeList
    : columnNameType (COMMA columnNameType)*
    ;

columnNameTypeOrConstraintList
    : columnNameTypeOrConstraint (COMMA columnNameTypeOrConstraint)*
    ;

columnNameColonTypeList
    : columnNameColonType (COMMA columnNameColonType)*
    ;

columnNameList
    : columnName (COMMA columnName)*
    ;

columnName
    : identifier
    ;

extColumnName
    : identifier (DOT (KW_ELEM_TYPE | KW_KEY_TYPE | KW_VALUE_TYPE | identifier))*
    ;

columnNameOrderList
    : columnNameOrder (COMMA columnNameOrder)*
    ;

columnParenthesesList
    : LPAREN columnNameList RPAREN
    ;

enableValidateSpecification
    : enableSpecification validateSpecification?
    | enforcedSpecification
    ;

enableSpecification
    : KW_ENABLE
    | KW_DISABLE
    ;

validateSpecification
    : KW_VALIDATE
    | KW_NOVALIDATE
    ;

enforcedSpecification
    : KW_ENFORCED
    | KW_NOT KW_ENFORCED
    ;

relySpecification
    :  KW_RELY
    |  (KW_NORELY)?
    ;

createConstraint
    : (KW_CONSTRAINT identifier)? pkConstraint constraintOptsCreate?
    ;

alterConstraintWithName
    : KW_CONSTRAINT identifier pkConstraint constraintOptsAlter?
    ;

pkConstraint
    : tableConstraintPrimaryKey pkCols=columnParenthesesList
    ;

createForeignKey
    : (KW_CONSTRAINT identifier)? KW_FOREIGN KW_KEY left=columnParenthesesList  KW_REFERENCES tableName right=columnParenthesesList constraintOptsCreate?
    ;

alterForeignKeyWithName
    : KW_CONSTRAINT identifier KW_FOREIGN KW_KEY columnParenthesesList  KW_REFERENCES tableName columnParenthesesList constraintOptsAlter?
    ;

skewedValueElement
    : skewedColumnValues
    | skewedColumnValuePairList
    ;

skewedColumnValuePairList
    : skewedColumnValuePair (COMMA skewedColumnValuePair)*
    ;

skewedColumnValuePair
    : LPAREN skewedColumnValues RPAREN
    ;

skewedColumnValues
    : skewedColumnValue (COMMA skewedColumnValue)*
    ;

skewedColumnValue
    : constant
    ;

skewedValueLocationElement
    : skewedColumnValue
    | skewedColumnValuePair
    ;

orderSpecification
    : KW_ASC
    | KW_DESC
    ;

nullOrdering
    : KW_NULLS KW_FIRST
    | KW_NULLS KW_LAST
    ;

columnNameOrder
    : identifier orderSpecification? nullOrdering?
    ;

columnNameCommentList
    : columnNameComment (COMMA columnNameComment)*
    ;

columnNameComment
    : identifier (KW_COMMENT StringLiteral)?
    ;

columnRefOrder
    : expression orderSpecification? nullOrdering?
    ;

columnNameType
    : identifier colType (KW_COMMENT StringLiteral)?
    ;

columnNameTypeOrConstraint
    : ( tableConstraint )
    | ( columnNameTypeConstraint )
    ;

tableConstraint
    : ( createForeignKey )
    | ( createConstraint )
    ;

columnNameTypeConstraint
    : identifier colType columnConstraint? (KW_COMMENT StringLiteral)?
    ;

columnConstraint
    : ( foreignKeyConstraint )
    | ( colConstraint )
    ;

foreignKeyConstraint
    : (KW_CONSTRAINT identifier)? KW_REFERENCES tableName LPAREN columnName RPAREN constraintOptsCreate?
    ;

colConstraint
    : (KW_CONSTRAINT identifier)? tableConstraintPrimaryKey constraintOptsCreate?
    ;

alterColumnConstraint
    : ( alterForeignKeyConstraint )
    | ( alterColConstraint )
    ;

alterForeignKeyConstraint
    : (KW_CONSTRAINT identifier)? KW_REFERENCES tableName LPAREN columnName RPAREN constraintOptsAlter?
    ;

alterColConstraint
    : (KW_CONSTRAINT identifier)? tableConstraintPrimaryKey constraintOptsAlter?
    ;

tableConstraintPrimaryKey
    : KW_PRIMARY KW_KEY
    ;

constraintOptsCreate
    : enableValidateSpecification relySpecification
    ;

constraintOptsAlter
    : enableValidateSpecification relySpecification
    ;

columnNameColonType
    : identifier COLON colType (KW_COMMENT StringLiteral)?
    ;

colType
    : type_db_col
    ;

colTypeList
    : colType (COMMA colType)*
    ;

type_db_col
    : primitiveType
    | listType
    | structType
    | mapType
    | unionType
    ;

primitiveType
    : name=KW_TINYINT #genericType
    | name=KW_SMALLINT #genericType
    | name=KW_INT #genericType
    | name=KW_BIGINT #genericType
    | name=KW_BOOLEAN #genericType
    | name=KW_FLOAT #genericType
    | name=KW_DOUBLE KW_PRECISION? #genericType
    | name=KW_DATE #genericType
    | name=KW_DATETIME #genericType
    | name=KW_TIMESTAMP #genericType
    | name=KW_TIMESTAMPLOCALTZ #genericType
    | name=KW_TIMESTAMP KW_WITH KW_LOCAL KW_TIME KW_ZONE #genericType
    | name=KW_STRING #genericType
    | name=KW_BINARY #genericType
    | name=KW_DECIMAL (LPAREN typeParameter (COMMA typeParameter)? RPAREN)? #genericType
    | name=KW_VARCHAR LPAREN typeParameter RPAREN #genericType
    | name=KW_CHAR LPAREN typeParameter RPAREN #genericType
    ;

 typeParameter
    : Number
    ;

listType
    : KW_ARRAY LESSTHAN type_db_col GREATERTHAN
    ;

structType
    : KW_STRUCT LESSTHAN columnNameColonTypeList GREATERTHAN
    ;

mapType
    : KW_MAP LESSTHAN key=primitiveType COMMA value=type_db_col GREATERTHAN
    ;

unionType
    : KW_UNIONTYPE LESSTHAN colTypeList GREATERTHAN
    ;

setOperator
    : KW_UNION KW_ALL
    | KW_UNION KW_DISTINCT?
    | KW_INTERSECT KW_ALL
    | KW_INTERSECT KW_DISTINCT?
    | KW_EXCEPT KW_ALL
    | KW_EXCEPT KW_DISTINCT?
    | KW_MINUS KW_ALL
    | KW_MINUS KW_DISTINCT?
    ;

queryStatementExpression
    :
    /* Would be nice to do this as a gated semantic perdicate
       But the predicate gets pushed as a lookahead decision.
       Calling rule doesnot know about topLevel
    */
    withClause?
    queryStatementExpressionBody
    ;

queryStatementExpressionBody
    : fromStatement
    | regularBody
    ;

withClause
    : KW_WITH cteStatement (COMMA cteStatement)*
    ;

cteStatement
    : identifier KW_AS LPAREN queryStatementExpression RPAREN
    ;

fromStatement
    : (singleFromStatement)
	(setOperator singleFromStatement)*
	;


singleFromStatement
    : fromClause body+
    ;

/*
The valuesClause rule below ensures that the parse tree for
"insert into table FOO values (1,2),(3,4)" looks the same as
"insert into table FOO select a,b from (values(1,2),(3,4)) as BAR(a,b)" which itself is made to look
very similar to the tree for "insert into table FOO select a,b from BAR".
*/
regularBody
    : insertClause ( selectStatement | valuesClause)
    | selectStatement
    ;

atomSelectStatement
    : selectClause
    fromClause?
    whereClause?
    groupByClause?
    havingClause?
    window_clause?
    | LPAREN selectStatement RPAREN
    ;

selectStatement
    : atomSelectStatement setOpSelectStatement?  orderByClause?  clusterByClause?  distributeByClause?  sortByClause?  limitClause?
    ;

setOpSelectStatement
    :
    (setOperator atomSelectStatement)+
    ;

selectStatementWithCTE
    : withClause? selectStatement
    ;

body
    : insertClause
    selectClause
    lateralView?
    whereClause?
    groupByClause?
    havingClause?
    window_clause?
    orderByClause?
    clusterByClause?
    distributeByClause?
    sortByClause?
    limitClause?
    |
    selectClause
    lateralView?
    whereClause?
    groupByClause?
    havingClause?
    window_clause?
    orderByClause?
    clusterByClause?
    distributeByClause?
    sortByClause?
    limitClause?
    ;

insertClause
    : KW_INSERT KW_OVERWRITE destination ifNotExists?
    | KW_INSERT KW_INTO KW_TABLE? tableOrPartition (LPAREN columnNameList RPAREN)?
    ;

destination
   : KW_LOCAL? KW_DIRECTORY StringLiteral tableRowFormat? tableFileFormat?
   | KW_TABLE tableOrPartition
   ;

limitClause
   : KW_LIMIT ((Number COMMA)? Number)
   | KW_LIMIT Number KW_OFFSET Number
   ;

//DELETE FROM <tableName> WHERE ...;
deleteStatement
   : KW_DELETE KW_FROM tableName whereClause?
   ;

/*SET <columName> = (3 + col2)*/
columnAssignmentClause
   : tableOrColumn EQUAL expression
   ;

/*SET col1 = 5, col2 = (4 + col4), ...*/
setColumnsClause
   : KW_SET columnAssignmentClause (COMMA columnAssignmentClause)*
   ;

/*
  UPDATE <table>
  SET col1 = val1, col2 = val2... WHERE ...
*/
updateStatement
   : KW_UPDATE tableName setColumnsClause whereClause?
   ;

/*
BEGIN user defined transaction boundaries; follows SQL 2003 standard exactly except for addition of
"setAutoCommitStatement" which is not in the standard doc but is supported by most SQL engines.
*/
sqlTransactionStatement
  : startTransactionStatement
  | commitStatement
  | rollbackStatement
  | setAutoCommitStatement
  ;

startTransactionStatement
  : KW_START KW_TRANSACTION ( transactionMode  ( COMMA transactionMode )* )?
  ;

transactionMode
  : isolationLevel
  | transactionAccessMode
  ;

transactionAccessMode
  : KW_READ KW_ONLY
  | KW_READ KW_WRITE
  ;

isolationLevel
  : KW_ISOLATION KW_LEVEL levelOfIsolation
  ;

/*READ UNCOMMITTED | READ COMMITTED | REPEATABLE READ | SERIALIZABLE may be supported later*/
levelOfIsolation
  : KW_SNAPSHOT
  ;

commitStatement
  : KW_COMMIT KW_WORK?
  ;

rollbackStatement
  : KW_ROLLBACK KW_WORK?
  ;
setAutoCommitStatement
  : KW_SET KW_AUTOCOMMIT booleanValueTok
  ;
/*
END user defined transaction boundaries
*/

abortTransactionStatement
  : KW_ABORT KW_TRANSACTIONS Number+
  ;


/*
BEGIN SQL Merge statement
*/
mergeStatement
   : KW_MERGE KW_INTO tableName (KW_AS? identifier)? KW_USING joinSourcePart KW_ON expression whenClauses
   ;
/*
Allow 0,1 or 2 WHEN MATCHED clauses and 0 or 1 WHEN NOT MATCHED
Each WHEN clause may have AND <boolean predicate>.
If 2 WHEN MATCHED clauses are present, 1 must be UPDATE the other DELETE and the 1st one
must have AND <boolean predicate>
*/
whenClauses
   : (whenMatchedAndClause|whenMatchedThenClause)* whenNotMatchedClause?
   ;
whenNotMatchedClause
   : KW_WHEN KW_NOT KW_MATCHED (KW_AND expression)? KW_THEN KW_INSERT KW_VALUES valueRowConstructor
   ;
whenMatchedAndClause
   : KW_WHEN KW_MATCHED KW_AND expression KW_THEN updateOrDelete
   ;
whenMatchedThenClause
   : KW_WHEN KW_MATCHED KW_THEN updateOrDelete
   ;
updateOrDelete
   : KW_UPDATE setColumnsClause
   | KW_DELETE
   ;
/*
END SQL Merge statement
*/

killQueryStatement
  : KW_KILL KW_QUERY StringLiteral+
  ;
/**
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

   @author Canwei He
*/

//----------------------- Rules for parsing selectClause -----------------------------
// select a,b,c ...
selectClause
    : KW_SELECT QUERY_HINT? (((KW_ALL | KW_DISTINCT)? selectList)
                          | (KW_TRANSFORM selectTrfmClause))
    | trfmClause
    ;

selectList
    : selectItem ( COMMA  selectItem )*
    ;

selectTrfmClause
    : LPAREN selectExpressionList RPAREN
    rowFormat? recordWriter?
    KW_USING StringLiteral
    ( KW_AS ((LPAREN (aliasList | columnNameTypeList) RPAREN) | (aliasList | columnNameTypeList)))?
    rowFormat? recordReader?
    ;

selectItem
    : tableAllColumns
    | ( expression
      ((KW_AS? identifier) | (KW_AS LPAREN identifier (COMMA identifier)* RPAREN))?
    )
    ;

trfmClause
    : (   KW_MAP    selectExpressionList
      | KW_REDUCE selectExpressionList )
    rowFormat? recordWriter?
    KW_USING StringLiteral
    ( KW_AS ((LPAREN (aliasList | columnNameTypeList) RPAREN) | (aliasList | columnNameTypeList)))?
    rowFormat? recordReader?
    ;

selectExpression
    : tableAllColumns
    | expression
    ;

selectExpressionList
    : selectExpression (COMMA selectExpression)*
    ;

//---------------------- Rules for windowing clauses -------------------------------
window_clause
    :
    KW_WINDOW window_defn (COMMA window_defn)*
    ;

window_defn
    :
    identifier KW_AS window_specification
    ;

window_specification
    :
    (identifier | ( LPAREN identifier? partitioningSpec? window_frame? RPAREN))
    ;

window_frame
    : window_range_expression
    | window_value_expression
    ;

window_range_expression
    :
    KW_ROWS window_frame_start_boundary
    | KW_ROWS KW_BETWEEN window_frame_boundary KW_AND window_frame_boundary
    ;

window_value_expression
    : KW_RANGE window_frame_start_boundary
    | KW_RANGE KW_BETWEEN window_frame_boundary KW_AND window_frame_boundary
    ;

window_frame_start_boundary
    :
    KW_UNBOUNDED KW_PRECEDING
    | KW_CURRENT KW_ROW
    | Number KW_PRECEDING
    ;

    window_frame_boundary
    :
    KW_UNBOUNDED (KW_PRECEDING|KW_FOLLOWING)
    | KW_CURRENT KW_ROW
    | Number (KW_PRECEDING | KW_FOLLOWING )
    ;
/**
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

   @author Canwei He
*/


//-----------------------------------------------------------------------------------

tableAllColumns
    : STAR
    | tableName DOT STAR
    ;

// (table|column)
tableOrColumn
    : identifier
    ;

expressionList
    : expression (COMMA expression)*
    ;

aliasList
    : identifier (COMMA identifier)*
    ;

//----------------------- Rules for parsing fromClause ------------------------------
// from [col1, col2, col3] table1, [col4, col5] table2
fromClause
    : KW_FROM fromSource
    ;

fromSource
    : uniqueJoinToken uniqueJoinSource (COMMA uniqueJoinSource)+
    | joinSource
    ;


atomjoinSource
    : tableSource lateralView*
    | virtualTableSource lateralView*
    | subQuerySource lateralView*
    | partitionedTableFunction lateralView*
    | LPAREN joinSource RPAREN
    ;

joinSource
    : atomjoinSource (joinToken joinSourcePart (KW_ON expression | KW_USING columnParenthesesList)?)*
    ;

joinSourcePart
    : (tableSource | virtualTableSource | subQuerySource | partitionedTableFunction) lateralView*
    ;

uniqueJoinSource
    : KW_PRESERVE? uniqueJoinTableSource uniqueJoinExpr
    ;

uniqueJoinExpr
    : LPAREN expressionList RPAREN
    ;

uniqueJoinToken
    : KW_UNIQUEJOIN
    ;

joinToken
    : KW_JOIN
    | KW_INNER KW_JOIN
    | COMMA
    | KW_CROSS KW_JOIN
    | KW_LEFT  KW_OUTER? KW_JOIN
    | KW_RIGHT KW_OUTER? KW_JOIN
    | KW_FULL  KW_OUTER? KW_JOIN
    | KW_LEFT KW_SEMI KW_JOIN
    ;

lateralView
	: KW_LATERAL KW_VIEW KW_OUTER function_ tableAlias (KW_AS identifier (COMMA identifier)*)?
	| COMMA? KW_LATERAL KW_VIEW function_ tableAlias (KW_AS identifier (COMMA identifier)*)?
    | COMMA? KW_LATERAL KW_TABLE LPAREN valuesClause RPAREN KW_AS? tableAlias (LPAREN identifier (COMMA identifier)* RPAREN)?
	;

tableAlias
    : identifier
    ;

tableBucketSample
    : KW_TABLESAMPLE LPAREN KW_BUCKET Number KW_OUT KW_OF Number (KW_ON expression (COMMA expression)*)? RPAREN
    ;

splitSample
    : KW_TABLESAMPLE LPAREN  Number (KW_PERCENT|KW_ROWS) RPAREN
    | KW_TABLESAMPLE LPAREN  ByteLengthLiteral RPAREN
    ;

tableSample
    : tableBucketSample
    | splitSample
    ;

tableSource
    : tableName tableProperties? tableSample? (KW_AS? identifier)?
    ;

uniqueJoinTableSource
    : tableName tableSample? (KW_AS? identifier)?
    ;

tableName
    : identifier DOT identifier
    | identifier
    ;

viewName
    : (identifier DOT)? identifier
    ;

subQuerySource
    : LPAREN queryStatementExpression RPAREN KW_AS? identifier
    ;

//---------------------- Rules for parsing PTF clauses -----------------------------
partitioningSpec
   : partitionByClause orderByClause?
   | orderByClause
   | distributeByClause sortByClause?
   | sortByClause
   | clusterByClause
   ;

partitionTableFunctionSource
   : subQuerySource
   | tableSource
   | partitionedTableFunction
   ;

partitionedTableFunction
   : identifier LPAREN KW_ON
   partitionTableFunctionSource partitioningSpec?
   (Identifier LPAREN expression RPAREN ( COMMA Identifier LPAREN expression RPAREN)*)?
   RPAREN identifier?
   ;

//----------------------- Rules for parsing whereClause -----------------------------
// where a=b and ...
whereClause
    : KW_WHERE searchCondition
    ;

searchCondition
    : expression
    ;

//-----------------------------------------------------------------------------------

//-------- Row Constructor ----------------------------------------------------------
//in support of SELECT * FROM (VALUES(1,2,3),(4,5,6),...) as FOO(a,b,c) and
// INSERT INTO <table> (col1,col2,...) VALUES(...),(...),...
// INSERT INTO <table> (col1,col2,...) SELECT * FROM (VALUES(1,2,3),(4,5,6),...) as Foo(a,b,c)
/*
VALUES(1),(2) means 2 rows, 1 column each.
VALUES(1,2),(3,4) means 2 rows, 2 columns each.
VALUES(1,2,3) means 1 row, 3 columns
*/
valuesClause
    : KW_VALUES valuesTableConstructor
    ;

valuesTableConstructor
    : valueRowConstructor (COMMA valueRowConstructor)*
    ;

valueRowConstructor
    : expressionsInParenthesis
    ;

/*
This represents a clause like this:
TABLE(VALUES(1,2),(2,3)) as VirtTable(col1,col2)
*/
virtualTableSource
    : KW_TABLE LPAREN valuesClause RPAREN KW_AS? tableAlias (LPAREN identifier (COMMA identifier)*)? RPAREN
    ;

//-----------------------------------------------------------------------------------
/**
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

   @author Canwei He
*/

//-----------------------------------------------------------------------------------

// group by a,b
groupByClause
    : KW_GROUP KW_BY groupby_expression
    ;

// support for new and old rollup/cube syntax
groupby_expression
    : rollupStandard
    | rollupOldSyntax
    | groupByEmpty
    ;

groupByEmpty
    : LPAREN RPAREN
    ;

// standard rollup syntax
rollupStandard
    : (KW_ROLLUP | KW_CUBE)
    LPAREN expression ( COMMA expression)* RPAREN
    ;

// old hive rollup syntax
rollupOldSyntax
    : expressionsNotInParenthesis
    (KW_WITH KW_ROLLUP | KW_WITH KW_CUBE) ?
    (KW_GROUPING KW_SETS
    LPAREN groupingSetExpression ( COMMA groupingSetExpression)*  RPAREN ) ?
    ;


groupingSetExpression
   : groupingSetExpressionMultiple
   | groupingExpressionSingle
   ;

groupingSetExpressionMultiple
   : LPAREN
   expression? (COMMA expression)*
   RPAREN
   ;

groupingExpressionSingle
    : expression
    ;

havingClause
    : KW_HAVING havingCondition
    ;

havingCondition
    : expression
    ;

expressionsInParenthesis
    : LPAREN expressionsNotInParenthesis RPAREN
    ;

expressionsNotInParenthesis
    : expression expressionPart?
    ;

expressionPart
    : (COMMA expression)+
    ;

expressions
    : expressionsInParenthesis
    | expressionsNotInParenthesis
    ;

columnRefOrderInParenthesis
    : LPAREN columnRefOrder (COMMA columnRefOrder)* RPAREN
    ;

columnRefOrderNotInParenthesis
    : columnRefOrder (COMMA columnRefOrder)*
    ;

// order by a,b
orderByClause
    : KW_ORDER KW_BY columnRefOrder ( COMMA columnRefOrder)*
    ;

clusterByClause
    : KW_CLUSTER KW_BY expressions
    ;

partitionByClause
    : KW_PARTITION KW_BY expressions
    ;

distributeByClause
    : KW_DISTRIBUTE KW_BY expressions
    ;

sortByClause
    : KW_SORT KW_BY
    (
    columnRefOrderInParenthesis
    |
    columnRefOrderNotInParenthesis
    )
    ;

// fun(par1, par2, par3)
function_
    : functionName
    LPAREN
      (
        STAR
        | (KW_DISTINCT | KW_ALL)? (selectExpression (COMMA selectExpression)*)?
      )
    RPAREN (KW_OVER window_specification)?
    ;

functionName
    : // Keyword IF is also a function name
    functionIdentifier
    |
    sql11ReservedKeywordsUsedAsFunctionName
    ;

castExpression
    : KW_CAST
    LPAREN
          expression
          KW_AS
          primitiveType
    RPAREN
    ;

caseExpression
    : KW_CASE expression
    (KW_WHEN expression KW_THEN expression)+
    (KW_ELSE expression)?
    KW_END
    ;

whenExpression
    : KW_CASE
    ( KW_WHEN expression KW_THEN expression)+
    (KW_ELSE expression)?
    KW_END
    ;

floorExpression
    : KW_FLOOR
    LPAREN
          expression
          (KW_TO floorDateQualifiers)?
    RPAREN
    ;

floorDateQualifiers
    : KW_YEAR
    | KW_QUARTER
    | KW_MONTH
    | KW_WEEK
    | KW_DAY
    | KW_HOUR
    | KW_MINUTE
    | KW_SECOND
    ;

extractExpression
    : KW_EXTRACT
    LPAREN
          timeQualifiers
          KW_FROM
          expression
    RPAREN
    ;

timeQualifiers
    : KW_YEAR
    | KW_QUARTER
    | KW_MONTH
    | KW_WEEK
    | KW_DAY
    | KW_DOW
    | KW_HOUR
    | KW_MINUTE
    | KW_SECOND
    ;

constant
    : intervalLiteral
    | Number
    | dateLiteral
    | timestampLiteral
    | timestampLocalTZLiteral
    | StringLiteral
    | stringLiteralSequence
    | IntegralLiteral
    | NumberLiteral
    | charSetStringLiteral
    | booleanValue
    | KW_NULL
    ;

stringLiteralSequence
    : StringLiteral StringLiteral+
    ;

charSetStringLiteral
    : CharSetName CharSetLiteral
    ;

dateLiteral
    : KW_DATE StringLiteral
    | KW_CURRENT_DATE
    ;

timestampLiteral
    : KW_TIMESTAMP StringLiteral
    | KW_CURRENT_TIMESTAMP
    ;

timestampLocalTZLiteral
    : KW_TIMESTAMPLOCALTZ StringLiteral
    ;

intervalValue
    : StringLiteral
    | Number
    ;

intervalLiteral
    : intervalValue intervalQualifiers
    ;

intervalExpression
    : LPAREN intervalValue RPAREN intervalQualifiers
    | KW_INTERVAL intervalValue intervalQualifiers
    | KW_INTERVAL LPAREN expression RPAREN intervalQualifiers
    ;

intervalQualifiers
    : KW_YEAR KW_TO KW_MONTH
    | KW_DAY KW_TO KW_SECOND
    | KW_YEAR
    | KW_MONTH
    | KW_DAY
    | KW_HOUR
    | KW_MINUTE
    | KW_SECOND
    ;

atomExpression
    : constant
    | intervalExpression
    | castExpression
    | extractExpression
    | floorExpression
    | caseExpression
    | whenExpression
    | subQueryExpression
    | function_
    | tableOrColumn
    | expressionsInParenthesis
    ;

precedenceUnaryOperator
    : PLUS
    | MINUS
    | TILDE
    ;

isCondition
    : KW_NULL
    | KW_TRUE
    | KW_FALSE
    | KW_NOT KW_NULL
    | KW_NOT KW_TRUE
    | KW_NOT KW_FALSE
    ;

precedenceBitwiseXorOperator
    : BITWISEXOR
    ;

precedenceStarOperator
    : STAR
    | DIVIDE
    | MOD
    | DIV
    ;

precedencePlusOperator
    : PLUS
    | MINUS
    ;

precedenceConcatenateOperator
    : CONCATENATE
    ;

precedenceAmpersandOperator
    : AMPERSAND
    ;

precedenceBitwiseOrOperator
    : BITWISEOR
    ;

precedenceRegexpOperator
    : KW_LIKE
    | KW_RLIKE
    | KW_REGEXP
    ;

precedenceSimilarOperator
    : precedenceRegexpOperator
    | LESSTHANOREQUALTO
    | LESSTHAN
    | GREATERTHANOREQUALTO
    | GREATERTHAN
    ;

precedenceDistinctOperator
    : KW_IS KW_DISTINCT KW_FROM
    ;

precedenceEqualOperator
    : EQUAL
    | EQUAL_NS
    | NOTEQUAL
    | KW_IS KW_NOT KW_DISTINCT KW_FROM
    ;

precedenceNotOperator
    : KW_NOT
    ;

precedenceAndOperator
    : KW_AND
    ;

precedenceOrOperator
    : KW_OR
    ;

//precedenceFieldExpression
//precedenceUnaryPrefixExpression
//precedenceUnarySuffixExpression
//precedenceBitwiseXorExpression
//precedenceStarExpression
//precedencePlusExpression
//precedenceConcatenateExpression
//precedenceAmpersandExpression
//precedenceBitwiseOrExpression
//precedenceSimilarExpressionMain
//precedenceSimilarExpression
//precedenceEqualExpression
//precedenceNotExpression
//precedenceAndExpression
//precedenceOrExpression
expression
    : atomExpression ((LSQUARE expression RSQUARE) | (DOT identifier))*
    | precedenceUnaryOperator expression
    | expression KW_IS isCondition
    | expression precedenceBitwiseXorOperator expression
    | expression precedenceStarOperator expression
    | expression precedencePlusOperator expression
    | expression precedenceConcatenateOperator expression
    | expression precedenceAmpersandOperator expression
    | expression precedenceBitwiseOrOperator expression
    | expression precedenceSimilarExpressionPart
    | KW_EXISTS subQueryExpression
    | expression (precedenceEqualOperator | precedenceDistinctOperator) expression
    | precedenceNotOperator expression
    | expression precedenceAndOperator expression
    | expression precedenceOrOperator expression
    | LPAREN expression RPAREN
    ;

subQueryExpression
    : LPAREN selectStatement RPAREN
    ;

precedenceSimilarExpressionPart
    : precedenceSimilarOperator expression
    | precedenceSimilarExpressionAtom
    | KW_NOT precedenceSimilarExpressionPartNot
    ;

precedenceSimilarExpressionAtom
    : KW_IN precedenceSimilarExpressionIn
    | KW_BETWEEN expression KW_AND expression
    | KW_LIKE KW_ANY expressionsInParenthesis
    | KW_LIKE KW_ALL expressionsInParenthesis
    ;

precedenceSimilarExpressionIn
    : subQueryExpression
    | expressionsInParenthesis
    ;

precedenceSimilarExpressionPartNot
    : precedenceRegexpOperator expression
    | precedenceSimilarExpressionAtom
    ;

booleanValue
    : KW_TRUE
    | KW_FALSE
    ;

booleanValueTok
   : KW_TRUE
   | KW_FALSE
   ;

tableOrPartition
   : tableName partitionSpec?
   ;

partitionSpec
   : KW_PARTITION
   LPAREN partitionVal (COMMA  partitionVal)* RPAREN
   ;

partitionVal
    : identifier (EQUAL constant)?
    ;

dropPartitionSpec
    : KW_PARTITION LPAREN dropPartitionVal (COMMA  dropPartitionVal )* RPAREN
    ;

dropPartitionVal
    : identifier dropPartitionOperator constant
    ;

dropPartitionOperator
    : EQUAL
    | NOTEQUAL
    | LESSTHANOREQUALTO
    | LESSTHAN
    | GREATERTHANOREQUALTO
    | GREATERTHAN
    ;

sysFuncNames
    : KW_AND
    | KW_OR
    | KW_NOT
    | KW_LIKE
    | KW_IF
    | KW_CASE
    | KW_WHEN
    | KW_FLOOR
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
    | DIV
    | AMPERSAND
    | TILDE
    | BITWISEOR
    | BITWISEXOR
    | KW_RLIKE
    | KW_REGEXP
    | KW_IN
    | KW_BETWEEN
    ;

descFuncNames
    : sysFuncNames
    | StringLiteral
    | functionIdentifier
    ;

identifier
    : Identifier
    | nonReserved
    ;

functionIdentifier
    : identifier DOT identifier
    | identifier
    ;

principalIdentifier
    : identifier
    | QuotedIdentifier
    ;

// Here is what you have to do if you would like to add a new keyword.
// Note that non reserved keywords are basically the keywords that can be used as identifiers.
// (1) Add a new entry to HiveLexer, e.g., KW_TRUE : 'TRUE';
// (2) If it is reserved, you do NOT need to change IdentifiersParser.g
//                        because all the KW_* are automatically not only keywords, but also reserved keywords.
//                        However, you need to add a test to TestSQL11ReservedKeyWordsNegative.java.
//     Otherwise it is non-reserved, you need to put them in the nonReserved list below.
//If you are not sure, please refer to the SQL2011 column in
//http://www.postgresql.org/docs/9.5/static/sql-keywords-appendix.html
nonReserved
    : KW_ABORT | KW_ADD | KW_ADMIN | KW_AFTER | KW_ANALYZE | KW_ARCHIVE | KW_ASC | KW_BEFORE | KW_BUCKET | KW_BUCKETS
    | KW_CASCADE | KW_CHANGE | KW_CHECK | KW_CLUSTER | KW_CLUSTERED | KW_CLUSTERSTATUS | KW_COLLECTION | KW_COLUMNS
    | KW_COMMENT | KW_COMPACT | KW_COMPACTIONS | KW_COMPUTE | KW_CONCATENATE | KW_CONTINUE | KW_DATA | KW_DAY
    | KW_DATABASES | KW_DATETIME | KW_DBPROPERTIES | KW_DEFERRED | KW_DEFINED | KW_DELIMITED | KW_DEPENDENCY
    | KW_DESC | KW_DIRECTORIES | KW_DIRECTORY | KW_DISABLE | KW_DISTRIBUTE | KW_DOW | KW_ELEM_TYPE
    | KW_ENABLE | KW_ENFORCED | KW_ESCAPED | KW_EXCLUSIVE | KW_EXPLAIN | KW_EXPORT | KW_FIELDS | KW_FILE | KW_FILEFORMAT
    | KW_FIRST | KW_FORMAT | KW_FORMATTED | KW_FUNCTIONS | KW_HOUR | KW_IDXPROPERTIES
    | KW_INDEX | KW_INDEXES | KW_INPATH | KW_INPUTDRIVER | KW_INPUTFORMAT | KW_ITEMS | KW_JAR | KW_KILL
    | KW_KEYS | KW_KEY_TYPE | KW_LAST | KW_LIMIT | KW_OFFSET | KW_LINES | KW_LOAD | KW_LOCATION | KW_LOCK | KW_LOCKS | KW_LOGICAL | KW_LONG
    | KW_MAPJOIN | KW_MATERIALIZED | KW_METADATA | KW_MINUTE | KW_MONTH | KW_MSCK | KW_NOSCAN | KW_NULLS
    | KW_OPTION | KW_OUTPUTDRIVER | KW_OUTPUTFORMAT | KW_OVERWRITE | KW_OWNER | KW_PARTITIONED | KW_PARTITIONS | KW_PLUS
    | KW_PRINCIPALS | KW_PURGE | KW_QUERY | KW_QUARTER | KW_READ | KW_REBUILD | KW_RECORDREADER | KW_RECORDWRITER
    | KW_RELOAD | KW_RENAME | KW_REPAIR | KW_REPLACE | KW_REPLICATION | KW_RESTRICT | KW_REWRITE
    | KW_ROLE | KW_ROLES | KW_SCHEMA | KW_SCHEMAS | KW_SECOND | KW_SEMI | KW_SERDE | KW_SERDEPROPERTIES | KW_SERVER | KW_SETS | KW_SHARED
    | KW_SHOW | KW_SHOW_DATABASE | KW_SKEWED | KW_SORT | KW_SORTED | KW_SSL | KW_STATISTICS | KW_STORED
    | KW_STREAMTABLE | KW_STRING | KW_STRUCT | KW_TABLES | KW_TBLPROPERTIES | KW_TEMPORARY | KW_TERMINATED
    | KW_TINYINT | KW_TOUCH | KW_TRANSACTIONS | KW_UNARCHIVE | KW_UNDO | KW_UNIONTYPE | KW_UNLOCK | KW_UNSET
    | KW_UNSIGNED | KW_URI | KW_USE | KW_UTC | KW_UTCTIMESTAMP | KW_VALUE_TYPE | KW_VIEW | KW_WEEK | KW_WHILE | KW_YEAR
    | KW_WORK
    | KW_TRANSACTION
    | KW_WRITE
    | KW_ISOLATION
    | KW_LEVEL
    | KW_SNAPSHOT
    | KW_AUTOCOMMIT
    | KW_RELY
    | KW_NORELY
    | KW_VALIDATE
    | KW_NOVALIDATE
    | KW_KEY
    | KW_MATCHED
    | KW_REPL | KW_DUMP | KW_STATUS
    | KW_CACHE | KW_VIEWS
    | KW_VECTORIZATION
    | KW_SUMMARY
    | KW_OPERATOR
    | KW_EXPRESSION
    | KW_DETAIL
    | KW_WAIT
    | KW_ZONE
    | KW_DEFAULT
    | KW_REOPTIMIZATION
    | KW_RESOURCE | KW_PLAN | KW_PLANS | KW_QUERY_PARALLELISM | KW_ACTIVATE | KW_MOVE | KW_DO
    | KW_POOL | KW_ALLOC_FRACTION | KW_SCHEDULING_POLICY | KW_PATH | KW_MAPPING | KW_WORKLOAD | KW_MANAGEMENT | KW_ACTIVE | KW_UNMANAGED

;

//The following SQL2011 reserved keywords are used as function name only, but not as identifiers.
sql11ReservedKeywordsUsedAsFunctionName
    : KW_IF | KW_ARRAY | KW_MAP | KW_BIGINT | KW_BINARY | KW_BOOLEAN | KW_CURRENT_DATE | KW_CURRENT_TIMESTAMP | KW_DATE | KW_DOUBLE | KW_FLOAT | KW_GROUPING | KW_INT | KW_SMALLINT | KW_TIMESTAMP
    ;
