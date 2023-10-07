# 查询语句总览

## 引言

你可以通过FML语句来进行模型信息的查询，目前FML提供了show语句来进行模型信息的查询，有点类型`mysql`中的show语句内容。

<a name="top"></a>

## 查询语法内容

- 限制条件
    - 查询语句只支持查询当前业务板块的内容。
- 命令格式

```

SHOW <object_type> [FROM <table_name> ]
[condidtion];

object_type : 
    LAYERS
    | DOMAINS
    | BUSINESS_PROCESSES
    | BUSINESS_CATEGORIES
    | MARKETS
    | SUBJECTS
    | [tableType] TABLES
    | [indicatorType] INDICATORS
    | ADJUNCTS
    | TIME_PERIODS
    | MEASURE_UNITS 
    | DICTS
    | DICT GROUPS
    | MEASURE_UNIT GROUPS
    | COLUMNS
    | CODES
    | DIMENSIONS 
    | DIM_ATTRIBUTES 
    | DEPENDENCY
    ; 
 
 condition : 
     WHERE expression
    ;

```

- 参数说明
    - table_name, 可选，配合show columns进行使用。
    - condition， 可选，支持where的表达式的and和or的处理。
    