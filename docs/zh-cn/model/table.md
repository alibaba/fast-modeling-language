# 表

在FML中，表是实际未来设计的对象，其他的像业务板块、数据域、业务过程，更多的是数据管理的分类概念。 在未来物化以及模型设计，都涉及到表的处理和定义。 本文主要是介绍表操作的相关命令。

> 因为FML是一种设计语言，所以语义上的内容，需要实际的引擎来实现，这里只列出推荐的值。

| 类型 | 功能                     |
| --- |------------------------|
| 创建表 | 创建维度表、事实表、码表、DWS表、ADS表 |
| 表拷贝 | 支持表的信息拷贝处理             |
| 修改表的注释 | 修改表的注释内容               |
| 修改表的别名 | 修改表的别名                 |
| 重命名表 | 重命名表的名称                |
| 设置表的属性 | 设置表的属性信息               |
| 删除表 | 删除表                    |
| 查看表信息 | 查看FML设计表的信息            |
| 查看建表语句 | 查看表的FML DDL语句          |

## 创建表

创建维度表、事实表、码表、DWS表

- 限制条件
    - 表的限制条件请参见 [FML使用限制项](http)
- 命令格式

```text
--创建新表
      CREATE <table_type> TABLE
      IF NOT EXISTS
      --表名
      <table_name> ALIAS <alias>
      --定义列属性
      <col_name> <datatype> <category> COMMENT <comment> 
      [references]
      WITH (param=<value>,....)
      --定义约束
      PRIMARY KEY (<col_name>),
      --维度约束
      CONSTRAINT <constraint_name> DIM KEY (<col_name>) REFERENCES <ref_table_name> (<ref_table_col_name>),
      --层级约束
      CONSTRAINT <constraint_name> LEVEL <col_name:(<col_name>)>,
      --分组约束
      CONSTRAINT <constraint_name> COLUMN_GROUP(<col_name>,...), 
      --定义备注
      COMMENT 'comment'
      --定义分区
      PARTITIONED BY (col DATATYPE COMMENT 'comment' WITH ('key'='value',...), ...)
      --定义属性
      WITH ('key'='value', 'key1'='value1', ...)
      ;
      
tableType
    : dimDetailType? DIM
    | factDetailType? FACT
    | CODE
    | dwDetailType? DWS
    ;
    
 dimDetailType
    : NORMAL
    | LEVEL
    | ENUM
    ;
    
 factDetailType
    : TRANSACTION
    | AGGREGATE
    | PERIODIC_SNAPSHOT
    | ACCUMULATING_SNAPSHOT
    | CONSOLIDATED
   ;
   
   
 dwDetailType
 	 :  ADVANCED 
 
comment 
    : COMMENT 'comment'
    ;
  
references
    : REFERENCES 
    ;
```

- 参数说明
    - tableName，必填，表的名称。推荐在128个字符，建议为英文、数字、下划线等字符，
    - alias: 可选，别名，一般是中文名。
    - if not exists： 可选，参照SQL的语义， 推荐引擎实现：如果不指定if not exists选项而存在同名表，会报错。如果指定了if not exists，无论是否存在同名表
    - tableType表的类型，目前支持4种DIM, FACT, CODE, DWS四种表类型，其中DIM和FACT中又细分以下类型。
        - 维度表：NORMAL普通维度表，也是默认创建时指定的，LEVEL，层级维度表，比如省市区具有层级关系的维度表，ENUM枚举维度，一些常用的可枚举的值的集合，比如男女性别等。 枚举维度表，可以使用insert into语句进行写入到数据， 其他维度表和事实表不支持。
        - 事实表：TRANSACTION，事务表，是事实表默认创建，PERIODIC_SNAPSHOT，周期快照表， ACCUMULATING_SNAPSHOT， 累加快照表， AGGREGATE，聚合事实表，CONSOLIDATED合并事实表。
        - DWS表： 由特定指标进行合并的逻辑汇总表，定义语法与维度表和事实表类似，只是表的类型不一样。除了支持普通汇总逻辑表的定义，还支持高级汇总逻辑表的处理。
        - 码表，又叫标准代码：一种包含行业特定的属性的代码表，以水电行业为例， 可能会建比如是否有合同，供电合同类型等标准代码。
    - comment, 表的备注名，推荐长度在1024字符， 具有由FML引擎来决定实现。
    - columnDefinition
        - 列的定义，可选。注： FML支持先设计后物化的处理过程， 所以新建的表可以没有列信息。
        - col_name : 列名，可选， 列名格式支持英文、数字、下划线，如果是FML中的关键字，需要使用``来进行表示下。
        - dataType, 目前FML是和MaxCompute2.0的数据类型对齐，包含了BIGINT，SRRING，VARCHAR， CHAR，DECIMAL，DATETIME等，
          详细的数据类型列表，可以参考：[FML字段的数据类型。 ](https://yuque.antfin-inc.com/gzwan7/hddy77/izptgd/edit)
        - category: 可选，可以对列进行分类，在维度建模中，列具有业务属性， 默认是ATTRIBUTE， 其他的类型有： MEASUREMENT（度量）， CORRELATION（关联字段）。
    - constraint, 可选，定义表结构的约束情况。 FML通过constraint扩展了DDL的语义，可以丰富的表达维度建模中的业务涵义。
        - 主键约束（PrimaryConstraint）： PRIMARY KEY(col1, col2) 可选， 里面的字段列表必须是先定义的列表。
        - 维度约束（DimConstraint）： DIM KEY(col1, col2) REFERENCES table_name(ref1, ref2), 定义了维度约束的列，与引用表的字段的一一对应。
        - 层级约束（LevelConstraint）： 只在层级维度中进行生效，定义层级维度中层级的定义，定义格式如下可以参考下面的示例。
        - 分组约束（ColumnGroupConstraint，关键字：COLUMN_GROUP）, 定义表中的多个字段为一组的约束。
        - 以下是DWS表才会有的约束信息 since0.4.8
            - 指标维度约束(IndDimensionConstraint) : 定义指标维度
            - 时间周期引用：TimePeriodConstraint ： 定义时间周期约束
    - 分区定义，由于大数据的建模中，都会有分区的定义，所以在FML定义过程中，默认会提供了分区的操作，可以参考：Partitioned BY语法。
    - WITH是用户自定义的信息，采用key=value的方式，key和value都必须用单引号。 这样处理的原因，防止因为自定义的信息，与FML的关键字冲突， WITH中的扩展属性，可以由FML引擎来进行解析处理。


- 使用示例
    - 示例1： 创建一个没有列的普通维度表test1

```text
CREATE DIM TABLE IF NOT EXISTS test1 COMMENT '一个测试维度表';
```

- 示例2： 创建有列的普通维度表test1

```text
CREATE DIM TABLE test1 (
  	col1 BIGINT COMMENT 'comment'
)
```

- 示例3： 创建一个带有主键约束的普通维度表

```text
CREATE DIM TABLE test1 (
  	col1 STRING COMMENT 'comment',
    col2 STRING COMMENT 'comment',
    --约束名可以不用定义
    CONSTRAINT c1 PRIMARY KEY(col1)
) COMMENT '带有constraint的普通维度表'
```

- 示例4： 创建一个带有层级约束的层级维度表

```text
CREATE DIM TABLE test1(
    province_id BIGINT comment '省份ID',
  	province STRING COMMENT 'comment',
    city_id BIGINT comment '城市ID',
    city STRING COMMENT 'comment',
    --定义层级约束， 名称和备注都是可选
    CONSTRAINT c1 LEVEL <province_id:(province), city_id:(city)> COMMNET '层级关系'
) COMMENT '层级维度表示例'
```

- 示例5： 创建一个带有维度约束的事务事实表

```text
CREATE FACT TABLE fact_pay_order (
  	order_id BIGINT COMMENT '订单ID', 
    order_time DATETIME COMMENT '订单时间',
    sku_code string COMMENT '商品code',
 		shop_code string COMMENT '门店code',
  	gmt_create string COMMENT '创建时间',
    gmt_pay string COMMENT '支付时间',
    pay_type string COMMENT '支付类型',
    pay_price bigint COMMENT '支付金额',
    refund_price bigint COMMENT '退款金额',
    primary key (order_id),
    --定义约束信息
    constraint fact_pay_order_rel_dim_sku DIM KEY (sku_code,shop_code) REFERENCES dim_sku(sku_code,shop_code),
    constraint fact_pay_order_rel_dim_shop DIM KEY (shop_code) REFERENCES dim_shop(shop_code)
) COMMENT '支付订单事实表'
```

- 示例6： 创建一个带有分组约束的维度表

```text
CREATE DIM TABLE IF NOT EXISTS dim_sku_group
(
  sku_code string COMMENT '商品code',
  shop_code string COMMENT '门店code',
  constraint group_name1 COLUMN_GROUP (sku_code, shop_code)
) COMMENT '商品'

```

- 示例7： 创建一个带有分区的事实表

```text
CREATE FACT TABLE IF NOT EXISTS dim_sku_group
(
  sku_code string COMMENT '商品code',
  shop_code string COMMENT '门店code',
  constraint col_group COLUMN_GROUP (sku_code, shop_code)
) 
COMMENT '商品'
PARTITIONED BY (ds STRING COMMENT '日期分区' )

```

- 示例8： 创建一个码表

```text
--码表, 默认不需要指定列信息，一般由FML引擎描述固定字段。
CREATE CODE TABLE IF NOT EXISTS code_tabl1 
COMMENT '供电合并类型'
```

- 示例9： 创建一个带有自定义属性的表

```text
CREATE DIM TABLE dim_table_1 (
  	name STRING COMMENT '名称'
) COMMENT '带有自定义属性的表' WITH ('key1'='value1', 'key2'='value2');
```

- 示例10： 创建一个带有数据标准的维度表 ：dim_test_table

```text
CREATE DIM TABLE dim_test_table (
  name STRING COMMENT 'commet' WITH ('dict'='dict_code')
)
```

- 示例11： 创建一个带有别名的表

```text
CREATE DIM TABLE dim_test_table ALIAS '维度测试表' COMMENT '这是一个维度测试表的内容';
```

- 示例11：创建一个具有关联维度和指标的汇总逻辑表

## 表拷贝

FML支持从已有的表的信息中，拷贝新的表的信息，可以支持快速的建新的表信息。

- 命令格式

```text
CREATE DIM TABLE <table_name> ALIAS <alias> COMMENT <comment> LIKE <src_table_name>
```

- 参数说明
    - table_name, 必选，新的表名。
    - alias： 可选，新的表别名。
    - comment: 可选，表的备注。
    - src_table_name : 必选，新的表名内容。
- 使用示例
    - 示例1 ： 复制一个表模型内容dim_test, 叫dim_test2

```text
CREATE DIM TABLE dim_test2 LIKE dim_test; 
```

- 示例2： 复制一个表模型内容dim_test, 叫dim_test2, 并且别名测试名， 备注为测试表

```text
CREATE DIM TABLE dim_test2 ALIAS '测试名' COMMENT '测试表' LIKE dim_test; 
```

## 修改表的别名

修改表的别名信息

- 命令格式

```text
ALTER TALBE <table_name> SET ALIAS '<new_alias>'
```

- 参数说明
    - table_name : 必选，表名
    - alias： 必选，表的别名。
- 使用示例
    - 使用示例1：将表dim_test的别名修改为“维度测试表”

```text
ALTER TABLE dim_test SET ALIAS '维度测试表'; 
```

## 修改表的注释

修改表的注释内容。

- 命令格式

```text
ALTER TABLE <table_name> SET COMMENT '<new_comment>'
```

- 参数说明
    - table_name : 必填， 修改的表名
    - new_comment : 必填，修改后的注释名
- 使用示例

```text
ALTER TABLE test1 SET COMMENT '修改后的备注';
```

## 重命名表

将表的名称重新修改。

- 命令格式

```text
ALTER TABLE <table_name> RENAME TO <new_table_name>
```

- 参数说明
    - table_name : 必填， 修改的表名
    - new_table_name : 必填，修改后的表名
- 使用示例

```text
ALTER TABLE test1 RENAME TO test2;  --将表test1修改为test2
```

## 设置表的属性

设置表的自定义属性

- 命令格式

```text
ALTER TABLE <table_name> SET PROPERTIES(<key>=<value,...)
```

- 参数说明
    - table_name : 必填，表名
    - key，value ： 必填，是需要设置的属性的key和value，需要用单引号括起来。
- 使用示例

```text
ALTER TABLE tb1 SET PROPERTIES('key1'='value1');
```

## 删除表

将表从project中删除

- 命令格式

```text
DROP TABLE <table_name>
```

- 参数说明
    - table_name : 必填， 需要删除的表名
- 使用示例 : 删除表名为test1的表

```text
DROP TABLE test1; 
```

## 查看表的信息

查看表的信息

- 命令格式

```text
DESC TABLE <table_name>
```

- 参数说明
    - table_name : 必填，表名
- 使用示例

```text
DESC TABLE test1
```

返回结果如下：

## 查看建表语句

查看FML的建表语句

- 命令格式

```text
SHOW CREATE TABLE <table_name>
```

- 参数说明
    - table_name : 必填，
- 使用示例

```text
SHOW CREATE TABLE test1; 
```

返回结果：

```text
name |            ddl
------+---------------------------
 c    | CREATE DIM TABLE c (
      |    a BIGINT COMMENT 'abc'
      | )
      |  COMMENT 'abc'
(1 row)
```


