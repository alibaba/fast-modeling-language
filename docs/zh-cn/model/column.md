# 列与约束

| 类型      | 功能           |
|---------|--------------|
| 添加列     | 可以给表的模型增加列信息 |
| 添加分区列   | 增加分区列        |
| 删除分区列   | 增加分区列        |
| 修改列名    | 修改列的名字       |
| 修改列的注释  | 修改列的备注       |
| 修改列名和注释 | 修改列名和注释      |
| 修改列的顺序  | 删除表          |
| 删除列     | 删除列          |

## 添加列

为已存在的表，增加列信息

- 限制条件
    - 可以一次增加多个列信息。


- 命令格式

```
ALTER <table_type> TABLE <table_name> ADD COLUMNS (<col_name> ALIAS <alias> <col_type> <category> <constraint> <default_value> COMMENT <comment> WITH (<key>=<value>)
```

- 参数说明
    - table_type : 可选，表类型，与create语句中tabletype对应
    - table_name : 表名，必填。
    - col_name : 列名，必填。
    - col_type: 列类型， 必填，参考数据类型文档。
    - category： 列分类，可选，参考列数据分类文档。
    - constraint: 约束，支持PRIMARY KEY和NOT NULL的定义。
    - default_value: 默认值，可选。
    - comment：列注释，可选，描述
    - key，value： 定义列的自定义属性信息。需要加上单引号。
    - alias: 别名，可选，一般是中文名。
- 使用示例
    - 示例1： 给fact_login_info增加两个列

```
ALTER TABLE fact_login_info
    ADD COLUMNS(login_date DATETIME ALIAS '登录日期' COMMENT '登录日期描述' WITH ('pattern'='yyyyMMdd'), login_umid STRING NOT NULL COMMENT '登录设备');
```

- 示例2：给fact_login_info增加一个关联维度分类的列

```
ALTER TABLE fact_login_info
    ADD COLUMNS(login_uid BIGINT CORRELATION COMMENT '登录UID');
```

## 增加分区列

为已存在的表，增加列信息。

- 限制条件
    - 只有在表尚未物化的时候，才能新增分区列，否则会新增失败。


- 命令格式

```
ALTER
<table_type> TABLE <table_name> ADD PARTITION COLUMN (<col_name> <col_type> <comment> WITH(<param> = <value>))
```

- 参数说明
    - table_type : 可选，请参考 **增加列 **说明。
    - table_name : 必选， 请参考： **增加列 **说明。
    - col_name : 必选， 参考： **增加列**
    - col_type : 必选，参考： **增加列**
    - comment : 可选： 参考：**增加列**
- 使用示例
    - 示例1 ： 给fact_login_info增加一个ds分区：

```
ALTER TABLE fact_login_info
    ADD PARTITION COLUMN (ds BIGINT COMMENT '日期分区' WITH ('pattern'='yyyyMMdd'));
```

## 删除分区列

为已存在的表，删除分区列。

- 限制条件
    - 只有在表尚未物化的时候，才能新增分区列，否则会新增失败。


- 命令格式

```
ALTER
<table_type> TABLE <table_name> DROP
PARTITION COLUMN <col_name>
```

- 参数说明
    - table_type : 可选，请参考 **增加列 **说明。
    - table_name : 必选， 请参考： **增加列 **说明。
    - col_name : 必选， 删除列名， 参考： **增加列**
- 使用示例
    - 示例1 ： 将fact_login_info中ds分区列进行删除：

```
ALTER TABLE fact_login_info
    ADD PARTITION COLUMN (ds BIGINT COMMENT '日期分区' WITH ('pattern'='yyyyMMdd'));
```

## 删除列

将已存在的表中，删除指定列。

- 限制条件
    - 只有在表尚未物化的时候，才能新增分区列，否则会新增失效。
- 命令格式

```
ALTER
<table_type> TABLE <table_name> DROP
COLUMN <col_name>
```

- 参数说明
    - table_type :  可选， 请参考 增加列说明。
    - table_name:  必选， 表名
    - col_name :  必选，删除的列名，参考：增加列


- 使用示例
    - 示例1 ： 将fact_login_info中删除列信息

```
ALTER FACT TABLE fact_login_info DROP COLUMN col1; 
```

## 更改列信息

修改已有列的信息，支持修改列名，列注释，列类型等信息。

- 限制条件
    - 当表已经物化的时候，修改列类型，可能不会生效。
- 命令格式
    - 更改列名

```
ALTER <table_type> TABLE <table_name> CHANGE COLUMN <old_col_name> RENAME TO <new_col_name>
```

- 更改列备注

```
ALTER <table_type> TABLE <table_name> CHANGE COLUMN <col_name> SET COMMENT < comment >
```

- 更改列名、备注、约束等

```
ALTER <table_type> TABLE <table_name> CHANGE COLUMN <old_name> <new_name> ALIAS <alias> <data_type> COMMENT <column_constraint> <comment> 
```

- 参数说明
    - table_type : 表类型
    - table_name : 表名
    - old_name :  旧的列名
    - new_name : 新的列名
    - data_type :  数据类型
    - column_constraint:
        - PRIMARY KEY , 主键，可以定义ENABLE或者DISABLE ，代表是否启用或者禁止。
        - NOT NULL ，是否为空，可以定义ENABLE或者DISABLE，代表是否启用或者禁止。
    - comment: 备注信息
    - alias: 别名，可选，一般是中文名。
- 使用示例：
    - 示例1 ： 给fact_login_info中的ip更改为login_ip信息：

```
ALTER FACT TABLE fact_login_info CHANGE COLUMN ip RENAME TO login_ip;
```

- 示例2： 给fact_login_Info中的ip中的注释更改为登录ip地址：

```
ALTER TABLE fact_login_info CHANGE COLUMN ip SET COMMENT '登录IP地址';
```

- 示例3：修改fact_login_info中的ip字段的名字和备注信息

```
ALTER TABLE fact_login_info CHANGE COLUMN ip login_ip ALIAS '登录IP' STRING COMMENT '登录IP地址描述';
```

## 修改列的顺序

```
ALTER <table_type> TABLE <table_name> CHANGE COLUMN <old_column> <new_column> <data_type> AFTER <after_column> | FIRST
```

- 命令限制
    - 支持将表中的一个列的位置，设置为最开始或者在某个列之后
- 参数说明
    - table_type:  可选， 表类型.
    - table_name: 必选， 表名
    - old_column : 必选，原来的表名
    - new_column: 必选，最新的表名
    - data_type : 必选，列类型
    - after_column: 可选，设置在某个列之后

- 示例1，将表dim_shop中的shop_name顺序调整到shop_id之后

```
ALTER TABLE dim_shop CHANGE COLUMN shop_name shop_name STRING AFTER shop_id;
```

- 示例2， 将表dim_shop中的shop_id顺序调整到第一位

```
ALTER TABLE dim_shop CHANGE COLUMN shop_id shop_id STRING FIRST;
```

## 增加约束

给现有的表中增加不同类型的约束，目前FML支持针对表有以下的约束信息：

| 约束类型 | 说明 | 角色 |
| --- | --- | --- |
| 主键约束 | 可以支持多个联合主键的约束定义 | 项目成员权限 |
| 维度约束 | 定义事实表与维度表，维度表之间的维度约束信息 |  |
| 列组约束 | 定义列与列之间分组约束 |  |
| 数据标准约束 | 定义列与数据标准的约束 |  |
| 层级约束 | 层级维度表中的定义层级约束出礼 |  |

### 增加主键约束

- 命令格式

```
ALTER <table_type> TABLE <table_name> ADD CONSTRAINT <constraint_name> PRIMARY KEY (<col_name_list>)) COMMENT <comment>
```

- 参数说明
    - table_type: 可选，表类型
    - table_name: 必选， 表名
    - constraint_name : 必填， 约束名, 英文数字下划线。
    - col_name_list :  表的列名，支持多个，以逗号分割。


- 示例： 给fact_login_info表增加主键约束

```
ALTER TABLE fact_login_info ADD CONSTRAINT fact_login_primary PRIMARY KEY (login_ip); 
```

### 增加维度约束

- 命令格式

```
ALTER <table_type> TABLE <table_name> ADD CONSTRAINT <constraint_name> DIM KEY (<col_name_list) REFERENCES <dim_table_name> (<ref_col_name_list>)
```

- 参数说明
    - table_type: 可选
    - table_name: 必选，表名
    - constraint_name : 必填， 约束名
    - col_name_list: 可选，表的列的信息
    - dim_table_name: 必选，关联的维度表的表名
    - ref_col_name_list : 可选，关联的维度表的列名
- 限制条件
    - 维度关联的列，必须存在表中。
- 示例1： 给fact_login_info表增加dim_user_info的维度关联

```
ALTER TABLE fact_login_info ADD CONSTRAINT ref_dim_user DIM REFERENCES dim_user_info;
```

- 示例2： 给fact_login_info表通过uid的字段与dim_user_info进行关联

```
ALTER TABLE fact_login_info ADD CONSTRAINT ref_dim_user DIM KEY (login_uid) REFERENCES dim_user_info(uid);
```

## 删除约束

根据约束名删除指定约束信息。

- 命令格式

```
ALTER <table_type> TABLE <table_name> DROP CONSTRAINT <constraint_name>;
```

- 参数说明
    - table_type: 可选，表类型，请参考创建表：表类型定义
    - table_name : 必选，表名
    - constraint_name : 约束名
