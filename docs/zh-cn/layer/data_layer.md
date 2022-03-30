# 数仓分层

## 创建数仓分层

通过FML语句，你可以创建ODS（贴源层），DWD（明细层），DIM（维度层），DWS（汇总逻辑层），ADS（应用层）等架构信息。

- 限制条件
    - 无
- 命令格式

```text
-- 创建数仓分层
    CREATE [OR REPLACE] LAYER [IF NOT EXISTS] 
    <layer_name> [ALIAS <alias>]
    [checker,...]
    [COMMENT <comment>]
    [WITH (<property>,...)]
    ;
checker
    : CHECKER <checker_type> <checker_name> <string> [COMMENT <comment>]
    
property
    : <key> = <value>
```

- 参数说明
    - layer_name, 必填， 数仓规划的名字。
    - alias： 可选，别名，一般是简称。
    - comment: 可选，数仓规划的描述。
    - checker: 可选，你可以定义分层检查器，用于在分层中模型信息的检查处理。比如常见的检查器有表名检查器。
        - checker_type : 必填，检查器类型，具体由引擎实现，常见的有表名检查器
        - checker_name : 必填，检查器名称
        - comment : 可选，检查器的描述
    - property: 可选， 数仓分层属性定义
        - key：属性的key，用单引号.

- 使用示例
    - 示例1： 创建名为ODS的数仓分层

```text
 CREATE LAYER ODS  
 ALIAS '贴源层' COMMENT '贴源层描述'
 WITH ('type' = 'ODS')
 
```

## 修改数仓别名

支持通过FML语句，修改数仓别名

- 限制条件
    - 无
- 命令格式

```text
ALTER LAYER <layer_name> SET ALIAS <alias>
```

- 参数说明
    - layer_name, 必填，数仓规划的名字
    - alias : 必填，新的别名名称
- 使用示例
    - 示例1： 修改ODS的别名为贴源层测试

```text
    ALTER LAYER ODS SET ALIAS '贴源层测试';
```

## 检查器类型

- 表名检查器