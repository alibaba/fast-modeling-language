## 概览

| 命令   | 英文          | 说明                       |
|------|-------------|--------------------------|
| 导入   | import_sql  | 导入其他引擎的sql, 输出FML模型内容 |
| 导出   | export_sql  | 将fml模型导出为其他引擎的sql。       |

## IMPORT_SQL

将其他引擎的文本内容，转换为FML语句。

- 命令格式

```
IMPORT_SQL -m <mode> -t <text> | -u <uri> [WITH <properties>]
```

- 参数说明
    - mode : 必选，导入的sql的模式，比如mysql，oracle，hologres等
    - text：导入的sql文本内容
    - uri：支持从其他uri导入的sql文本内容
    - properties：可选，配置属性信心

- 示例1， 导入mysql的创建表的语句

```
IMPORT_SQL -m mysql -t "create table a (col1 bigint comment 'col1 comment')" WITH ('reverseTableType' = 'normal')
```

## EXPORT_SQL

将其他引擎的文本内容，转换为FML语句。

- 命令格式

```
  EXPORT_SQL -m <mode> -t <text> | -u <uri> [WITH <properties>]
```

- 参数说明
    - mode : 必选，导出的sql的模式，比如mysql，oracle，hologres等
    - text：导出的fml文本内容
    - uri：支持从其他uri导出的fml文本内容
    - properties：可选，配置属性信心

- 示例1， 将fml语句导出为mysql的创建表的语句

```
  EXPORT_SQL -m mysql -t "create dim table a (col1 bigint comment 'col1 comment')"
```
