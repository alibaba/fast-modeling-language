## 基本元素

FML支持建模95%场景的模型对象定义，每种的模型对象，在使用DDL定义时，会有相似的基本元素组成。

## 例子

先以一个基本的例子开始， 比如创建一个名为：example的维度表

```
   CREATE DIM TABLE example 
   (
     name ALIAS 'Name' STRING COMMENT 'name comment'
   ) 
   COMMENT 'comment' 
   WITH ('param' = 'value');
```

## CODE-编码

模型对象的唯一标识， 支持全英文或者英文+数字组合的标识，支持多个单词以"."连接起来。

## ALIAS-别名

模型对象的别名，一般用于显示名，以单引号括起来作为字符字面量。

## COMMENT-备注

模型对象的备注。

## PROPERTY-扩展属性

模型对象的扩展属性，采用key-value的方式，key和value需要用单引号括起来。

## 业务对象英文表

| 单词                | 复数                  | 说明   |
|-------------------|---------------------|------|
| ADJUNCT           | ADJUNCTS            | 修饰词  |
| DICT              | DICTS               | 数据标准 |
| LAYER             | LAYERS              | 数仓分层 |
| BUSINESS_PROCESS  | BUSINESS_PROCESSES  | 业务过程 |
| BUSINESS_CATEGORY | BUSINESS_CATEGORIES | 业务分类 |
| MARKET            | MARKETS             | 数据集市 |
| SUBJECT           | SUBJECTS            | 主题域  |
| TABLE             | TABLES              | 表模型  |
| INDICATOR         | INDICATORS          | 指标   |


