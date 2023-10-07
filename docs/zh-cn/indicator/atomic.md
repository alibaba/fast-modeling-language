## 创建原子指标

- 命令格式

```sql
CREATE
<indicator_type> INDICATOR <indicator_name> 
<data_type>  
COMMENT <comment> 
WITH PROPERTIES (<key>=<value>) 
```

- 参数说明
    - indicator_type : 必选， 指标类型，ATOMIC
    - indicator_name： 必选，指标名称。
    - data_type : 可选， 指标结果的数值类型
    - comment： 可选，指标的注释信息
    - key=value： 可选，定义指标的自定义信息, 原子支持以下的扩展信息定义原子指标内容：
        - is_distinct : 可选，是否去重
        - biz_caliber : 必选， 业务口径
        - agg_function： 必选，聚合函数，目前支持：count，sum， min，max，average等
        - data_unit : 必选，数据单位，与度量单位进行关联。
        - data_round: 可选， 小数位数，用于指标小数最后取值，默认是0
        - extend_name ： 可选， 扩展名，根据本地化语言进行定义。
        - business_process : 可选： 业务过程。
- 示例
    - 示例1，创建商品数量的原子指标

```sql
CREATE
ATOMIC Indicator test_bu.sku_count bigint COMMENT '商品数量'
WITH (
'data_unit' = 'jian'
,'is_distinct' = 'false'
,'agg_function' = 'count'
,'extend_name' = 'sku_count_name'
,'business_process' = 'test_bp'
,'biz_caliber' = 'count(1)'
);
```

