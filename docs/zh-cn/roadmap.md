## RoadMap

```plantuml
projectscale quarterly
@startgantt
Project starts 2022-03-01
[支持hologres的从sql逆向到fml模型处理] lasts 15 days
[支持clickhouse sql的转换和逆向处理] lasts 30 days
[支持FML元数据能力] lasts 60 days
[引擎数据类型转换可定制] lasts 60 days
[引擎数据类型转换可定制]->[支持clickhouse sql的转换和逆向处理]
[支持hologres的从sql逆向到fml模型处理] -> [引擎数据类型转换可定制]
@endgantt
