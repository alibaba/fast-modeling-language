# MaxCompute 转换

`fastmodel-transform-maxcompute` 在本地解析和生成 MaxCompute SQL，支持与 FML 模型之间的双向转换。它不连接 MaxCompute 项目，也不执行 SQL，无需配置云账号凭证。

## 依赖

使用与其他 FML 模块相同版本的 BOM 管理版本，再添加模块依赖：

```xml
<dependency>
    <groupId>com.aliyun.fastmodel</groupId>
    <artifactId>fastmodel-transform-maxcompute</artifactId>
</dependency>
```

完整 BOM 配置见[转换指南](guide.md)。该模块通过 SPI 注册解析器、转换器、比较器和语句构建器。使用自定义打包工具时，应保留并合并 `META-INF/services` 文件。

## Java API

默认方言为 MaxCompute 2.0。以下示例将合成建表语句解析为 FML，再生成 MaxCompute SQL：

```java
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.transform.api.Transformer;
import com.aliyun.fastmodel.transform.api.TransformerFactory;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;

Transformer<Node> transformer = TransformerFactory.getInstance()
    .get(DialectMeta.DEFAULT_MAX_COMPUTE);
Node model = transformer.reverse(new DialectNode(
    "CREATE TABLE sample_product (product_id BIGINT, product_name STRING) "
        + "PARTITIONED BY (ds STRING) LIFECYCLE 7;"));
DialectNode sql = transformer.transform(model);
System.out.println(sql.getNode());
```

模块还提供 Table DTO 转换、差异 DDL，以及 DQC/规则定义到 SQL 或 JSON 的转换。调用 Java API 时，应检查 `DialectNode.isExecutable()`；例如无法直接执行的列顺序调整仍可能返回用于描述目标结构的 SQL。

## CLI

构建包含所有已接入方言的 CLI：

```bash
./mvnw -pl fastmodel-agent-cli -am package -DskipTests
FML_CLI_JAR="$(find fastmodel-agent-cli/target -name 'fastmodel-agent-cli-*-executable.jar' \
  -type f -print -quit)"
test -n "$FML_CLI_JAR"
java -jar "$FML_CLI_JAR" capabilities
```

CLI 接受的名字为 `max-compute`、`max-compute@1.0`、`max-compute@2.0`。默认使用 2.0；1.0 转换器按对应类型系统输出，例如将小整数类型映射为 `BIGINT`。以实际输入和转换结果判断类型保留情况。

```bash
java -jar "$FML_CLI_JAR" transform --dialect max-compute \
  --text "create dim table sample_product (product_id bigint);"
java -jar "$FML_CLI_JAR" transform --dialect max-compute@1.0 \
  --text "create dim table sample_product (product_id bigint);"
java -jar "$FML_CLI_JAR" transform --dialect max-compute@2.0 \
  --text "create dim table sample_product (product_id bigint);"
java -jar "$FML_CLI_JAR" validate --dialect max-compute \
  --text "CREATE TABLE sample_product (product_id BIGINT);"
```

`validate` 检查本地语法解析，不能代替目标 MaxCompute 服务的执行验证。命令输出为 JSON，并以退出码表示成功或失败；详细契约见仓库中的 `skills/fml/references/cli.md`。

## IDE

IDE 的导入和导出菜单均提供 `MaxCompute`。导入原生建表 SQL 得到 FML，编辑后可再导出 MaxCompute SQL。后端包含本模块运行依赖，使用与 Java API 相同的 SPI 转换器。

## 覆盖范围

回归覆盖建表及常见表结构变更、分区、生命周期、表属性、复杂类型和 VECTOR，以及模型往返、版本差异、DQC/规则生成与跨方言转换。支持范围以语法、转换器实现和回归测试为准；引擎特有属性或不可执行变更应检查生成结果后处理。
