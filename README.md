# Fast Modeling Language

FML(Fast Modeling Language) 用于维度建模领域快速构建的一门类SQL语言。主要目标是提供一套kimball维度建模理论下，结合大数据开发场景下的一种领域特定语言。
FML采用了类SQL的语言方式， 创建表语法是参考了SQL标准语法，并有自己的扩展。 FML是一种模型设计语言，期望做到设计与实现解耦，在设计过程中，不用特别考虑各个大数据引擎的实现方式。
建模引擎会根据FML定义的Schema去驱动底层各个数据引擎的执行和操作。用户在使用FML时，并不需要特别关注底层数据引擎的细节部分，只有在实际物化（将设计的表转换为底层引擎的物理表时）阶段，建模引擎会根据物化的选择，将FML语言，转换为数据引擎可识别的SQL语法，并提交任务节点执行。具体与各个数据引擎的转换，请参考
FML Transformer。

### Features

* 一种支持维度建模的领域特定语言，类SQL语法。

* 支持数仓规划、字段标准、标准代码、指标等数仓建设中全流程的语法定义。

* 使用Java编写，可以方便的构造语法的节点API进行模型构建。

* 支持FML语法转换到常见引擎，如Hive， Hologres，Mysql 等Transform API.

* 提供基于JDBC Driver的方式，来使用FML语言来与模型引擎进行交互处理。



### Quick Start

```xml
 <dependencyManagement>
  <dependencies>
    <dependency>
      <groupId>com.aliyun.fastmodel</groupId>
      <artifactId>fastmodel-bom</artifactId>
      <version>${lastest}</version>
      <type>pom</type>
      <scope>import</scope>
    </dependency>
  </dependencies>
</dependencyManagement>

<dependencies>
<!-- core parser-->
<dependency>
    <groupId>com.aliyun.fastmodel</groupId>
    <artifactId>fastmodel-core</artifactId>
  </dependency>
  <dependency>
    <groupId>com.aliyun.fastmodel</groupId>
    <artifactId>fastmodel-parser</artifactId>
  </dependency>

<!--transformer-->
<dependency>
  <groupId>com.aliyun.fastmodel</groupId>
  <artifactId>fastmodel-transform-api</artifactId>
</dependency>
<dependency>
  <groupId>com.aliyun.fastmodel</groupId>
  <artifactId>fastmodel-transform-hive</artifactId>
</dependency>

<dependency>
  <groupId>com.aliyun.fastmodel</groupId>
  <artifactId>fastmodel-transform-mysql</artifactId>
</dependency>

<dependency>
  <groupId>com.aliyun.fastmodel</groupId>
  <artifactId>fastmodel-transform-hologres</artifactId>
</dependency>
</dependencies>
```

### Parser Example

```java

import com.aliyun.fastmodel.core.parser.FastModelParser;
import com.aliyun.fastmodel.core.parser.FastModelParserFactory;

public class HelloFML {

  //单例
  private static final FastModelParser PARSER = FastModelParserFactory.getInstance()
          .get();

  public static void main(String[] args) {
    String fml
            = "create dim table t_1 alias 'alias_name' (col bigint alias 'alias_name' comment 'col_comment') comment 'comment';";
    CreateDimTable createDimTable = PARSER.parseStatement(fml);
    //do your work
  }
}

```

### Transformer Example

```java
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.transform.api.Transformer;
import com.aliyun.fastmodel.transform.api.TransformerFactory;
import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;

public class HelloFMLTransformer {

  public static void main(String[] args) {
    DialectMeta dialectMeta = DialectMeta.DEFAULT_HIVE;
    Transformer<BaseStatement> statementTransformer = TransformerFactory.getInstance()
                    .get(dialectMeta);
    statementTransformer.transform(statement, context).getNode();
  }
}
```

## Building FML from Source

FML构建准备条件：

* Unix-like environment (we use Linux, Mac OS X, Cygwin, WSL)
* Git
* Maven (we recommend version 3.5.0+)
* Java 8

```
git clone git@github.com:alibaba/fast-modeling-language.git
cd fast-modeling-language
mvn clean package -DskipTests # this will take up to 10 minutes
```

## Developing FML

FML提交者使用IntelliJ IDEA来开发FML代码库，我们推荐IntelliJ IDEA开发Java工程

IDE的最小支持集包括:

* 支持Java工程
* 支持Maven

### IntelliJ IDEA

IntelliJ IDE 支持以下插件

* IntelliJ 下载地址: [https://www.jetbrains.com/idea/](https://www.jetbrains.com/idea/)
* IntelliJ ANTLR
  插件地址: [https://plugins.jetbrains.com/plugin/7358-antlr-v4](https://plugins.jetbrains.com/plugin/7358-antlr-v4)
* 代码样式模板文件：`docs/Alibaba_CodeStyle.xml`

## Documentation

FML语法介绍可以在 `docs/` 目录查看源文件, docs编写使用了：`docsify` 进行文档管理，具体使用可以参考：
[docsify quickstart](https://docsify.js.org/#/quickstart)
- 本地执行：`docsify serve docs`
- 访问：http://localhost:3000

## Related Product
* [DataWorks智能数据建模](https://help.aliyun.com/document_detail/276018.html)
![DataWorks](https://help-static-aliyun-doc.aliyuncs.com/assets/img/zh-CN/8759336261/p295134.png)

## Notice
fast-modeling-language is a modeling DSL developed by Alibaba and licensed under the Apache License (Version 2.0) This product contains various third-party components under other open source licenses.
See the NOTICE file for more information.
