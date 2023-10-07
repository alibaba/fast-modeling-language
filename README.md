> 语言： [中文](https://alibaba.github.io/fast-modeling-language/) | English

# Fast Modeling Language

FML(Fast Modeling Language) is a SQL-like language designed for Dimensional Modeling. It is an attempt for fast modeling follows the “Kimball
Dimensional Modeling Techniques Theory” and the “Alibaba’s OneData Theory.“ FML inherits many features from the SQL. For example, the FML studied and
consulted the standard DDL syntax when creating a model, and extensions upon it. While designing a model, you no longer have to care about the
specific rules for different kinds of big data engines. The FML Transformer will convert your models into a SQL syntax, which can be read directly by
a certain kind of engine.

### Features

- A SQL-like language specially designed dimensional modeling.
- Supports syntax definition in the process of data warehouse establishment, such as: Data Warehouse Planning, Fields Standards, Code Tables, Data
  Indicators.
- Build with Java, you can build models easily with the Syntax Nodes Construct API.
- Transforms models into varies syntax with the Transform API for common engines, such as: Hive, Hologres, Mysql.
- Supports JDBC Driver mode, interact with the model engine.

Relation Document: https://alibaba.github.io/fast-modeling-language/

### Parser Example

```java

import com.aliyun.fastmodel.core.parser.FastModelParser;
import com.aliyun.fastmodel.core.parser.FastModelParserFactory;

public class HelloFML {
    //单例
    private static final FastModelParser FAST_MODEL_PARSER = FastModelParserFactory.getInstance().get();

    public static void main(String[] args) {
        String fml
            = "create dim table t_1 alias 'alias_name' (col bigint alias 'alias_name' comment 'col_comment') comment 'comment';";
        CreateDimTable createDimTable = FAST_MODEL_PARSER.parseStatement(fml);
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
        DialectMeta dialectMeta =
            Transformer < BaseStatement > statementTransformer = TransformerFactory.getInstance().get(dialectMeta);
        statementTransformer.transform(statement, context).getNode();
    }
}
```

## Building FML from Source

Build environment and conditions for FML

- Unix-like environment (we use Linux, Mac OS X, Cygwin, WSL)
- Git
- Maven (version 3.5.0+ recommended)
- Java 8

Clone and build

```Shell
git clone git@github.com:alibaba/fast-modeling-language.git
cd fast-modeling-language
mvn clean package -DskipTests # this will take up to 10 minutes
```

## Develop FML

IntelliJ IDEA is recommended for the FML Java project. We provide IntelliJ IDEA to developers who would like to commit to the FML code base.
Developer’s IDE need to support at least:

- Java project development
- Support Maven

IntelliJ IDEA IntelliJ IDEA Plugins

- IntelliJ地址: https://www.jetbrains.com/idea/
- IntelliJ ANTLR: https://plugins.jetbrains.com/plugin/7358-antlr-v4
- Code style template: docs/Alibaba_CodeStyle.xml
- ErrorProne: https://errorprone.info/docs/installation

## Documentation

FML文档可The introduction and guide of the FML can be found under the `doc/` directory, the docs is managed with the Docsify, to learn more about it: docsify quickstart

- Run locally: `docsify serve docs`
- Then visit http://localhost:3000

