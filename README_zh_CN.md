> 语言： [中文](https://alibaba.github.io/fast-modeling-language/) | English

# Fast Modeling Language

FML（Fast Modeling Language）是一种类似SQL的语言，专为维度建模而设计。它旨在实现快速建模，遵循"Kimball维度建模技术理论"和"阿里巴巴OneData理论"。FML从SQL中继承了许多特性。例如，在创建模型时，FML研究并参考了标准的DDL语法，并在此基础上进行了扩展。在设计模型时，您不再需要关注不同种类大数据引擎的具体规则。FML转换器将把您的模型转换为SQL语法，可以直接被特定类型的引擎读取。

### 特性

- 一种专门为维度建模设计的类SQL语言。
- 支持在数据仓库建立过程中定义语法，如：数仓规划、字段标准、码表、数据指标。
- 基于Java构建，您可以使用语法结构API轻松构建模型。
- 使用Transform API将模型转换为常见引擎的各种语法，如：Hive、Hologres、Mysql。
- 支持JDBC驱动模式，与模型引擎交互。


相关文档（持续更新中）: https://alibaba.github.io/fast-modeling-language/

### 解析示例

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

### Transformer示例

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

## 源码构建

环境要求：

- 类Unix环境（我们使用Linux、Mac OS X、Cygwin、WSL）
- Git
- Maven3.5.0+ 
- Java 8

下载和构建

```Shell
git clone git@github.com:alibaba/fast-modeling-language.git
cd fast-modeling-language
mvn clean package -DskipTests # this will take up to 10 minutes
```

## 开发FML

FML的开发者可以使用IntelliJ IDEA来开发。我们提供IntelliJ IDEA给开发者，他们想提交FML代码库的开发者。
开发者的IDE需要支持至少：

- Java项目开发
- 支持Maven

IntelliJ IDEA IntelliJ IDEA插件

- IntelliJ地址: https://www.jetbrains.com/idea/
- IntelliJ ANTLR插件: https://plugins.jetbrains.com/plugin/7358-antlr-v4
- Code style 模板: docs/Alibaba_CodeStyle.xml
- ErrorProne: https://errorprone.info/docs/installation



## 文档

FML文档的介绍和指南可以在doc/目录下找到，文档是通过Docsify管理的，要了解更多信息请参阅：docsify快速入门
- 本地执行: `docsify serve docs`
- 之后访问： http://localhost:3000

