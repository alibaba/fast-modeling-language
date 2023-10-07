# 建表SDK

[maven引入](zh-cn/transformer/guide.md)

## 先从一个demo开始

```java
import java.util.List;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.transform.api.client.CodeGenerator;
import com.aliyun.fastmodel.transform.api.client.dto.request.DdlGeneratorModelRequest;
import com.aliyun.fastmodel.transform.api.client.dto.request.DdlReverseSqlRequest;
import com.aliyun.fastmodel.transform.api.client.dto.result.DdlGeneratorResult;
import com.aliyun.fastmodel.transform.api.client.dto.result.DdlTableResult;
import com.aliyun.fastmodel.transform.api.client.dto.table.Column;
import com.aliyun.fastmodel.transform.api.client.dto.table.Table;
import com.aliyun.fastmodel.transform.api.client.dto.table.TableConfig;
import com.aliyun.fastmodel.transform.api.client.generator.DefaultCodeGenerator;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class Test {
    @Test
    public void testHive() {
        CodeGenerator codeGenerator = new DefaultCodeGenerator();
        Table table = getTable("c1", "c1", 1000L);
        TableConfig config = TableConfig.builder()
            .dialectMeta(DialectMeta.DEFAULT_HIVE)  //这里根据目标引擎选择
            .caseSensitive(false)
            .build();
        DdlGeneratorModelRequest request = DdlGeneratorModelRequest.builder()
            .after(table)
            .config(config)
            .build();
        DdlGeneratorResult generate = codeGenerator.generate(request);
        List<DialectNode> dialectNodes = generate.getDialectNodes();
        assertEquals(dialectNodes.stream().map(DialectNode::getNode).collect(Collectors.joining(";\n")), "CREATE TABLE IF NOT EXISTS a\n"
            + "(\n"
            + "   c1 STRING COMMENT 'comment'\n"
            + ")\n");
    }

    private Table getTable(String id, String name, long lifeCycleSeconds) {
        List<Column> columns = getColumns(id, name);
        return Table.builder()
            .name("a")
            .columns(columns)
            .lifecycleSeconds(lifeCycleSeconds)
            .build();
    }

    private List<Column> getColumns(String id, String name) {
        return Lists.newArrayList(
            Column.builder()
                .id(id)
                .name(name)
                .dataType("BIGINT")
                .comment("comment")
                .build()
        );
    }
}

```

## 步骤解析

将Table转为转为DDL语句，需要经历过以下的步骤:

1. 将Table模型转为FML模型
2. 将FML模型输出到具体的方言的DDL语句
3. 支持全量和增量的比对的生成，借助FML的Compare的能力生成增量模型
4. 将增量模型转为目标的引擎的DDL语句

## 模型示意图

```plantuml
package client <<用于对外的客户端的操作>>{
	 class Table {
		     type : TableType
	 		 database : String
              schema : String
             name : String
		     comment : String
		     columns : List<Column>
		     lifecycleSeconds : Long
		     constraints : List<Constraint>
		     properties : List<BaseClientProperty>
	 }
	
	
	 class Constraint {
	       name : String 
		   columnName : List<String>
	 }
	
	 class BaseClientProperty<T> {
	       name : String
           value : T
	}
	 
	
	  class Column {
        id : String
        name : String
        dataType : String
        length: Integer
        precision: Integer
        scale : Integer
        comment : String
        nullable : boolean
        primaryKey : boolean
        partitionKey : boolean
        partitionKeyIndex : Integer
	 }
	
	Table *-- Column
	Table *-- Constraint
	Table *-- BaseClientProperty
	 
	
}
```



