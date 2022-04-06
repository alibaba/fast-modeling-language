/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.mc.client;

import java.util.List;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.transform.api.client.CodeGenerator;
import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.api.client.dto.request.DdlGeneratorModelRequest;
import com.aliyun.fastmodel.transform.api.client.dto.request.DdlGeneratorSqlRequest;
import com.aliyun.fastmodel.transform.api.client.dto.request.DdlReverseSqlRequest;
import com.aliyun.fastmodel.transform.api.client.dto.result.DdlGeneratorResult;
import com.aliyun.fastmodel.transform.api.client.dto.result.DdlTableResult;
import com.aliyun.fastmodel.transform.api.client.dto.table.Column;
import com.aliyun.fastmodel.transform.api.client.dto.table.Table;
import com.aliyun.fastmodel.transform.api.client.dto.table.TableConfig;
import com.aliyun.fastmodel.transform.api.client.generator.DefaultCodeGenerator;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.mc.client.property.LifeCycle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * code generator
 *
 * @author panguanjing
 * @date 2022/6/10
 */
public class MaxComputeCodeGeneratorModelTest {

    @Test
    public void testGeneratorNewTable() {
        CodeGenerator codeGenerator = new DefaultCodeGenerator();
        Table table = getTable("c1", "c1", 1000L);
        TableConfig config = TableConfig.builder()
            .dialectMeta(DialectMeta.DEFAULT_MAX_COMPUTE)
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
            + ")\n"
            + "LIFECYCLE 1;");
    }

    @Test
    public void testGeneratorNewTableWithBefore() {
        CodeGenerator codeGenerator = new DefaultCodeGenerator();
        Table table = getTable("c1", "c1", 864000L);
        TableConfig config = TableConfig.builder()
            .dialectMeta(DialectMeta.DEFAULT_MAX_COMPUTE)
            .caseSensitive(false)
            .build();
        Table beforeTable = getTable("c1", "c1", 0L);
        DdlGeneratorModelRequest request = DdlGeneratorModelRequest.builder()
            .before(beforeTable)
            .after(table)
            .config(config)
            .build();
        DdlGeneratorResult generate = codeGenerator.generate(request);
        List<DialectNode> dialectNodes = generate.getDialectNodes();
        assertEquals(dialectNodes.stream().map(DialectNode::getNode).collect(Collectors.joining("\n")), "ALTER TABLE a ENABLE LIFECYCLE;\n"
            + "ALTER TABLE a SET LIFECYCLE 10;");
    }

    @Test
    public void testGeneratorNewTableWithRenameColumn() {
        CodeGenerator codeGenerator = new DefaultCodeGenerator();
        Table table = getTable("c1", "c2", 1000L);
        TableConfig config = TableConfig.builder()
            .dialectMeta(DialectMeta.DEFAULT_MAX_COMPUTE)
            .caseSensitive(false)
            .build();
        Table beforeTable = getTable("c1", "c1", 500L);
        DdlGeneratorModelRequest request = DdlGeneratorModelRequest.builder()
            .before(beforeTable)
            .after(table)
            .config(config)
            .build();
        DdlGeneratorResult generate = codeGenerator.generate(request);
        List<DialectNode> dialectNodes = generate.getDialectNodes();
        assertEquals(dialectNodes.stream().map(DialectNode::getNode).collect(Collectors.joining("\n")), "ALTER TABLE a CHANGE COLUMN c1 c2 STRING;");
    }

    @Test
    public void testGeneratorNewTableWithAddColumn() {
        CodeGenerator codeGenerator = new DefaultCodeGenerator();
        Table table = getTable("c1", "c2", 1000L);
        table.getColumns().add(
            Column.builder()
                .name("c3")
                .dataType("decimal")
                .precision(10)
                .scale(10)
                .build()
        );
        TableConfig config = TableConfig.builder()
            .dialectMeta(DialectMeta.DEFAULT_MAX_COMPUTE)
            .caseSensitive(false)
            .build();
        Table beforeTable = getTable("c1", "c1", 500L);
        DdlGeneratorModelRequest request = DdlGeneratorModelRequest.builder()
            .before(beforeTable)
            .after(table)
            .config(config)
            .build();
        DdlGeneratorResult generate = codeGenerator.generate(request);
        List<DialectNode> dialectNodes = generate.getDialectNodes();
        assertEquals(dialectNodes.stream().map(DialectNode::getNode).collect(Collectors.joining("\n")), "ALTER TABLE a CHANGE COLUMN c1 c2 STRING;\n"
            + "ALTER TABLE a ADD COLUMNS\n"
            + "(\n"
            + "   c3 DECIMAL(10,10)\n"
            + ");");
    }

    @Test
    public void testGeneratorNewTableWithDropColumn() {
        CodeGenerator codeGenerator = new DefaultCodeGenerator();
        Table table = getTable("c1", "c2", 1000L);
        TableConfig config = TableConfig.builder()
            .dialectMeta(DialectMeta.DEFAULT_MAX_COMPUTE)
            .caseSensitive(false)
            .build();
        Table beforeTable = getTable("c1", "c1", 500L);
        beforeTable.getColumns().add(
            Column.builder()
                .name("c3")
                .dataType("array<string>")
                .build()
        );
        DdlGeneratorModelRequest request = DdlGeneratorModelRequest.builder()
            .before(beforeTable)
            .after(table)
            .config(config)
            .build();
        DdlGeneratorResult generate = codeGenerator.generate(request);
        List<DialectNode> dialectNodes = generate.getDialectNodes();
        assertEquals(dialectNodes.stream().map(DialectNode::getNode).collect(Collectors.joining("\n")), "ALTER TABLE a CHANGE COLUMN c1 c2 STRING;\n"
            + "ALTER TABLE a DROP COLUMN c3;");
    }

    @Test
    public void testGeneratorNewTableWithSchema() {
        CodeGenerator codeGenerator = new DefaultCodeGenerator();
        Table table = Table.builder()
            .schema("sc")
            .name("tb")
            .columns(getColumns("c1", "c1"))
            .build();
        TableConfig config = TableConfig.builder()
            .dialectMeta(DialectMeta.DEFAULT_MAX_COMPUTE)
            .caseSensitive(false)
            .build();

        DdlGeneratorModelRequest request = DdlGeneratorModelRequest.builder()
            .before(null)
            .after(table)
            .config(config)
            .build();
        DdlGeneratorResult generate = codeGenerator.generate(request);
        List<DialectNode> dialectNodes = generate.getDialectNodes();
        assertEquals(dialectNodes.stream().map(DialectNode::getNode).collect(Collectors.joining(",\n")), "CREATE TABLE IF NOT EXISTS sc.tb\n"
            + "(\n"
            + "   c1 STRING COMMENT 'comment'\n"
            + ")\n;");
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
                .dataType("string")
                .comment("comment")
                .build()
        );
    }

    @Test
    public void testConvertToTable() {
        DefaultCodeGenerator defaultCodeGenerator = new DefaultCodeGenerator();
        DdlReverseSqlRequest request = DdlReverseSqlRequest.builder()
            .database("d1")
            .schema("c1")
            .code("create table if not exists a (b string);")
            .dialectMeta(DialectMeta.DEFAULT_MAX_COMPUTE).build();
        DdlTableResult reverse = defaultCodeGenerator.reverse(request);
        Table table = reverse.getTable();
        assertEquals(table.getDatabase(), "d1");
        assertEquals(table.getSchema(), "c1");
        assertEquals(table.getName(), "a");
        assertEquals(table.getColumns().size(), 1);
    }

    @Test
    public void testConvertToTableIssueLifecycle() {
        DefaultCodeGenerator defaultCodeGenerator = new DefaultCodeGenerator();
        DdlGeneratorModelRequest request1 = DdlGeneratorModelRequest.builder()
            .config(TableConfig.builder().dialectMeta(DialectMeta.DEFAULT_MAX_COMPUTE).build())
            .before(Table.builder().name("a")
                .lifecycleSeconds(365000000000L)
                .columns(
                Lists.newArrayList(Column.builder().name("b").dataType("bigint")
                    .build())
            ).build())
            .after(Table.builder()
                .lifecycleSeconds(null)
                .name("a")
                .columns(Lists.newArrayList(
                    Column.builder()
                        .name("b")
                        .dataType("bigint")
                        .build()
                ))
                .build()
            ).build();
        DdlGeneratorResult reverse = defaultCodeGenerator.generate(request1);
        List<DialectNode> dialectNodes = reverse.getDialectNodes();
        assertEquals(1, dialectNodes.size());
        assertEquals(dialectNodes.get(0).getNode(), "ALTER TABLE a DISABLE LIFECYCLE;");
    }

    @Test
    public void testGeneratorWithTwoPartition() {
        DefaultCodeGenerator defaultCodeGenerator = new DefaultCodeGenerator();
        Column c1 = Column.builder()
            .name("pt")
            .dataType("bigint")
            .partitionKey(true)
            .partitionKeyIndex(0)
            .build();
        Column c2 = Column.builder()
            .name("c2")
            .dataType("bigint")
            .partitionKey(true)
            .partitionKeyIndex(1)
            .build();
        Column c3 = Column.builder()
            .name("c3")
            .dataType("bigint")
            .partitionKey(false)
            .build();
        List<Column> columns = ImmutableList.of(
            c1,
            c2,
            c3
        );
        Table table = Table.builder()
            .name("abc")
            .columns(columns)
            .build();
        DdlGeneratorModelRequest modelRequest = DdlGeneratorModelRequest.builder()
            .after(table)
            .config(TableConfig.builder().dialectMeta(DialectMeta.DEFAULT_MAX_COMPUTE).build())
            .build();
        DdlGeneratorResult generate = defaultCodeGenerator.generate(modelRequest);
        assertEquals(generate.getDialectNodes().get(0).getNode(), "CREATE TABLE IF NOT EXISTS abc\n"
            + "(\n"
            + "   c3 BIGINT\n"
            + ")\n"
            + "PARTITIONED BY\n"
            + "(\n"
            + "   pt BIGINT,\n"
            + "   c2 BIGINT\n"
            + ")\n;");
    }
}
