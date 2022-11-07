/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.hologres.client.generator;

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

/**
 * code generator
 *
 * @author panguanjing
 * @date 2022/6/10
 */
public class HologresCodeGeneratorModelTest {

    @Test
    public void testGeneratorNewTable() {
        CodeGenerator codeGenerator = new DefaultCodeGenerator();
        Table table = getTable("c1", "c1", 1000L);
        TableConfig config = TableConfig.builder()
            .dialectMeta(DialectMeta.DEFAULT_HOLO)
            .caseSensitive(false)
            .build();
        DdlGeneratorModelRequest request = DdlGeneratorModelRequest.builder()
            .after(table)
            .config(config)
            .build();
        DdlGeneratorResult generate = codeGenerator.generate(request);
        List<DialectNode> dialectNodes = generate.getDialectNodes();
        assertEquals(dialectNodes.stream().map(DialectNode::getNode).collect(Collectors.joining(",\n")), "BEGIN;\n"
            + "CREATE TABLE IF NOT EXISTS a (\n"
            + "   c1 TEXT NOT NULL\n"
            + ");\n"
            + "CALL SET_TABLE_PROPERTY('a', 'time_to_live_in_seconds', '1000');\n"
            + "COMMENT ON COLUMN a.c1 IS 'comment';\n"
            + "COMMIT;");
    }

    @Test
    public void testGeneratorNewTableWithBefore() {
        CodeGenerator codeGenerator = new DefaultCodeGenerator();
        Table table = getTable("c1", "c1", 1000L);
        TableConfig config = TableConfig.builder()
            .dialectMeta(DialectMeta.DEFAULT_HOLO)
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
        assertEquals(dialectNodes.stream().map(DialectNode::getNode).collect(Collectors.joining(",\n")), "BEGIN;\n"
            + "CALL SET_TABLE_PROPERTY('a', 'time_to_live_in_seconds', '1000');\n"
            + "COMMIT;");
    }

    @Test
    public void testGeneratorNewTableWithRenameColumn() {
        CodeGenerator codeGenerator = new DefaultCodeGenerator();
        Table table = getTable("c1", "c2", 1000L);
        TableConfig config = TableConfig.builder()
            .dialectMeta(DialectMeta.DEFAULT_HOLO)
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
        assertEquals(dialectNodes.stream().map(DialectNode::getNode).collect(Collectors.joining("\n")), "ALTER TABLE a RENAME COLUMN c1 TO c2;\n"
            + "BEGIN;\n"
            + "CALL SET_TABLE_PROPERTY('a', 'time_to_live_in_seconds', '1000');\n"
            + "COMMIT;");
    }

    @Test
    public void testGeneratorNewTableWithAddColumn() {
        CodeGenerator codeGenerator = new DefaultCodeGenerator();
        Table table = getTable("c1", "c2", 1000L);
        table.getColumns().add(
            Column.builder()
                .name("c3")
                .dataType("json")
                .build()
        );
        TableConfig config = TableConfig.builder()
            .dialectMeta(DialectMeta.DEFAULT_HOLO)
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
        assertEquals(dialectNodes.stream().map(DialectNode::getNode).collect(Collectors.joining("\n")), "ALTER TABLE a RENAME COLUMN c1 TO c2;\n"
            + "BEGIN;\n"
            + "ALTER TABLE IF EXISTS a ADD COLUMN c3 JSON;\n"
            + "COMMIT;\n"
            + "BEGIN;\n"
            + "CALL SET_TABLE_PROPERTY('a', 'time_to_live_in_seconds', '1000');\n"
            + "COMMIT;");
    }

    @Test
    public void testGeneratorNewTableWithDropColumn() {
        CodeGenerator codeGenerator = new DefaultCodeGenerator();
        Table table = getTable("c1", "c2", 1000L);
        TableConfig config = TableConfig.builder()
            .dialectMeta(DialectMeta.DEFAULT_HOLO)
            .caseSensitive(false)
            .build();
        Table beforeTable = getTable("c1", "c1", 500L);
        beforeTable.getColumns().add(
            Column.builder()
                .name("c3")
                .dataType("json")
                .build()
        );
        DdlGeneratorModelRequest request = DdlGeneratorModelRequest.builder()
            .before(beforeTable)
            .after(table)
            .config(config)
            .build();
        DdlGeneratorResult generate = codeGenerator.generate(request);
        List<DialectNode> dialectNodes = generate.getDialectNodes();
        assertEquals(dialectNodes.stream().map(DialectNode::getNode).collect(Collectors.joining("\n")), "ALTER TABLE a RENAME COLUMN c1 TO c2;\n"
            + "ALTER TABLE a DROP COLUMN c3;\n"
            + "BEGIN;\n"
            + "CALL SET_TABLE_PROPERTY('a', 'time_to_live_in_seconds', '1000');\n"
            + "COMMIT;");
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
            .dialectMeta(DialectMeta.DEFAULT_HOLO)
            .caseSensitive(false)
            .build();

        DdlGeneratorModelRequest request = DdlGeneratorModelRequest.builder()
            .before(null)
            .after(table)
            .config(config)
            .build();
        DdlGeneratorResult generate = codeGenerator.generate(request);
        List<DialectNode> dialectNodes = generate.getDialectNodes();
        assertEquals(dialectNodes.stream().map(DialectNode::getNode).collect(Collectors.joining(",\n")), "BEGIN;\n"
            + "CREATE TABLE IF NOT EXISTS sc.tb (\n"
            + "   c1 TEXT NOT NULL\n"
            + ");\n"
            + "CALL SET_TABLE_PROPERTY('sc.tb', 'orientation', 'column');\n"
            + "CALL SET_TABLE_PROPERTY('sc.tb', 'time_to_live_in_seconds', '3153600000');\n"
            + "COMMENT ON COLUMN sc.tb.c1 IS 'comment';\n"
            + "COMMIT;");
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
                .dataType("text")
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
            .code("create table if not exists a (b text);")
            .dialectMeta(DialectMeta.DEFAULT_HOLO).build();
        DdlTableResult reverse = defaultCodeGenerator.reverse(request);
        Table table = reverse.getTable();
        assertEquals(table.getDatabase(), "d1");
        assertEquals(table.getSchema(), "c1");
        assertEquals(table.getName(), "a");
        assertEquals(table.getColumns().size(), 1);
    }
}
