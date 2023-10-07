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
import com.aliyun.fastmodel.transform.api.client.dto.request.DdlGeneratorSqlRequest;
import com.aliyun.fastmodel.transform.api.client.dto.result.DdlGeneratorResult;
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
public class HologresCodeGeneratorSqlTest {

    @Test
    public void testGeneratorNewTable() {
        CodeGenerator codeGenerator = new DefaultCodeGenerator();
        List<Column> columns = Lists.newArrayList(
            Column.builder()
                .name("c1")
                .dataType("text")
                .comment("comment")
                .build()
        );
        Table table = Table.builder()
            .name("a")
            .columns(columns)
            .lifecycleSeconds(1000L)
            .build();
        TableConfig config = TableConfig.builder()
            .dialectMeta(DialectMeta.DEFAULT_HOLO)
            .caseSensitive(false)
            .build();
        DdlGeneratorSqlRequest request = DdlGeneratorSqlRequest.builder()
            .before("create table if not exists a (c1 varchar);")
            .after(table)
            .config(config)
            .build();
        DdlGeneratorResult generate = codeGenerator.generate(request);
        List<DialectNode> dialectNodes = generate.getDialectNodes();
        String dialectNode = dialectNodes.stream().filter(DialectNode::isExecutable).map(DialectNode::getNode).collect(Collectors.joining("\n"));
        assertEquals(dialectNode, "BEGIN;\n"
            + "COMMENT ON COLUMN a.c1 IS 'comment';\n"
            + "COMMIT;\n"
            + "BEGIN;\n"
            + "CALL SET_TABLE_PROPERTY('a', 'time_to_live_in_seconds', '1000');\n"
            + "COMMIT;");
    }

    @Test
    public void testGeneratorNewTableDouble() {
        CodeGenerator codeGenerator = new DefaultCodeGenerator();
        List<Column> columns = Lists.newArrayList(
            Column.builder()
                .name("\"c1.abc\"")
                .dataType("DOUBLE PRECISION")
                .comment("comment")
                .build()
        );
        Table table = Table.builder()
            .name("\"a\"")
            .columns(columns)
            .lifecycleSeconds(1000L)
            .build();
        TableConfig config = TableConfig.builder()
            .dialectMeta(DialectMeta.DEFAULT_HOLO)
            .caseSensitive(false)
            .build();
        DdlGeneratorSqlRequest request = DdlGeneratorSqlRequest.builder()
            .after(table)
            .config(config)
            .build();
        DdlGeneratorResult generate = codeGenerator.generate(request);
        List<DialectNode> dialectNodes = generate.getDialectNodes();
        String dialectNode = dialectNodes.stream().filter(DialectNode::isExecutable).map(DialectNode::getNode).collect(Collectors.joining("\n"));
        assertEquals(dialectNode, "BEGIN;\n"
            + "CREATE TABLE IF NOT EXISTS \"a\" (\n"
            + "   \"c1.abc\" DOUBLE PRECISION NOT NULL\n"
            + ");\n"
            + "CALL SET_TABLE_PROPERTY('a', 'time_to_live_in_seconds', '1000');\n"
            + "COMMENT ON COLUMN \"a\".\"c1.abc\" IS 'comment';\n"
            + "COMMIT;");
    }

    @Test
    public void testGeneratorNewTable2() {
        CodeGenerator codeGenerator = new DefaultCodeGenerator();
        List<Column> columns = Lists.newArrayList(
            Column.builder()
                .name("c1.abc")
                .dataType("DOUBLE PRECISION")
                .comment("comment")
                .build()
        );
        Table table = Table.builder()
            .name("\"a\"")
            .columns(columns)
            .lifecycleSeconds(1000L)
            .build();
        TableConfig config = TableConfig.builder()
            .dialectMeta(DialectMeta.DEFAULT_HOLO)
            .caseSensitive(false)
            .build();
        DdlGeneratorSqlRequest request = DdlGeneratorSqlRequest.builder()
            .after(table)
            .config(config)
            .build();
        DdlGeneratorResult generate = codeGenerator.generate(request);
        List<DialectNode> dialectNodes = generate.getDialectNodes();
        String dialectNode = dialectNodes.stream().filter(DialectNode::isExecutable).map(DialectNode::getNode).collect(Collectors.joining("\n"));
        assertEquals(dialectNode, "BEGIN;\n"
            + "CREATE TABLE IF NOT EXISTS \"a\" (\n"
            + "   \"c1.abc\" DOUBLE PRECISION NOT NULL\n"
            + ");\n"
            + "CALL SET_TABLE_PROPERTY('a', 'time_to_live_in_seconds', '1000');\n"
            + "COMMENT ON COLUMN \"a\".\"c1.abc\" IS 'comment';\n"
            + "COMMIT;");
    }

    @Test
    public void testGeneratorBigSerial() {
        CodeGenerator codeGenerator = new DefaultCodeGenerator();
        List<Column> columns = Lists.newArrayList(
            Column.builder()
                .name("\"c1.abc\"")
                .dataType("BIGSERIAL")
                .comment("comment")
                .build()
        );
        Table table = Table.builder()
            .name("\"a\"")
            .columns(columns)
            .lifecycleSeconds(1000L)
            .build();
        TableConfig config = TableConfig.builder()
            .dialectMeta(DialectMeta.DEFAULT_HOLO)
            .caseSensitive(false)
            .build();
        DdlGeneratorSqlRequest request = DdlGeneratorSqlRequest.builder()
            .after(table)
            .config(config)
            .build();
        DdlGeneratorResult generate = codeGenerator.generate(request);
        List<DialectNode> dialectNodes = generate.getDialectNodes();
        String dialectNode = dialectNodes.stream().filter(DialectNode::isExecutable).map(DialectNode::getNode).collect(Collectors.joining("\n"));
        assertEquals(dialectNode, "BEGIN;\n"
            + "CREATE TABLE IF NOT EXISTS \"a\" (\n"
            + "   \"c1.abc\" BIGSERIAL NOT NULL\n"
            + ");\n"
            + "CALL SET_TABLE_PROPERTY('a', 'time_to_live_in_seconds', '1000');\n"
            + "COMMENT ON COLUMN \"a\".\"c1.abc\" IS 'comment';\n"
            + "COMMIT;");
    }
}
