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
import com.aliyun.fastmodel.transform.api.client.dto.result.DdlGeneratorResult;
import com.aliyun.fastmodel.transform.api.client.dto.table.Column;
import com.aliyun.fastmodel.transform.api.client.dto.table.Table;
import com.aliyun.fastmodel.transform.api.client.dto.table.TableConfig;
import com.aliyun.fastmodel.transform.api.client.generator.DefaultCodeGenerator;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.hologres.parser.tree.datatype.HologresDataTypeName;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Test set property
 *
 * @author panguanjing
 * @date 2022/7/12
 */
public class HologresCodeGeneratorSetDefaultValueTest {
    @Test
    public void testSetDefaultValueFromValueToNull() {
        CodeGenerator codeGenerator = new DefaultCodeGenerator();
        List<Column> beforeColumn = ImmutableList.of(
            Column.builder()
                .name("c1")
                .dataType("text")
                .defaultValue("abc")
                .build()
        );
        List<Column> afterColumn = ImmutableList.of(
            Column.builder()
                .name("c1")
                .dataType("text")
                .defaultValue(null)
                .build()
        );
        DdlGeneratorModelRequest request = DdlGeneratorModelRequest.builder()
            .before(Table.builder().name("a").columns(beforeColumn).build())
            .after(Table.builder().name("a").columns(afterColumn).build())
            .config(TableConfig.builder().dialectMeta(DialectMeta.DEFAULT_HOLO).build())
            .build();
        DdlGeneratorResult generate = codeGenerator.generate(request);
        String result =
            generate.getDialectNodes().stream().filter(DialectNode::isExecutable).map(DialectNode::getNode).collect(Collectors.joining("\n"));
        assertEquals(result, "BEGIN;\n"
            + "ALTER TABLE a ALTER COLUMN c1 DROP DEFAULT;\n"
            + "COMMIT;");
    }

    @Test
    public void testSetDefaultFromNullToValue() {
        CodeGenerator codeGenerator = new DefaultCodeGenerator();
        List<Column> beforeColumn = ImmutableList.of(
            Column.builder()
                .name("c1")
                .dataType("text")
                .build()
        );
        List<Column> afterColumn = ImmutableList.of(
            Column.builder()
                .name("c1")
                .dataType("text")
                .defaultValue("null")
                .build()
        );
        DdlGeneratorModelRequest request = DdlGeneratorModelRequest.builder()
            .before(Table.builder().name("a").columns(beforeColumn).build())
            .after(Table.builder().name("a").columns(afterColumn).build())
            .config(TableConfig.builder().dialectMeta(DialectMeta.DEFAULT_HOLO).build())
            .build();
        DdlGeneratorResult generate = codeGenerator.generate(request);
        String result =
            generate.getDialectNodes().stream().filter(DialectNode::isExecutable).map(DialectNode::getNode).collect(Collectors.joining("\n"));
        assertEquals("BEGIN;\n"
            + "ALTER TABLE a ALTER COLUMN c1 SET DEFAULT null;\n"
            + "COMMIT;", result);
    }

    @Test
    public void testSetDefaultValueTimestamp() {
        CodeGenerator codeGenerator = new DefaultCodeGenerator();
        List<Column> beforeColumn = ImmutableList.of(
            Column.builder()
                .name("c1")
                .dataType(HologresDataTypeName.TIMESTAMP.getValue())
                .defaultValue(null)
                .build()
        );
        List<Column> afterColumn = ImmutableList.of(
            Column.builder()
                .name("c1")
                .dataType(HologresDataTypeName.TIMESTAMP.getValue())
                .defaultValue("CURRENT_TIMESTAMP")
                .build()
        );
        DdlGeneratorModelRequest request = DdlGeneratorModelRequest.builder()
            .before(Table.builder().name("a").columns(beforeColumn).build())
            .after(Table.builder().name("a").columns(afterColumn).build())
            .config(TableConfig.builder().dialectMeta(DialectMeta.DEFAULT_HOLO).build())
            .build();
        DdlGeneratorResult generate = codeGenerator.generate(request);
        String result =
            generate.getDialectNodes().stream().filter(DialectNode::isExecutable).map(DialectNode::getNode).collect(Collectors.joining("\n"));
        assertEquals("BEGIN;\n"
            + "ALTER TABLE a ALTER COLUMN c1 SET DEFAULT CURRENT_TIMESTAMP;\n"
            + "COMMIT;", result);
    }

    @Test
    public void testSetDefaultValueDouble() {
        CodeGenerator codeGenerator = new DefaultCodeGenerator();
        List<Column> columns = ImmutableList.of(
            Column.builder()
                .name("c1")
                .dataType(HologresDataTypeName.DOUBLE_PRECISION.getValue())
                .defaultValue("0.81")
                .build()
        );

        DdlGeneratorModelRequest request = DdlGeneratorModelRequest.builder()
            .after(Table.builder().name("t_double_1").columns(columns).build())
            .config(TableConfig.builder().dialectMeta(DialectMeta.DEFAULT_HOLO).build())
            .build();
        DdlGeneratorResult generate = codeGenerator.generate(request);
        String result =
            generate.getDialectNodes().stream().filter(DialectNode::isExecutable).map(DialectNode::getNode).collect(Collectors.joining("\n"));
        assertEquals("BEGIN;\n"
            + "CREATE TABLE IF NOT EXISTS t_double_1 (\n"
            + "   c1 DOUBLE PRECISION NOT NULL DEFAULT 0.81\n"
            + ");\n\n"
            + "COMMIT;", result);
    }

    @Test
    public void testSetDefaultValueCurrentTimestamp() {
        CodeGenerator codeGenerator = new DefaultCodeGenerator();
        List<Column> columns = ImmutableList.of(
            Column.builder()
                .name("c1")
                .dataType(HologresDataTypeName.TEXT.getValue())
                .defaultValue("'current_timestamp'")
                .build()
        );
        DdlGeneratorModelRequest request = DdlGeneratorModelRequest.builder()
            .after(Table.builder().name("t_double_1").columns(columns).build())
            .config(TableConfig.builder().dialectMeta(DialectMeta.DEFAULT_HOLO).build())
            .build();
        DdlGeneratorResult generate = codeGenerator.generate(request);
        String result =
            generate.getDialectNodes().stream().filter(DialectNode::isExecutable).map(DialectNode::getNode).collect(Collectors.joining("\n"));
        assertEquals("BEGIN;\n"
            + "CREATE TABLE IF NOT EXISTS t_double_1 (\n"
            + "   c1 TEXT NOT NULL DEFAULT 'current_timestamp'\n"
            + ");\n\n"
            + "COMMIT;", result);
    }

    @Test
    public void testSetDefaultValueWithQuoteCurrentTimestamp() {
        CodeGenerator codeGenerator = new DefaultCodeGenerator();
        List<Column> columns = ImmutableList.of(
            Column.builder()
                .name("c1")
                .dataType(HologresDataTypeName.TEXT.getValue())
                .defaultValue("''current_timestamp''")
                .build()
        );
        DdlGeneratorModelRequest request = DdlGeneratorModelRequest.builder()
            .after(Table.builder().name("t_double_1").columns(columns).build())
            .config(TableConfig.builder().dialectMeta(DialectMeta.DEFAULT_HOLO).build())
            .build();
        DdlGeneratorResult generate = codeGenerator.generate(request);
        String result =
            generate.getDialectNodes().stream().filter(DialectNode::isExecutable).map(DialectNode::getNode).collect(Collectors.joining("\n"));
        assertEquals("BEGIN;\n"
            + "CREATE TABLE IF NOT EXISTS t_double_1 (\n"
            + "   c1 TEXT NOT NULL DEFAULT '''current_timestamp'''\n"
            + ");\n\n"
            + "COMMIT;", result);
    }
}
