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
import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * code generator
 *
 * @author panguanjing
 * @date 2022/6/10
 */
public class HologresCodeGeneratorModelInvalidDataTypeTest {

    @Test
    public void testGeneratorNewTable() {
        CodeGenerator codeGenerator = new DefaultCodeGenerator();
        Table table = getTable("a", 1000L);
        table.setColumns(getColumns("c1", "c1"));
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
    public void testGeneratorNewTableArrayType() {
        testColumnDataType("text[]", 10, 10, 10, "BEGIN;\n"
            + "CREATE TABLE IF NOT EXISTS a (\n"
            + "   c1 TEXT[] NOT NULL\n"
            + ");\n"
            + "CALL SET_TABLE_PROPERTY('a', 'time_to_live_in_seconds', '1000');\n"
            + "COMMENT ON COLUMN a.c1 IS 'comment';\n"
            + "COMMIT;");
    }

    @Test
    public void testGeneratorNewTableDecimal() {
        testColumnDataType("decimal", 1, 10, 10, "BEGIN;\n"
            + "CREATE TABLE IF NOT EXISTS a (\n"
            + "   c1 DECIMAL(10,10) NOT NULL\n"
            + ");\n"
            + "CALL SET_TABLE_PROPERTY('a', 'time_to_live_in_seconds', '1000');\n"
            + "COMMENT ON COLUMN a.c1 IS 'comment';\n"
            + "COMMIT;");
    }

    @Test
    public void testGeneratorNewTableChar() {
        testColumnDataType("bpchar", null, 10, 20, "BEGIN;\n"
            + "CREATE TABLE IF NOT EXISTS a (\n"
            + "   c1 CHAR NOT NULL\n"
            + ");\n"
            + "CALL SET_TABLE_PROPERTY('a', 'time_to_live_in_seconds', '1000');\n"
            + "COMMENT ON COLUMN a.c1 IS 'comment';\n"
            + "COMMIT;");
        testColumnDataType("char", 1, 10, 10, "BEGIN;\n"
            + "CREATE TABLE IF NOT EXISTS a (\n"
            + "   c1 CHAR(1) NOT NULL\n"
            + ");\n"
            + "CALL SET_TABLE_PROPERTY('a', 'time_to_live_in_seconds', '1000');\n"
            + "COMMENT ON COLUMN a.c1 IS 'comment';\n"
            + "COMMIT;");
    }

    private void testColumnDataType(String dataType, Integer length, Integer precision, Integer scale, String expect) {
        CodeGenerator codeGenerator = new DefaultCodeGenerator();
        Table table = getTable("a", 1000L);
        List<Column> columns = getColumns("c1", "c1", dataType, length, precision, scale);
        table.setColumns(columns);
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
        assertEquals(dialectNodes.stream().map(DialectNode::getNode).collect(Collectors.joining(",\n")), expect);
    }

    private Table getTable(String name, long lifeCycleSeconds) {
        return Table.builder()
            .name(name)
            .lifecycleSeconds(lifeCycleSeconds)
            .build();
    }

    private List<Column> getColumns(String id, String name, String dataTypeName, Integer length, Integer precision, Integer scale) {
        return Lists.newArrayList(
            Column.builder()
                .id(id)
                .name(name)
                .dataType(dataTypeName)
                .length(length)
                .precision(precision)
                .scale(scale)
                .comment("comment")
                .build()
        );
    }

    private List<Column> getColumns(String id, String name) {
        return Lists.newArrayList(
            Column.builder()
                .id(id)
                .name(name)
                .dataType("text")
                .length(10)
                .precision(10)
                .scale(38)
                .comment("comment")
                .build()
        );
    }

}
