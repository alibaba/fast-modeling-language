/*
 * Copyright (c)  2026. Aliyun.com All right reserved.
 */

package com.aliyun.fastmodel.transform.mc.client.converter;

import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.transform.api.client.dto.table.Table;
import com.aliyun.fastmodel.transform.api.client.dto.table.TableConfig;
import com.aliyun.fastmodel.transform.mc.parser.MaxComputeLanguageParser;
import com.aliyun.fastmodel.transform.mc.context.MaxComputeContext;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** Regression for VECTOR conversion under the Javassist test classloader. */
@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"jdk.internal.reflect.*", "javax.management.*"})
public class VectorClientConverterPowerMockTest {

    private static final String COMPLETE_CREATE_TABLE = "CREATE TABLE IF NOT EXISTS project.schema.table\n"
        + "(\n"
        + "   col1 STRING COMMENT '',\n"
        + "   col2 STRING COMMENT '',\n"
        + "   col3 MAP<STRING,STRUCT<x:DECIMAL(38,10),y:VARCHAR(10)>> COMMENT '',\n"
        + "   col4 ARRAY<VARCHAR(10)> COMMENT '',\n"
        + "   col5 STRUCT<x:VARCHAR(10),y:CHAR(10)> COMMENT '',\n"
        + "   col6 VECTOR(FLOAT,3) COMMENT ''\n"
        + ")\n"
        + "COMMENT 'created by DI'\n"
        + "PARTITIONED BY\n"
        + "(\n"
        + "   pt STRING COMMENT '',\n"
        + "   ds STRING COMMENT ''\n"
        + ")\n"
        + "LIFECYCLE 36500;";

    @Test
    public void convertCompleteCreateTableWithVectorUnderPowerMockClassLoader() {
        MaxComputeLanguageParser parser = new MaxComputeLanguageParser();
        MaxComputeClientConverter converter = new MaxComputeClientConverter();
        CreateTable statement = parser.parseNode(COMPLETE_CREATE_TABLE);

        Table table = converter.convertToTable(
            statement, MaxComputeContext.builder().build());

        assertEquals(8, table.getColumns().size());
        assertEquals("STRING", table.getColumns().get(0).getDataType());
        assertTrue(table.getColumns().get(2).getDataType().contains("MAP"));
        assertTrue(table.getColumns().get(3).getDataType().contains("ARRAY"));
        assertTrue(table.getColumns().get(4).getDataType().contains("STRUCT"));
        assertEquals("VECTOR(FLOAT,3)", table.getColumns().get(5).getDataType());
        assertTrue(table.getColumns().get(6).isPartitionKey());
        assertTrue(table.getColumns().get(7).isPartitionKey());
        assertEquals(0, table.getColumns().get(6).getPartitionKeyIndex().intValue());
        assertEquals(1, table.getColumns().get(7).getPartitionKeyIndex().intValue());

        CreateTable roundTrip = (CreateTable)converter.convertToNode(table, TableConfig.builder().build());
        assertEquals("VECTOR(FLOAT,3)", roundTrip.getColumnDefines().get(5).getDataType().toString());
    }

    @Test
    public void convertTopLevelParameterizedTypesUnderPowerMockClassLoader() {
        CreateTable statement = new MaxComputeLanguageParser().parseNode(
            "CREATE TABLE typed_table (decimal_col DECIMAL(38,10), varchar_col VARCHAR(20), char_col CHAR(10));");

        Table table = new MaxComputeClientConverter().convertToTable(
            statement, MaxComputeContext.builder().build());

        assertEquals(38, table.getColumns().get(0).getPrecision().intValue());
        assertEquals(10, table.getColumns().get(0).getScale().intValue());
        assertEquals(20, table.getColumns().get(1).getLength().intValue());
        assertEquals(10, table.getColumns().get(2).getLength().intValue());
    }

    @Test
    public void convertTwoArgumentTypeWithOnlyPrecisionUnderPowerMockClassLoader() {
        CreateTable statement = new MaxComputeLanguageParser().parseNode(
            "CREATE TABLE precision_table (decimal_col DECIMAL(10));");

        Table table = new MaxComputeClientConverter().convertToTable(
            statement, MaxComputeContext.builder().build());

        assertEquals(10, table.getColumns().get(0).getPrecision().intValue());
        assertNull(table.getColumns().get(0).getScale());
    }
}
