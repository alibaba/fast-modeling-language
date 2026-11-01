/*
 * Copyright 2021-2022 Alibaba Group Holding Ltd.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.aliyun.fastmodel.transform.clickhouse;

import java.util.List;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.api.client.dto.table.Column;
import com.aliyun.fastmodel.transform.api.client.dto.table.Table;
import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.clickhouse.format.ClickHousePropertyKey;
import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * ClickHouse 分区相关建表语句的回归用例
 * <p>
 * 样本来自 ClickHouse 官方文档与社区常见写法：按月/日/周分区、格式化分区、取模与哈希分区、
 * 裸列名分区、混合元组分区、空元组分区、可空时间列分区等。
 * 口径：裸列名列表回填列的分区标记；函数/运算表达式只保留 partition_by 属性，不标记列。
 *
 * @author panguanjing
 */
public class ClickHousePartitionSqlTest {

    private static final String COLUMNS = "id Int64, ts DateTime, dt String, region String";

    private final ClickHouseTransformer transformer = new ClickHouseTransformer();

    private final TransformContext context = TransformContext.builder().build();

    private String createDdl(String partitionBy) {
        return "CREATE TABLE db.t (" + COLUMNS + ") ENGINE = MergeTree PARTITION BY " + partitionBy + " ORDER BY id";
    }

    private Table reverseToTable(String ddl) {
        Node node = transformer.reverse(new DialectNode(ddl));
        return transformer.transformTable(node, context);
    }

    private String regenerate(String ddl) {
        BaseStatement statement = transformer.reverse(new DialectNode(ddl));
        return transformer.transform(statement, context).getNode();
    }

    private String getProperty(List<BaseClientProperty> properties, ClickHousePropertyKey propertyKey) {
        return properties.stream().filter(p -> propertyKey.getValue().equals(p.getKey()))
            .findFirst().map(p -> String.valueOf(p.getValue())).orElse(null);
    }

    /**
     * 表达式分区：属性原文保留，不标记任何列为分区列，生成结果幂等
     */
    private void assertExpressionPartition(String partitionBy) {
        Table table = reverseToTable(createDdl(partitionBy));
        assertEquals(partitionBy, getProperty(table.getProperties(), ClickHousePropertyKey.TABLE_PARTITION_BY));
        for (Column column : table.getColumns()) {
            assertFalse(partitionBy + " 不应回填列 " + column.getName(), column.isPartitionKey());
        }
        String generated = regenerate(createDdl(partitionBy));
        assertTrue(generated, generated.contains("PARTITION BY " + partitionBy));
        assertEquals(generated, regenerate(generated));
    }

    private void assertColumnListPartition(String partitionBy, String... expectedColumns) {
        Table table = reverseToTable(createDdl(partitionBy));
        assertEquals(partitionBy, getProperty(table.getProperties(), ClickHousePropertyKey.TABLE_PARTITION_BY));
        for (int i = 0; i < expectedColumns.length; i++) {
            final String expectedColumn = expectedColumns[i];
            Column column = table.getColumns().stream()
                .filter(c -> expectedColumn.equals(c.getName())).findFirst().orElseThrow(AssertionError::new);
            assertTrue(expectedColumn + " 应为分区列", column.isPartitionKey());
            assertEquals(Integer.valueOf(i), column.getPartitionKeyIndex());
        }
        String generated = regenerate(createDdl(partitionBy));
        assertTrue(generated, generated.contains("PARTITION BY " + partitionBy));
        assertEquals(generated, regenerate(generated));
    }

    @Test
    public void testToYYYYMMPartition() {
        assertExpressionPartition("toYYYYMM(ts)");
    }

    @Test
    public void testToYYYYMMDDPartition() {
        assertExpressionPartition("toYYYYMMDD(ts)");
    }

    @Test
    public void testToStartOfMonthPartition() {
        assertExpressionPartition("toStartOfMonth(ts)");
    }

    @Test
    public void testToMondayPartition() {
        assertExpressionPartition("toMonday(ts)");
    }

    @Test
    public void testToDatePartition() {
        assertExpressionPartition("toDate(ts)");
    }

    @Test
    public void testFormatDateTimePartition() {
        assertExpressionPartition("formatDateTime(ts, '%Y-%m')");
    }

    @Test
    public void testParenSingleExpressionPartition() {
        assertExpressionPartition("(toYYYYMM(ts))");
    }

    @Test
    public void testIntDivPartition() {
        assertExpressionPartition("intDiv(id, 1000000)");
    }

    @Test
    public void testHashModPartition() {
        assertExpressionPartition("cityHash64(id) % 16");
    }

    @Test
    public void testModuloPartition() {
        assertExpressionPartition("id % 5");
    }

    @Test
    public void testSubstringPartition() {
        assertExpressionPartition("substring(dt, 1, 7)");
    }

    @Test
    public void testTupleEmptyPartition() {
        assertExpressionPartition("tuple()");
    }

    @Test
    public void testNullableColumnPartition() {
        String ddl = "CREATE TABLE db.t (id Int64, ts Nullable(DateTime)) ENGINE = MergeTree "
            + "PARTITION BY toYYYYMM(ts) ORDER BY id";
        Table table = reverseToTable(ddl);
        assertEquals("toYYYYMM(ts)", getProperty(table.getProperties(), ClickHousePropertyKey.TABLE_PARTITION_BY));
        assertFalse(table.getColumns().get(1).isPartitionKey());
        assertTrue(regenerate(ddl).contains("PARTITION BY toYYYYMM(ts)"));
    }

    @Test
    public void testMixedTuplePartition() {
        assertExpressionPartition("(toYYYYMM(ts), region)");
    }

    @Test
    public void testBareColumnListPartition() {
        assertColumnListPartition("(region, dt)", "region", "dt");
    }

    @Test
    public void testBareColumnOrderFollowsExpression() {
        assertColumnListPartition("(dt, region)", "dt", "region");
    }

    @Test
    public void testSingleBareColumnPartition() {
        assertColumnListPartition("dt", "dt");
    }

    @Test
    public void testClusterEngineAndPartitionCombo() {
        String ddl = "CREATE TABLE db.t ON CLUSTER c1 (" + COLUMNS + ") ENGINE = ReplacingMergeTree "
            + "ORDER BY id PARTITION BY (dt, region)";
        Table table = reverseToTable(ddl);
        assertEquals("c1", getProperty(table.getProperties(), ClickHousePropertyKey.TABLE_CLUSTER));
        assertEquals("ReplacingMergeTree", getProperty(table.getProperties(), ClickHousePropertyKey.TABLE_ENGINE));
        assertEquals("(dt, region)", getProperty(table.getProperties(), ClickHousePropertyKey.TABLE_PARTITION_BY));
        assertEquals(Integer.valueOf(0), table.getColumns().stream().filter(c -> "dt".equals(c.getName()))
            .findFirst().get().getPartitionKeyIndex());
        assertEquals(Integer.valueOf(1), table.getColumns().stream().filter(c -> "region".equals(c.getName()))
            .findFirst().get().getPartitionKeyIndex());
        String generated = regenerate(ddl);
        assertTrue(generated.contains("ON CLUSTER c1"));
        assertTrue(generated.contains("PARTITION BY (dt, region)"));
        assertEquals(generated, regenerate(generated));
    }

    @Test
    public void testDtoExpressionPartitionGeneratesSingleClause() {
        List<Column> columns = Lists.newArrayList();
        columns.add(Column.builder().name("id").dataType("BIGINT").nullable(false).build());
        columns.add(Column.builder().name("ts").dataType("DATETIME").nullable(false).build());
        List<BaseClientProperty> properties = Lists.newArrayList();
        properties.add(transformer.create(ClickHousePropertyKey.TABLE_ENGINE.getValue(), "MergeTree"));
        properties.add(transformer.create(ClickHousePropertyKey.TABLE_ORDER_BY.getValue(), "id"));
        properties.add(transformer.create(ClickHousePropertyKey.TABLE_PARTITION_BY.getValue(), "toYYYYMM(ts)"));
        Table table = Table.builder()
            .ifNotExist(false)
            .database("db")
            .name("t")
            .columns(columns)
            .properties(properties)
            .build();
        String generated = transformer.transform((BaseStatement)transformer.reverseTable(table), context).getNode();
        assertTrue(generated.contains("PARTITION BY toYYYYMM(ts)"));
        assertEquals(1, countOccurrences(generated, "PARTITION BY"));
        //表达式分区不应把列升级成分区列
        Table back = reverseToTable(generated);
        for (Column column : back.getColumns()) {
            assertFalse(column.isPartitionKey());
        }
    }

    @Test
    public void testRealWorldPartitionedTableRoundTrip() {
        String ddl = "CREATE TABLE db.events\n"
            + "(\n"
            + "   id     UInt64,\n"
            + "   ts     DateTime,\n"
            + "   region String,\n"
            + "   amount Decimal(18,2)\n"
            + ")\n"
            + "ENGINE = MergeTree\n"
            + "PARTITION BY (toYYYYMM(ts), region)\n"
            + "PRIMARY KEY id\n"
            + "ORDER BY (id, ts)\n"
            + "TTL ts + INTERVAL 30 DAY\n"
            + "SETTINGS index_granularity = 8192;";
        Table table = reverseToTable(ddl);
        assertEquals("(toYYYYMM(ts), region)",
            getProperty(table.getProperties(), ClickHousePropertyKey.TABLE_PARTITION_BY));
        assertFalse(table.getColumns().stream().anyMatch(Column::isPartitionKey));
        assertEquals(ddl, regenerate(ddl));
    }

    private int countOccurrences(String text, String keyword) {
        int count = 0;
        int index = text.indexOf(keyword);
        while (index >= 0) {
            count++;
            index = text.indexOf(keyword, index + keyword.length());
        }
        return count;
    }
}
