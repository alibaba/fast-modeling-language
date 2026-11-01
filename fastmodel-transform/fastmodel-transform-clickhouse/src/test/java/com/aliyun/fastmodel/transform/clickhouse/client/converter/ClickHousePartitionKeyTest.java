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

package com.aliyun.fastmodel.transform.clickhouse.client.converter;

import java.util.List;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.api.client.dto.table.Column;
import com.aliyun.fastmodel.transform.api.client.dto.table.Table;
import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.clickhouse.ClickHouseTransformer;
import com.aliyun.fastmodel.transform.clickhouse.format.ClickHousePropertyKey;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * PARTITION BY 反向回填分区列的测试
 * <p>
 * 设计口径：仅当 PARTITION BY 是裸列名列表时回填 partitionKey=true 与索引；
 * 函数/运算表达式分区只保留 partition_by 属性，不标记列。
 *
 * @author panguanjing
 */
public class ClickHousePartitionKeyTest {

    private static final String ORDERS_DDL = "CREATE TABLE db.orders (\n"
        + "  id Int64, ds String, region String, version UInt64,\n"
        + "  is_deleted UInt8, sign Int8, amount Decimal(18,2)\n"
        + ") ENGINE = MergeTree\n"
        + "PARTITION BY (region, ds)\n"
        + "ORDER BY id;";

    private final ClickHouseTransformer transformer = new ClickHouseTransformer();

    private final TransformContext context = TransformContext.builder().build();

    private Table reverseToTable(String ddl) {
        Node node = transformer.reverse(new DialectNode(ddl));
        return transformer.transformTable(node, context);
    }

    private Column getColumn(Table table, String name) {
        return table.getColumns().stream().filter(c -> name.equals(c.getName())).findFirst().orElse(null);
    }

    private String getProperty(List<BaseClientProperty> properties, ClickHousePropertyKey propertyKey) {
        return properties.stream().filter(p -> propertyKey.getValue().equals(p.getKey()))
            .findFirst().map(p -> String.valueOf(p.getValue())).orElse(null);
    }

    @Test
    public void testBackfillColumnListPartition() {
        Table table = reverseToTable(ORDERS_DDL);
        assertEquals("(region, ds)", getProperty(table.getProperties(), ClickHousePropertyKey.TABLE_PARTITION_BY));

        Column region = getColumn(table, "region");
        Column ds = getColumn(table, "ds");
        assertTrue(region.isPartitionKey());
        assertEquals(Integer.valueOf(0), region.getPartitionKeyIndex());
        assertTrue(ds.isPartitionKey());
        assertEquals(Integer.valueOf(1), ds.getPartitionKeyIndex());

        //非分区列不应被标记
        for (String name : new String[] {"id", "version", "is_deleted", "sign", "amount"}) {
            Column column = getColumn(table, name);
            assertFalse(name + " 不应是分区列", column.isPartitionKey());
            assertNull(name + " 不应有分区索引", column.getPartitionKeyIndex());
        }
    }

    @Test
    public void testBackfillSingleColumnPartition() {
        Table table = reverseToTable("CREATE TABLE db.t (id Int64, ds String) ENGINE = MergeTree "
            + "PARTITION BY ds ORDER BY id");
        assertEquals("ds", getProperty(table.getProperties(), ClickHousePropertyKey.TABLE_PARTITION_BY));
        Column ds = getColumn(table, "ds");
        assertTrue(ds.isPartitionKey());
        assertEquals(Integer.valueOf(0), ds.getPartitionKeyIndex());
        assertFalse(getColumn(table, "id").isPartitionKey());
    }

    @Test
    public void testBackfillQuotedColumnPartition() {
        Table table = reverseToTable("CREATE TABLE db.t (id Int64, `ds` String) ENGINE = MergeTree "
            + "PARTITION BY (`ds`) ORDER BY id");
        Column ds = getColumn(table, "ds");
        assertTrue(ds.isPartitionKey());
        assertEquals(Integer.valueOf(0), ds.getPartitionKeyIndex());
    }

    @Test
    public void testExpressionPartitionKeepsPropertyOnly() {
        Table table = reverseToTable("CREATE TABLE db.t (id Int64, ts DateTime) ENGINE = MergeTree "
            + "PARTITION BY toYYYYMM(ts) ORDER BY id");
        assertEquals("toYYYYMM(ts)", getProperty(table.getProperties(), ClickHousePropertyKey.TABLE_PARTITION_BY));
        assertFalse(getColumn(table, "ts").isPartitionKey());
        assertNull(getColumn(table, "ts").getPartitionKeyIndex());
    }

    @Test
    public void testMixedTuplePartitionKeepsPropertyOnly() {
        Table table = reverseToTable("CREATE TABLE db.t (id Int64, ds String, ts DateTime) ENGINE = MergeTree "
            + "PARTITION BY (ds, toYYYYMM(ts)) ORDER BY id");
        assertEquals("(ds, toYYYYMM(ts))", getProperty(table.getProperties(), ClickHousePropertyKey.TABLE_PARTITION_BY));
        assertFalse(getColumn(table, "ds").isPartitionKey());
        assertFalse(getColumn(table, "ts").isPartitionKey());
    }

    @Test
    public void testModuloPartitionKeepsPropertyOnly() {
        Table table = reverseToTable("CREATE TABLE db.t (id Int64) ENGINE = MergeTree "
            + "PARTITION BY id % 5 ORDER BY id");
        assertEquals("id % 5", getProperty(table.getProperties(), ClickHousePropertyKey.TABLE_PARTITION_BY));
        assertFalse(getColumn(table, "id").isPartitionKey());
    }

    @Test
    public void testUnknownPartitionColumnIgnored() {
        Table table = reverseToTable("CREATE TABLE db.t (id Int64) ENGINE = MergeTree "
            + "PARTITION BY missing_col ORDER BY id");
        assertEquals("missing_col", getProperty(table.getProperties(), ClickHousePropertyKey.TABLE_PARTITION_BY));
        assertFalse(getColumn(table, "id").isPartitionKey());
    }

    @Test
    public void testNoPartitionClause() {
        Table table = reverseToTable("CREATE TABLE db.t (id Int64) ENGINE = MergeTree ORDER BY id");
        assertNull(getProperty(table.getProperties(), ClickHousePropertyKey.TABLE_PARTITION_BY));
        assertFalse(getColumn(table, "id").isPartitionKey());
    }

    @Test
    public void testRoundTripKeepsPartitionColumns() {
        Table table = reverseToTable(ORDERS_DDL);
        Node node = transformer.reverseTable(table);
        String generated = transformer.transform((BaseStatement)node, context).getNode();
        assertTrue(generated.contains("PARTITION BY (region, ds)"));
        //回填后再次往返，分区列标记保持稳定
        Table second = reverseToTable(generated);
        assertTrue(getColumn(second, "region").isPartitionKey());
        assertTrue(getColumn(second, "ds").isPartitionKey());
        assertEquals(Integer.valueOf(0), getColumn(second, "region").getPartitionKeyIndex());
        assertEquals(Integer.valueOf(1), getColumn(second, "ds").getPartitionKeyIndex());
    }
}
