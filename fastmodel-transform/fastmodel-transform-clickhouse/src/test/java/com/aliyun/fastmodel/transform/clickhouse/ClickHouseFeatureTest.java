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

import java.util.Collections;
import java.util.List;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.transform.api.client.dto.table.Column;
import com.aliyun.fastmodel.transform.api.client.dto.table.Table;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.clickhouse.context.ClickHouseContext;
import com.aliyun.fastmodel.transform.clickhouse.format.ClickHouseExpressionVisitor;
import com.aliyun.fastmodel.transform.clickhouse.format.ClickHousePropertyKey;
import com.aliyun.fastmodel.transform.clickhouse.parser.ClickHouseLanguageParser;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * ClickHouse 建表语句特性的解析与再生成测试
 *
 * @author panguanjing
 */
public class ClickHouseFeatureTest {

    private final ClickHouseTransformer transformer = new ClickHouseTransformer();

    private final ClickHouseLanguageParser clickHouseLanguageParser = new ClickHouseLanguageParser();

    private final TransformContext context = TransformContext.builder().build();

    private String regenerate(String ddl) {
        BaseStatement statement = transformer.reverse(new DialectNode(ddl));
        return transformer.transform(statement, context).getNode();
    }

    private CreateTable parse(String ddl) {
        return (CreateTable)transformer.reverse(new DialectNode(ddl));
    }

    private String getProperty(List<Property> properties, ClickHousePropertyKey propertyKey) {
        return properties.stream().filter(p -> propertyKey.getValue().equals(p.getName()))
            .findFirst().map(Property::getValue).orElse(null);
    }

    @Test
    public void testMergeTreeWithEmptyArgs() {
        String ddl = "CREATE TABLE IF NOT EXISTS db.t1 (id UInt64, s String) ENGINE = MergeTree() ORDER BY id";
        assertEquals("CREATE TABLE IF NOT EXISTS db.t1\n"
            + "(\n"
            + "   id UInt64,\n"
            + "   s  String\n"
            + ")\n"
            + "ENGINE = MergeTree()\n"
            + "ORDER BY id;", regenerate(ddl));
        CreateTable createTable = parse(ddl);
        assertTrue(createTable.isNotExists());
        assertEquals("MergeTree()", getProperty(createTable.getProperties(), ClickHousePropertyKey.TABLE_ENGINE));
    }

    @Test
    public void testReplacingMergeTreeWithPartition() {
        String ddl = "CREATE TABLE db.t2 (id UInt64, ts DateTime) ENGINE = ReplacingMergeTree(ts) "
            + "PARTITION BY toYYYYMM(ts) ORDER BY (id, ts)";
        assertEquals("CREATE TABLE db.t2\n"
            + "(\n"
            + "   id UInt64,\n"
            + "   ts DateTime\n"
            + ")\n"
            + "ENGINE = ReplacingMergeTree(ts)\n"
            + "PARTITION BY toYYYYMM(ts)\n"
            + "ORDER BY (id, ts);", regenerate(ddl));
    }

    @Test
    public void testReplicatedMergeTreeKeepsEngineArgs() {
        String ddl = "CREATE TABLE db.t3 (id UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/t3', 'replica1') "
            + "ORDER BY id";
        String node = regenerate(ddl);
        assertTrue(node.contains("ENGINE = ReplicatedMergeTree('/clickhouse/tables/t3', 'replica1')"));
        assertEquals("CREATE TABLE db.t3\n"
            + "(\n"
            + "   id UInt64\n"
            + ")\n"
            + "ENGINE = ReplicatedMergeTree('/clickhouse/tables/t3', 'replica1')\n"
            + "ORDER BY id;", node);
    }

    @Test
    public void testDistributedEngine() {
        String ddl = "CREATE TABLE db.t4 (id UInt64, v String) ENGINE = Distributed('cluster1', 'db', 't1', rand())";
        assertEquals("CREATE TABLE db.t4\n"
            + "(\n"
            + "   id UInt64,\n"
            + "   v  String\n"
            + ")\n"
            + "ENGINE = Distributed('cluster1', 'db', 't1', rand());", regenerate(ddl));
    }

    @Test
    public void testEngineWithoutSortKey() {
        assertEquals("CREATE TABLE db.t5\n"
            + "(\n"
            + "   id UInt64\n"
            + ")\n"
            + "ENGINE = Memory;", regenerate("CREATE TABLE db.t5 (id UInt64) ENGINE = Memory"));
        assertEquals("CREATE TABLE db.t6\n"
            + "(\n"
            + "   id UInt64\n"
            + ")\n"
            + "ENGINE = TinyLog;", regenerate("CREATE TABLE db.t6 (id UInt64) ENGINE = TinyLog"));
    }

    @Test
    public void testComplexAndNestedDataTypes() {
        String ddl = "CREATE TABLE db.t7 (id UInt64, arr Array(Array(Int32)), m Map(String, Int64), "
            + "tp Tuple(String, Int32), lc LowCardinality(String)) ENGINE = Log";
        assertEquals("CREATE TABLE db.t7\n"
            + "(\n"
            + "   id  UInt64,\n"
            + "   arr Array(Array(Int32)),\n"
            + "   m   Map(String,Int64),\n"
            + "   tp  Tuple(String,Int32),\n"
            + "   lc  LowCardinality(String)\n"
            + ")\n"
            + "ENGINE = Log;", regenerate(ddl));
        Table table = transformer.transformTable(parse(ddl), context);
        assertEquals("Array(Array(Int32))", table.getColumns().get(1).getDataType());
        assertEquals("Map(String,Int64)", table.getColumns().get(2).getDataType());
        assertEquals("Tuple(String,Int32)", table.getColumns().get(3).getDataType());
        assertEquals("LowCardinality(String)", table.getColumns().get(4).getDataType());
    }

    @Test
    public void testEnumType() {
        String ddl = "CREATE TABLE db.t8 (id UInt64, st Enum8('a' = 1, 'b' = 2), st16 Enum16('x' = 100)) "
            + "ENGINE = MergeTree ORDER BY id";
        assertEquals("CREATE TABLE db.t8\n"
            + "(\n"
            + "   id   UInt64,\n"
            + "   st   Enum8('a' = 1,'b' = 2),\n"
            + "   st16 Enum16('x' = 100)\n"
            + ")\n"
            + "ENGINE = MergeTree\n"
            + "ORDER BY id;", regenerate(ddl));
    }

    @Test
    public void testFixedStringDecimalAndDateTime64() {
        String ddl = "CREATE TABLE db.t9 (id UInt64, f FixedString(16), d Decimal(18, 4), dt64 DateTime64(3), u UUID) "
            + "ENGINE = TinyLog";
        assertEquals("CREATE TABLE db.t9\n"
            + "(\n"
            + "   id   UInt64,\n"
            + "   f    FixedString(16),\n"
            + "   d    Decimal(18,4),\n"
            + "   dt64 DateTime64(3),\n"
            + "   u    UUID\n"
            + ")\n"
            + "ENGINE = TinyLog;", regenerate(ddl));
        Table table = transformer.transformTable(parse(ddl), context);
        assertEquals(Integer.valueOf(16), table.getColumns().get(1).getLength());
        assertEquals(Integer.valueOf(18), table.getColumns().get(2).getPrecision());
        assertEquals(Integer.valueOf(4), table.getColumns().get(2).getScale());
        assertEquals(Integer.valueOf(3), table.getColumns().get(3).getPrecision());
    }

    @Test
    public void testColumnDefaultAndComment() {
        String ddl = "CREATE TABLE db.t10 (id UInt64, v Int32 DEFAULT 0, s String DEFAULT 'x', "
            + "c Int8 COMMENT 'it''s ok') ENGINE = MergeTree ORDER BY id";
        assertEquals("CREATE TABLE db.t10\n"
            + "(\n"
            + "   id UInt64,\n"
            + "   v  Int32 DEFAULT 0,\n"
            + "   s  String DEFAULT 'x',\n"
            + "   c  Int8 COMMENT 'it\\'s ok'\n"
            + ")\n"
            + "ENGINE = MergeTree\n"
            + "ORDER BY id;", regenerate(ddl));
        Table table = transformer.transformTable(parse(ddl), context);
        assertEquals("0", table.getColumns().get(1).getDefaultValue());
        assertEquals("x", table.getColumns().get(2).getDefaultValue());
        assertEquals("it's ok", table.getColumns().get(3).getComment());
    }

    @Test
    public void testNullableColumn() {
        String ddl = "CREATE TABLE db.t11 (id UInt64, name Nullable(String)) ENGINE = MergeTree ORDER BY id";
        Table table = transformer.transformTable(parse(ddl), context);
        assertFalse(table.getColumns().get(0).isNullable());
        assertTrue(table.getColumns().get(1).isNullable());
        assertEquals("String", table.getColumns().get(1).getDataType());
        assertEquals("CREATE TABLE db.t11\n"
            + "(\n"
            + "   id   UInt64,\n"
            + "   name Nullable(String)\n"
            + ")\n"
            + "ENGINE = MergeTree\n"
            + "ORDER BY id;", regenerate(ddl));
    }

    @Test
    public void testFullEngineClause() {
        String ddl = "CREATE TABLE db.t12 (id UInt64, ts DateTime) ENGINE = MergeTree ORDER BY id "
            + "PARTITION BY toYYYYMM(ts) SAMPLE BY id TTL ts + INTERVAL 1 DAY DELETE SETTINGS index_granularity = 8192";
        CreateTable createTable = parse(ddl);
        List<Property> properties = createTable.getProperties();
        assertEquals("MergeTree", getProperty(properties, ClickHousePropertyKey.TABLE_ENGINE));
        assertEquals("toYYYYMM(ts)", getProperty(properties, ClickHousePropertyKey.TABLE_PARTITION_BY));
        assertEquals("id", getProperty(properties, ClickHousePropertyKey.TABLE_ORDER_BY));
        assertEquals("id", getProperty(properties, ClickHousePropertyKey.TABLE_SAMPLE_BY));
        assertEquals("ts + INTERVAL 1 DAY DELETE", getProperty(properties, ClickHousePropertyKey.TABLE_TTL));
        assertEquals("index_granularity = 8192", getProperty(properties, ClickHousePropertyKey.TABLE_SETTINGS));
        // 生成顺序固定为 ENGINE / PARTITION BY / PRIMARY KEY / ORDER BY / SAMPLE BY / TTL / SETTINGS
        assertEquals("CREATE TABLE db.t12\n"
            + "(\n"
            + "   id UInt64,\n"
            + "   ts DateTime\n"
            + ")\n"
            + "ENGINE = MergeTree\n"
            + "PARTITION BY toYYYYMM(ts)\n"
            + "ORDER BY id\n"
            + "SAMPLE BY id\n"
            + "TTL ts + INTERVAL 1 DAY DELETE\n"
            + "SETTINGS index_granularity = 8192;", regenerate(ddl));
    }

    @Test
    public void testMultiSettings() {
        String ddl = "CREATE TABLE db.t13 (id UInt64) ENGINE = MergeTree ORDER BY id "
            + "SETTINGS index_granularity = 4096, min_bytes_for_wide_part = 0";
        assertEquals("index_granularity = 4096, min_bytes_for_wide_part = 0",
            getProperty(parse(ddl).getProperties(), ClickHousePropertyKey.TABLE_SETTINGS));
        assertTrue(regenerate(ddl).endsWith("SETTINGS index_granularity = 4096, min_bytes_for_wide_part = 0;"));
    }

    @Test
    public void testTtlToDisk() {
        String ddl = "CREATE TABLE db.t14 (id UInt64, ts DateTime) ENGINE = MergeTree ORDER BY id "
            + "TTL ts + INTERVAL 7 DAY TO DISK 'cold'";
        assertEquals("ts + INTERVAL 7 DAY TO DISK 'cold'",
            getProperty(parse(ddl).getProperties(), ClickHousePropertyKey.TABLE_TTL));
        assertTrue(regenerate(ddl).endsWith("TTL ts + INTERVAL 7 DAY TO DISK 'cold';"));
    }

    @Test
    public void testTableComment() {
        String ddl = "CREATE TABLE db.t15 (id UInt64 COMMENT '主键') ENGINE = MergeTree ORDER BY tuple() COMMENT '表注释'";
        CreateTable createTable = parse(ddl);
        assertEquals("表注释", createTable.getCommentValue());
        assertEquals("主键", createTable.getColumnDefines().get(0).getComment().getComment());
        assertEquals("tuple()", getProperty(createTable.getProperties(), ClickHousePropertyKey.TABLE_ORDER_BY));
        assertTrue(regenerate(ddl).endsWith("COMMENT '表注释';"));
    }

    @Test
    public void testOnCluster() {
        String ddl = "CREATE TABLE db.t16 ON CLUSTER c1 (id UInt64) ENGINE = MergeTree ORDER BY id";
        assertEquals("c1", getProperty(parse(ddl).getProperties(), ClickHousePropertyKey.TABLE_CLUSTER));
        assertEquals("CREATE TABLE db.t16 ON CLUSTER c1\n"
            + "(\n"
            + "   id UInt64\n"
            + ")\n"
            + "ENGINE = MergeTree\n"
            + "ORDER BY id;", regenerate(ddl));
    }

    @Test
    public void testOnClusterWithStringLiteral() {
        String ddl = "CREATE TABLE IF NOT EXISTS db.t17 ON CLUSTER '{cluster}' (id UInt64) ENGINE = MergeTree ORDER BY id";
        assertEquals("'{cluster}'", getProperty(parse(ddl).getProperties(), ClickHousePropertyKey.TABLE_CLUSTER));
        assertEquals("CREATE TABLE IF NOT EXISTS db.t17 ON CLUSTER '{cluster}'\n"
            + "(\n"
            + "   id UInt64\n"
            + ")\n"
            + "ENGINE = MergeTree\n"
            + "ORDER BY id;", regenerate(ddl));
    }

    @Test
    public void testQuotedIdentifiers() {
        String ddl = "CREATE TABLE `db`.`T18` (`Id` UInt64, `Name` String) ENGINE = MergeTree ORDER BY `Id`";
        assertEquals("CREATE TABLE `db`.`T18`\n"
            + "(\n"
            + "   `Id`   UInt64,\n"
            + "   `Name` String\n"
            + ")\n"
            + "ENGINE = MergeTree\n"
            + "ORDER BY `Id`;", regenerate(ddl));
        Table table = transformer.transformTable(parse(ddl), context);
        assertEquals("db", table.getDatabase());
        assertEquals("T18", table.getName());
    }

    @Test
    public void testRoundTripIsIdempotent() {
        String ddl = "CREATE TABLE IF NOT EXISTS db.t19 ON CLUSTER c1\n"
            + "(\n"
            + "   id          UInt64,\n"
            + "   name        Nullable(String) COMMENT '名称',\n"
            + "   amount      Decimal(18,4) DEFAULT 0,\n"
            + "   tags        Array(String),\n"
            + "   st          Enum8('a' = 1,'b' = 2),\n"
            + "   create_time DateTime64(3)\n"
            + ")\n"
            + "ENGINE = ReplacingMergeTree(create_time)\n"
            + "PARTITION BY toYYYYMM(create_time)\n"
            + "PRIMARY KEY id\n"
            + "ORDER BY (id, create_time)\n"
            + "TTL create_time + INTERVAL 30 DAY DELETE\n"
            + "SETTINGS index_granularity = 8192\n"
            + "COMMENT '示例表';";
        String first = regenerate(ddl);
        Table table = transformer.transformTable(parse(ddl), context);
        assertEquals("示例表", table.getComment());
        assertEquals("Enum8('a' = 1,'b' = 2)", table.getColumns().get(4).getDataType());
        String second = regenerate(first);
        assertEquals(first, second);
    }

    @Test
    public void testReverseTableWithoutEngine() {
        Table table = Table.builder()
            .ifNotExist(false)
            .database("db")
            .name("t20")
            .columns(Collections.singletonList(Column.builder()
                .name("id").dataType("UInt64").nullable(false).build()))
            .build();
        DialectNode node = transformer.transform((BaseStatement)transformer.reverseTable(table), context);
        assertEquals("CREATE TABLE db.t20\n"
            + "(\n"
            + "   id UInt64\n"
            + ");", node.getNode());
    }

    @Test
    public void testParseDataType() {
        ClickHouseExpressionVisitor expressionVisitor = new ClickHouseExpressionVisitor(ClickHouseContext.builder().build());
        ReverseContext reverseContext = ReverseContext.builder().build();
        assertEquals("Map(String,Int64)",
            expressionVisitor.process(clickHouseLanguageParser.parseDataType("Map(String, Int64)", reverseContext)));
        assertEquals("Decimal(18,4)",
            expressionVisitor.process(clickHouseLanguageParser.parseDataType("Decimal(18, 4)", reverseContext)));
        assertEquals("Enum8('a' = 1,'b' = 2)",
            expressionVisitor.process(clickHouseLanguageParser.parseDataType("Enum8('a' = 1, 'b' = 2)", reverseContext)));
    }
}
