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
import com.aliyun.fastmodel.transform.api.client.dto.property.StringProperty;
import com.aliyun.fastmodel.transform.api.client.dto.table.Column;
import com.aliyun.fastmodel.transform.api.client.dto.table.Table;
import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.clickhouse.ClickHouseTransformer;
import com.aliyun.fastmodel.transform.clickhouse.format.ClickHousePropertyKey;
import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * ClickHouse 的 Table DTO 与模型互转测试
 *
 * @author panguanjing
 */
public class ClickHouseClientConverterTest {

    private final ClickHouseTransformer transformer = new ClickHouseTransformer();

    private final TransformContext context = TransformContext.builder().build();

    private String toDdl(Table table) {
        Node node = transformer.reverseTable(table);
        return transformer.transform((BaseStatement)node, context).getNode();
    }

    private Table.TableBuilder tableBuilder(String name) {
        return Table.builder().ifNotExist(false).database("db").name(name);
    }

    private BaseClientProperty engine() {
        return transformer.create(ClickHousePropertyKey.TABLE_ENGINE.getValue(), "MergeTree");
    }

    private BaseClientProperty orderBy(String value) {
        return transformer.create(ClickHousePropertyKey.TABLE_ORDER_BY.getValue(), value);
    }

    @Test
    public void testDataTypeAliasMapping() {
        List<Column> columns = Lists.newArrayList();
        columns.add(Column.builder().name("c1").dataType("TINYINT").nullable(false).build());
        columns.add(Column.builder().name("c2").dataType("SMALLINT").nullable(false).build());
        columns.add(Column.builder().name("c3").dataType("INT").nullable(false).build());
        columns.add(Column.builder().name("c4").dataType("BIGINT").nullable(false).build());
        columns.add(Column.builder().name("c5").dataType("FLOAT").nullable(false).build());
        columns.add(Column.builder().name("c6").dataType("DOUBLE").nullable(false).build());
        columns.add(Column.builder().name("c7").dataType("DECIMAL").precision(10).scale(2).nullable(false).build());
        columns.add(Column.builder().name("c8").dataType("BOOLEAN").nullable(false).build());
        columns.add(Column.builder().name("c9").dataType("VARCHAR").length(32).nullable(false).build());
        columns.add(Column.builder().name("c10").dataType("DATETIME").nullable(false).build());
        columns.add(Column.builder().name("c11").dataType("DATE").nullable(false).build());
        columns.add(Column.builder().name("c12").dataType("FixedString").length(8).nullable(false).build());
        columns.add(Column.builder().name("c13").dataType("DateTime64").precision(3).nullable(false).build());
        columns.add(Column.builder().name("c14").dataType("Array(String)").nullable(false).build());
        columns.add(Column.builder().name("c15").dataType("UUID").nullable(false).build());
        Table table = tableBuilder("alias_table")
            .columns(columns)
            .properties(Lists.newArrayList(engine(), orderBy("c1")))
            .build();
        assertEquals("CREATE TABLE db.alias_table\n"
            + "(\n"
            + "   c1  Int8,\n"
            + "   c2  Int16,\n"
            + "   c3  Int32,\n"
            + "   c4  Int64,\n"
            + "   c5  Float32,\n"
            + "   c6  Float64,\n"
            + "   c7  Decimal(10,2),\n"
            + "   c8  Bool,\n"
            + "   c9  String,\n"
            + "   c10 DateTime,\n"
            + "   c11 Date,\n"
            + "   c12 FixedString(8),\n"
            + "   c13 DateTime64(3),\n"
            + "   c14 Array(String),\n"
            + "   c15 UUID\n"
            + ")\n"
            + "ENGINE = MergeTree\n"
            + "ORDER BY c1;", toDdl(table));
    }

    @Test
    public void testNullableColumnWrapping() {
        List<Column> columns = Lists.newArrayList();
        columns.add(Column.builder().name("id").dataType("BIGINT").nullable(false).build());
        columns.add(Column.builder().name("name").dataType("String").nullable(true).build());
        columns.add(Column.builder().name("already").dataType("Nullable(Int32)").nullable(true).build());
        Table table = tableBuilder("nullable_table")
            .columns(columns)
            .properties(Lists.newArrayList(engine(), orderBy("id")))
            .build();
        String ddl = toDdl(table);
        assertTrue(ddl.contains("name    Nullable(String)"));
        //已带 Nullable 包装的类型不重复包装
        assertTrue(ddl.contains("already Nullable(Int32)"));
    }

    @Test
    public void testColumnDefaultValues() {
        List<Column> columns = Lists.newArrayList();
        columns.add(Column.builder().name("id").dataType("BIGINT").nullable(false).build());
        columns.add(Column.builder().name("num").dataType("INT").nullable(false).defaultValue("0").build());
        columns.add(Column.builder().name("str").dataType("String").nullable(false).defaultValue("abc").build());
        columns.add(Column.builder().name("flag").dataType("BOOLEAN").nullable(false).defaultValue("true").build());
        columns.add(Column.builder().name("empty").dataType("String").nullable(true).defaultValue("NULL").build());
        columns.add(Column.builder().name("amount").dataType("DECIMAL").precision(10).scale(4).nullable(false)
            .defaultValue("0.5").build());
        Table table = tableBuilder("default_table")
            .columns(columns)
            .properties(Lists.newArrayList(engine(), orderBy("id")))
            .build();
        assertEquals("CREATE TABLE db.default_table\n"
            + "(\n"
            + "   id     Int64,\n"
            + "   num    Int32 DEFAULT 0,\n"
            + "   str    String DEFAULT 'abc',\n"
            + "   flag   Bool DEFAULT true,\n"
            + "   empty  Nullable(String) DEFAULT NULL,\n"
            + "   amount Decimal(10,4) DEFAULT 0.5\n"
            + ")\n"
            + "ENGINE = MergeTree\n"
            + "ORDER BY id;", toDdl(table));
    }

    @Test
    public void testPartitionKeyColumns() {
        List<Column> columns = Lists.newArrayList();
        columns.add(Column.builder().name("id").dataType("BIGINT").nullable(false).build());
        columns.add(Column.builder().name("dt").dataType("String").nullable(false)
            .partitionKey(true).partitionKeyIndex(0).build());
        Table table = tableBuilder("partition_table")
            .columns(columns)
            .properties(Lists.newArrayList(engine(), orderBy("id")))
            .build();
        String ddl = toDdl(table);
        assertTrue(ddl.contains("PARTITION BY (dt)"));
        assertTrue(ddl.endsWith("ORDER BY id;"));
    }

    @Test
    public void testMultiColumnPrimaryKeyFromColumnFlags() {
        List<Column> columns = Lists.newArrayList();
        columns.add(Column.builder().name("a").dataType("BIGINT").nullable(false).primaryKey(true).build());
        columns.add(Column.builder().name("b").dataType("BIGINT").nullable(false).primaryKey(true).build());
        Table table = tableBuilder("pk_table")
            .columns(columns)
            .properties(Lists.newArrayList(engine(), orderBy("(a, b)")))
            .build();
        String ddl = toDdl(table);
        assertTrue(ddl.contains("PRIMARY KEY (a,b)"));
        assertTrue(ddl.endsWith("ORDER BY (a, b);"));
    }

    @Test
    public void testPrimaryKeyPropertyWinsOverColumnFlags() {
        List<Column> columns = Lists.newArrayList();
        columns.add(Column.builder().name("a").dataType("BIGINT").nullable(false).primaryKey(true).build());
        List<BaseClientProperty> properties = Lists.newArrayList(engine(), orderBy("a"),
            transformer.create(ClickHousePropertyKey.TABLE_PRIMARY_KEY.getValue(), "a"));
        Table table = tableBuilder("pk_explicit").columns(columns).properties(properties).build();
        assertEquals(1, countOccurrences(toDdl(table), "PRIMARY KEY"));
        assertTrue(toDdl(table).contains("PRIMARY KEY a"));
    }

    @Test
    public void testClusterProperty() {
        List<Column> columns = Lists.newArrayList();
        columns.add(Column.builder().name("id").dataType("BIGINT").nullable(false).build());
        Table table = tableBuilder("cluster_table")
            .columns(columns)
            .properties(Lists.newArrayList(engine(), orderBy("id"),
                transformer.create(ClickHousePropertyKey.TABLE_CLUSTER.getValue(), "{cluster}")))
            .build();
        assertEquals("CREATE TABLE db.cluster_table ON CLUSTER {cluster}\n"
            + "(\n"
            + "   id Int64\n"
            + ")\n"
            + "ENGINE = MergeTree\n"
            + "ORDER BY id;", toDdl(table));
    }

    @Test
    public void testCommentEscaping() {
        List<Column> columns = Lists.newArrayList();
        columns.add(Column.builder().name("id").dataType("BIGINT").nullable(false).build());
        columns.add(Column.builder().name("c").dataType("String").nullable(false).comment("it's ok").build());
        Table table = tableBuilder("comment_table")
            .columns(columns)
            .comment("表'注释")
            .properties(Lists.newArrayList(engine(), orderBy("id")))
            .build();
        String ddl = toDdl(table);
        assertTrue(ddl.contains("COMMENT 'it\\'s ok'"));
        assertTrue(ddl.endsWith("COMMENT '表\\'注释';"));
    }

    @Test
    public void testTransformTableKeepsProperties() {
        String ddl = "CREATE TABLE db.src\n"
            + "(\n"
            + "   id Int64,\n"
            + "   st Enum8('a' = 1,'b' = 2),\n"
            + "   name Nullable(String)\n"
            + ")\n"
            + "ENGINE = MergeTree\n"
            + "PRIMARY KEY id\n"
            + "ORDER BY (id)\n"
            + "COMMENT '源表';";
        BaseStatement statement = transformer.reverse(new DialectNode(ddl));
        Table table = transformer.transformTable((Node)statement, context);
        assertEquals("db", table.getDatabase());
        assertEquals("src", table.getName());
        assertEquals("源表", table.getComment());
        assertTrue(table.isIfNotExist() == false);
        assertEquals("Enum8('a' = 1,'b' = 2)", table.getColumns().get(1).getDataType());
        List<BaseClientProperty> properties = table.getProperties();
        assertEquals(3, properties.size());
        assertEquals("MergeTree", getProperty(properties, ClickHousePropertyKey.TABLE_ENGINE));
        assertEquals("id", getProperty(properties, ClickHousePropertyKey.TABLE_PRIMARY_KEY));
        assertEquals("(id)", getProperty(properties, ClickHousePropertyKey.TABLE_ORDER_BY));
        assertNull(getProperty(properties, ClickHousePropertyKey.TABLE_CLUSTER));
        // 反向得到的 DTO 再生成 DDL 应与首次生成一致
        String regenerated = toDdl(table);
        assertEquals(regenerated, transformer.transform(
            transformer.reverse(new DialectNode(regenerated)), context).getNode());
    }

    @Test
    public void testTransformTableWithContextDatabase() {
        String ddl = "CREATE TABLE src2 (id Int64) ENGINE = MergeTree ORDER BY id";
        Table table = transformer.transformTable(transformer.reverse(new DialectNode(ddl)),
            TransformContext.builder().database("ctx_db").build());
        assertEquals("ctx_db", table.getDatabase());
        assertEquals("src2", table.getName());
    }

    @Test
    public void testCreateProperty() {
        BaseClientProperty property = transformer.create(ClickHousePropertyKey.TABLE_SETTINGS.getValue(), "index_granularity = 8192");
        assertTrue(property instanceof StringProperty);
        assertEquals("settings", property.getKey());
        assertEquals("index_granularity = 8192", property.getValue());
        //未登记的自定义属性也要保留,便于透传
        assertFalse(transformer.create("custom_key", "v").getKey().isEmpty());
    }

    private String getProperty(List<BaseClientProperty> properties, ClickHousePropertyKey propertyKey) {
        return properties.stream().filter(p -> propertyKey.getValue().equals(p.getKey()))
            .findFirst().map(p -> String.valueOf(p.getValue())).orElse(null);
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
