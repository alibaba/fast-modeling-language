/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.clickhouse;

import java.util.List;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
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
 * ClickHouseTransformerTest
 *
 * @author panguanjing
 * @date 2022/8/10
 */
public class ClickHouseTransformerTest {

    private static final String CREATE_TABLE = "CREATE TABLE db.demo\n"
        + "(\n"
        + "    id Int64,\n"
        + "    name Nullable(String) COMMENT '名称',\n"
        + "    amount Decimal(10, 2) DEFAULT 0.00,\n"
        + "    tags Array(String),\n"
        + "    create_time DateTime\n"
        + ")\n"
        + "ENGINE = MergeTree\n"
        + "PARTITION BY toYYYYMM(create_time)\n"
        + "PRIMARY KEY id\n"
        + "ORDER BY (id, create_time)\n"
        + "TTL create_time + INTERVAL 30 DAY\n"
        + "SETTINGS index_granularity = 8192\n"
        + "COMMENT '示例表';";

    ClickHouseTransformer clickHouseTransformer = new ClickHouseTransformer();

    @Test
    public void reverse() {
        DialectNode node = new DialectNode("create table a (a UInt64 )");
        BaseStatement reverse = clickHouseTransformer.reverse(node);
        CreateTable createTable = (CreateTable)reverse;
        List<ColumnDefinition> columnDefines = createTable.getColumnDefines();
        assertEquals(1, columnDefines.size());
        BaseDataType dataType = columnDefines.get(0).getDataType();
        assertEquals(dataType.toString(), "UInt64");
    }

    @Test
    public void reverseEngineClause() {
        BaseStatement reverse = clickHouseTransformer.reverse(new DialectNode(CREATE_TABLE));
        CreateTable createTable = (CreateTable)reverse;
        assertEquals(5, createTable.getColumnDefines().size());
        assertEquals("MergeTree", getProperty(createTable.getProperties(), ClickHousePropertyKey.TABLE_ENGINE));
        assertEquals("toYYYYMM(create_time)", getProperty(createTable.getProperties(), ClickHousePropertyKey.TABLE_PARTITION_BY));
        assertEquals("(id, create_time)", getProperty(createTable.getProperties(), ClickHousePropertyKey.TABLE_ORDER_BY));
        assertEquals("create_time + INTERVAL 30 DAY", getProperty(createTable.getProperties(), ClickHousePropertyKey.TABLE_TTL));
        assertEquals("示例表", createTable.getCommentValue());
        assertTrue(createTable.getColumnDefines().get(0).getPrimary());
        assertFalse(Boolean.TRUE.equals(createTable.getColumnDefines().get(1).getPrimary()));
        //Nullable 的语义落在 notNull 上,类型还原为原始类型
        ColumnDefinition name = createTable.getColumnDefines().get(1);
        assertEquals("String", name.getDataType().getTypeName().getValue());
        assertFalse(name.getNotNull());
        assertTrue(createTable.getColumnDefines().get(0).getNotNull());
    }

    @Test
    public void transform() {
        Node node = clickHouseTransformer.reverse(new DialectNode(CREATE_TABLE));
        DialectNode transform = clickHouseTransformer.transform((BaseStatement)node, TransformContext.builder().build());
        String expected = "CREATE TABLE db.demo\n"
            + "(\n"
            + "   id          Int64,\n"
            + "   name        Nullable(String) COMMENT '名称',\n"
            + "   amount      Decimal(10,2) DEFAULT 0,\n"
            + "   tags        Array(String),\n"
            + "   create_time DateTime\n"
            + ")\n"
            + "ENGINE = MergeTree\n"
            + "PARTITION BY toYYYYMM(create_time)\n"
            + "PRIMARY KEY id\n"
            + "ORDER BY (id, create_time)\n"
            + "TTL create_time + INTERVAL 30 DAY\n"
            + "SETTINGS index_granularity = 8192\n"
            + "COMMENT '示例表';";
        assertEquals(expected, transform.getNode());
    }

    @Test
    public void transformTable() {
        Node node = clickHouseTransformer.reverse(new DialectNode(CREATE_TABLE));
        Table table = clickHouseTransformer.transformTable(node, TransformContext.builder().build());
        assertEquals("db", table.getDatabase());
        assertEquals("demo", table.getName());
        assertEquals("示例表", table.getComment());
        assertEquals(5, table.getColumns().size());
        assertEquals("Int64", table.getColumns().get(0).getDataType());
        assertFalse(table.getColumns().get(0).isNullable());
        assertEquals("String", table.getColumns().get(1).getDataType());
        assertTrue(table.getColumns().get(1).isNullable());
        assertEquals(Integer.valueOf(10), table.getColumns().get(2).getPrecision());
        assertEquals(Integer.valueOf(2), table.getColumns().get(2).getScale());
        assertEquals("Array(String)", table.getColumns().get(3).getDataType());
        assertEquals("MergeTree", getClientProperty(table.getProperties(), ClickHousePropertyKey.TABLE_ENGINE));
        assertEquals("(id, create_time)", getClientProperty(table.getProperties(), ClickHousePropertyKey.TABLE_ORDER_BY));
    }

    @Test
    public void reverseTable() {
        Table table = buildTable();
        Node node = clickHouseTransformer.reverseTable(table);
        DialectNode transform = clickHouseTransformer.transform((BaseStatement)node);
        String expected = "CREATE TABLE db.di_demo\n"
            + "(\n"
            + "   id          Int64,\n"
            + "   name        Nullable(String) COMMENT '名称',\n"
            + "   amount      Nullable(Decimal(10,2)) DEFAULT 0,\n"
            + "   create_time DateTime\n"
            + ")\n"
            + "ENGINE = MergeTree\n"
            + "PARTITION BY toYYYYMM(create_time)\n"
            + "PRIMARY KEY id\n"
            + "ORDER BY (id);";
        assertEquals(expected, transform.getNode());
    }

    @Test
    public void roundTrip() {
        Node node = clickHouseTransformer.reverse(new DialectNode(CREATE_TABLE));
        Table table = clickHouseTransformer.transformTable(node, TransformContext.builder().build());
        Node reverseNode = clickHouseTransformer.reverseTable(table);
        DialectNode transform = clickHouseTransformer.transform((BaseStatement)reverseNode, TransformContext.builder().build());
        assertEquals(clickHouseTransformer.transform((BaseStatement)node, TransformContext.builder().build()).getNode(),
            transform.getNode());
    }

    @Test
    public void replaceKeepsSemantics() {
        String ddl = "REPLACE TABLE db.t (id UInt64 DEFAULT 1) ENGINE = MergeTree ORDER BY id";
        assertEquals("CREATE OR REPLACE TABLE db.t\n"
            + "(\n"
            + "   id UInt64 DEFAULT 1\n"
            + ")\n"
            + "ENGINE = MergeTree\n"
            + "ORDER BY id;", clickHouseTransformer.transform(
            (BaseStatement)clickHouseTransformer.reverse(new DialectNode(ddl)), TransformContext.builder().build()).getNode());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void rejectExpressionDefaultInsteadOfDroppingIt() {
        clickHouseTransformer.reverse(new DialectNode(
            "CREATE TABLE db.t (id UInt64 DEFAULT toUInt64(1)) ENGINE = MergeTree ORDER BY id"));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void rejectMaterializedColumnInsteadOfDroppingIt() {
        clickHouseTransformer.reverse(new DialectNode(
            "CREATE TABLE db.t (id UInt64, v UInt64 MATERIALIZED id) ENGINE = MergeTree ORDER BY id"));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void rejectTableIndexInsteadOfDroppingIt() {
        clickHouseTransformer.reverse(new DialectNode(
            "CREATE TABLE db.t (id UInt64, INDEX idx id TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY id"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectNullableContainerDto() {
        List<Column> columns = Lists.newArrayList(Column.builder().name("tags").dataType("Array(String)").nullable(true).build());
        List<BaseClientProperty> properties = Lists.newArrayList(
            clickHouseTransformer.create(ClickHousePropertyKey.TABLE_ENGINE.getValue(), "MergeTree"),
            clickHouseTransformer.create(ClickHousePropertyKey.TABLE_ORDER_BY.getValue(), "tags"));
        Table table = Table.builder().database("db").name("t").columns(columns).properties(properties).build();
        clickHouseTransformer.transform((BaseStatement)clickHouseTransformer.reverseTable(table));
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectDecimalWithoutScale() {
        List<Column> columns = Lists.newArrayList(Column.builder().name("amount").dataType("DECIMAL").precision(10).build());
        Table table = Table.builder().database("db").name("t").columns(columns).build();
        clickHouseTransformer.reverseTable(table);
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectUnsafeDtoProperty() {
        clickHouseTransformer.create(ClickHousePropertyKey.TABLE_ENGINE.getValue(), "MergeTree; DROP TABLE t");
    }

    private String getClientProperty(List<BaseClientProperty> properties, ClickHousePropertyKey propertyKey) {
        return properties.stream().filter(p -> propertyKey.getValue().equals(p.getKey()))
            .findFirst().map(p -> String.valueOf(p.getValue())).orElse(null);
    }

    private String getProperty(List<Property> properties, ClickHousePropertyKey propertyKey) {
        return properties.stream().filter(p -> propertyKey.getValue().equals(p.getName()))
            .findFirst().map(Property::getValue).orElse(null);
    }

    private Table buildTable() {
        List<Column> columns = Lists.newArrayList();
        columns.add(Column.builder().name("id").dataType("BIGINT").nullable(false).primaryKey(true).build());
        columns.add(Column.builder().name("name").dataType("VARCHAR").length(255).nullable(true).comment("名称").build());
        columns.add(Column.builder().name("amount").dataType("DECIMAL").precision(10).scale(2).nullable(true).defaultValue("0").build());
        columns.add(Column.builder().name("create_time").dataType("DATETIME").nullable(false).build());
        List<BaseClientProperty> properties = Lists.newArrayList();
        properties.add(clickHouseTransformer.create(ClickHousePropertyKey.TABLE_ENGINE.getValue(), "MergeTree"));
        properties.add(clickHouseTransformer.create(ClickHousePropertyKey.TABLE_PARTITION_BY.getValue(), "toYYYYMM(create_time)"));
        properties.add(clickHouseTransformer.create(ClickHousePropertyKey.TABLE_ORDER_BY.getValue(), "(id)"));
        return Table.builder()
            .ifNotExist(false)
            .database("db")
            .name("di_demo")
            .columns(columns)
            .properties(properties)
            .build();
    }
}
