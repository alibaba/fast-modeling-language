package com.aliyun.fastmodel.transform.flink;

import java.util.stream.Collectors;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.PrimaryConstraint;
import com.aliyun.fastmodel.transform.api.client.dto.constraint.Constraint;
import com.aliyun.fastmodel.transform.api.client.dto.constraint.OutlineConstraintType;
import com.aliyun.fastmodel.transform.api.client.dto.table.Column;
import com.aliyun.fastmodel.transform.api.client.dto.table.Table;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.WaterMarkConstraint;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;

import static com.aliyun.fastmodel.transform.flink.parser.FlinkLanguageParser.FLINK_PARSE_MODE_KEY;
import static com.aliyun.fastmodel.transform.flink.parser.FlinkLanguageParser.FLINK_PARSE_MODE_SDK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author 子梁
 * @date 2024/5/15
 */
public class FlinkTransformerTest {

    FlinkTransformer flinkTransformer = new FlinkTransformer();

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testReverse() throws Exception {
        DialectNode dialectNode = new DialectNode("CREATE TABLE catalog.database.MyTable (\n" +
            "  `user_id` BIGINT COMMENT '23',\n" +
            "  `name` STRING NOT NULL,\n" +
            "  `age` VARCHAR(4) NOT NULL,\n" +
            "  `time` TIMESTAMP(4) WITH LOCAL TIME ZONE,\n" +
            "  `test` DECIMAL(20,4),\n" +
            "  `array` ARRAY<STRING>,\n" +
            "  `map` MAP<STRING,BIGINT>,\n" +
            "  `row` ROW<a STRING, b BIGINT>,\n" +
            "  `raw` RAW('a','b'),\n" +
            "  `timestamp` TIMESTAMP_LTZ(3) METADATA FROM `row`,  -- use column name as metadata key\n" +
            "  `cost` AS age * test,\n" +
            "   WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND,\n" +
            "   PRIMARY KEY (user_id, name) NOT ENFORCED \n" +
            ")\n" +
            "PARTITIONED BY (user_id, name)\n" +
            "WITH (\n" +
            "  'connector' = 'kafka',\n" +
            "  'test' = 'true'\n" +
            ");");
        ReverseContext context = ReverseContext.builder().build();
        BaseStatement reverse = flinkTransformer.reverse(dialectNode, context);
        assertTrue(reverse instanceof CreateTable);
        CreateTable createTable = (CreateTable) reverse;
        assertEquals(3, createTable.getQualifiedName().getParts().size());
        assertEquals("catalog.database.mytable", createTable.getQualifiedName().toString());
        assertEquals(11, createTable.getColumnDefines().size());
        ColumnDefinition userId = createTable.getColumnDefines().get(0);
        assertEquals("user_id", userId.getColName().getValue());
        assertEquals("BIGINT", userId.getDataType().toString());
        assertEquals("23", userId.getComment().getComment());
        ColumnDefinition age = createTable.getColumnDefines().get(2);
        assertEquals("age", age.getColName().getValue());
        assertEquals("VARCHAR(4)", age.getDataType().toString());
        assertEquals(true, age.getNotNull());
        ColumnDefinition test = createTable.getColumnDefines().get(4);
        assertEquals("DECIMAL(20,4)", test.getDataType().toString());
        ColumnDefinition array = createTable.getColumnDefines().get(5);
        assertEquals("ARRAY<STRING>", array.getDataType().toString());
        ColumnDefinition map = createTable.getColumnDefines().get(6);
        assertEquals("MAP<STRING,BIGINT>", map.getDataType().toString());
        ColumnDefinition row = createTable.getColumnDefines().get(7);
        assertEquals("ROW<a STRING,b BIGINT>", row.getDataType().toString());
        ColumnDefinition raw = createTable.getColumnDefines().get(8);
        assertEquals("RAW('a','b')", raw.getDataType().toString());

        ColumnDefinition timestamp = createTable.getColumnDefines().get(9);
        assertEquals("TIMESTAMP_LTZ(3)", timestamp.getDataType().toString());
        assertEquals(2, timestamp.getColumnProperties().size());
        assertEquals("metadata", timestamp.getColumnProperties().get(0).getName());
        assertEquals("true", timestamp.getColumnProperties().get(0).getValue());
        assertEquals("metadata_key", timestamp.getColumnProperties().get(1).getName());
        assertEquals("row", timestamp.getColumnProperties().get(1).getValue());

        ColumnDefinition cost = createTable.getColumnDefines().get(10);
        assertEquals(2, cost.getColumnProperties().size());
        assertEquals("computed", cost.getColumnProperties().get(0).getName());
        assertEquals("true", cost.getColumnProperties().get(0).getValue());
        assertEquals("computed_column_expression", cost.getColumnProperties().get(1).getName());
        assertEquals("age * test", cost.getColumnProperties().get(1).getValueLiteral().getOrigin());

        assertEquals(2, createTable.getConstraintStatements().size());
        PrimaryConstraint primaryConstraint = (PrimaryConstraint)createTable.getConstraintStatements().get(0);
        assertEquals("[user_id, name]", primaryConstraint.getColNames().toString());
        WaterMarkConstraint waterMarkConstraint = (WaterMarkConstraint)createTable.getConstraintStatements().get(1);
        assertEquals("order_time", waterMarkConstraint.getColumn().toString());
        assertEquals("order_time - INTERVAL '5' SECOND", waterMarkConstraint.getExpression().getOrigin());

        PartitionedBy partitionedBy = createTable.getPartitionedBy();
        assertEquals(2, partitionedBy.getColumnDefinitions().size());
        assertEquals("[user_id, name]", partitionedBy.getColumnDefinitions().stream()
            .map(ColumnDefinition::getColName).collect(Collectors.toList()).toString());

        assertEquals(2, createTable.getProperties().size());
        assertEquals("connector", createTable.getProperties().get(0).getName());
        assertEquals("kafka", createTable.getProperties().get(0).getValue());
        assertEquals("test", createTable.getProperties().get(1).getName());
        assertEquals("true", createTable.getProperties().get(1).getValue());
    }

    @Test
    public void testTransform() throws Exception {
        DialectNode dialectNode = new DialectNode("CREATE TABLE catalog.database.MyTable (\n" +
            "  `user_id` BIGINT COMMENT '23',\n" +
            "  `name` STRING NOT NULL,\n" +
            "  `age` VARCHAR(4) NOT NULL,\n" +
            "  `time` TIMESTAMP(4) WITH LOCAL TIME ZONE,\n" +
            "  `test` DECIMAL(20,4),\n" +
            "  `array` ARRAY<STRING>,\n" +
            "  `map` MAP<STRING,BIGINT>,\n" +
            "  `row` ROW<a STRING, b BIGINT>,\n" +
            "  `raw` RAW('a','b'),\n" +
            "  `timestamp` TIMESTAMP_LTZ(3) METADATA FROM `row`,  -- use column name as metadata key\n" +
            "  `cost` AS age * test,\n" +
            "   WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND,\n" +
            "   PRIMARY KEY (user_id, name) NOT ENFORCED \n" +
            ")\n" +
            "PARTITIONED BY (user_id, name)\n" +
            "WITH (\n" +
            "  'connector' = 'kafka',\n" +
            "  'test' = 'true'\n" +
            ");");
        ReverseContext context = ReverseContext.builder().build();
        BaseStatement createTable = flinkTransformer.reverse(dialectNode, context);

        DialectNode result = flinkTransformer.transform(createTable, new TransformContext(null));
        Assert.assertEquals("CREATE TABLE catalog.database.MyTable\n" +
            "(\n" +
            "   user_id   BIGINT COMMENT '23',\n" +
            "   name      STRING NOT NULL,\n" +
            "   age       VARCHAR(4) NOT NULL,\n" +
            "   time      TIMESTAMP(4) WITH LOCAL TIME ZONE,\n" +
            "   test      DECIMAL(20,4),\n" +
            "   array     ARRAY<STRING>,\n" +
            "   map       MAP<STRING,BIGINT>,\n" +
            "   row       ROW<a STRING,b BIGINT>,\n" +
            "   raw       RAW('a','b'),\n" +
            "   timestamp TIMESTAMP_LTZ(3) METADATA FROM row,\n" +
            "   cost      AS age * test,\n" +
            "   WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND,\n" +
            "   PRIMARY KEY(user_id,name)\n" +
            ")\n" +
            "PARTITIONED BY (user_id, name)\n" +
            "WITH ('connector'='kafka','test'='true');", result.getNode());
    }

    @Test
    public void testTransformTable() throws Exception {
        DialectNode dialectNode = new DialectNode("CREATE TABLE catalog.database.MyTable (\n" +
            "  `user_id` BIGINT COMMENT '23',\n" +
            "  `name` STRING NOT NULL,\n" +
            "  `age` VARCHAR(4) NOT NULL,\n" +
            "  `time` TIMESTAMP(4) WITH LOCAL TIME ZONE,\n" +
            "  `test` DECIMAL(20,4),\n" +
            "  `array` ARRAY<STRING>,\n" +
            "  `map` MAP<STRING,BIGINT>,\n" +
            "  `row` ROW<a STRING, b BIGINT>,\n" +
            "  `raw` RAW('a','b'),\n" +
            "  `timestamp` TIMESTAMP_LTZ(3) METADATA FROM `row`,  -- use column name as metadata key\n" +
            "  `cost` AS age * test,\n" +
            "   WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND,\n" +
            "   PRIMARY KEY (user_id, name) NOT ENFORCED \n" +
            ")\n" +
            "PARTITIONED BY (user_id, name)\n" +
            "WITH (\n" +
            "  'connector' = 'kafka',\n" +
            "  'test' = 'true'\n" +
            ");");
        ReverseContext context = ReverseContext.builder().build();
        BaseStatement createTable = flinkTransformer.reverse(dialectNode, context);

        Table table = flinkTransformer.transformTable(createTable, new TransformContext(null));
        assertEquals("catalog", table.getCatalog());
        assertEquals("database", table.getDatabase());
        assertEquals("mytable", table.getName());
        assertEquals(11, table.getColumns().size());
        Column userId = table.getColumns().get(0);
        assertEquals("user_id", userId.getName());
        assertEquals("BIGINT", userId.getDataType());
        assertEquals("23", userId.getComment());
        assertEquals(true, userId.isPartitionKey());
        assertEquals(0, userId.getPartitionKeyIndex().intValue());
        Column age = table.getColumns().get(2);
        assertEquals("age", age.getName());
        assertEquals("VARCHAR", age.getDataType());
        assertEquals(4, age.getLength().intValue());
        assertEquals(false, age.isNullable());
        Column test = table.getColumns().get(4);
        assertEquals("DECIMAL", test.getDataType());
        assertEquals(20, test.getPrecision().intValue());
        assertEquals(4, test.getScale().intValue());
        Column array = table.getColumns().get(5);
        assertEquals("ARRAY<STRING>", array.getDataType());
        Column map = table.getColumns().get(6);
        assertEquals("MAP<STRING,BIGINT>", map.getDataType());
        Column row = table.getColumns().get(7);
        assertEquals("ROW<a STRING,b BIGINT>", row.getDataType());
        Column raw = table.getColumns().get(8);
        assertEquals("RAW('a','b')", raw.getDataType());

        Column timestamp = table.getColumns().get(9);
        assertEquals("TIMESTAMP_LTZ", timestamp.getDataType());
        assertEquals(3, timestamp.getLength().intValue());
        assertEquals(2, timestamp.getProperties().size());
        assertEquals("metadata", timestamp.getProperties().get(0).getKey());
        assertEquals("true", timestamp.getProperties().get(0).getValue());
        assertEquals("metadata_key", timestamp.getProperties().get(1).getKey());
        assertEquals("row", timestamp.getProperties().get(1).getValue());

        Column cost = table.getColumns().get(10);
        assertEquals(2, cost.getProperties().size());
        assertEquals("computed", cost.getProperties().get(0).getKey());
        assertEquals("true", cost.getProperties().get(0).getValue());
        assertEquals("computed_column_expression", cost.getProperties().get(1).getKey());
        assertEquals("age * test", cost.getProperties().get(1).getValue());

        assertEquals(2, table.getConstraints().size());
        Constraint waterMarkConstraint = table.getConstraints().get(0);
        assertEquals("[order_time]", waterMarkConstraint.getColumns().toString());
        assertEquals("order_time - INTERVAL '5' SECOND", waterMarkConstraint.getProperties().get(0).getValue());

        Constraint primaryKeyConstraint = table.getConstraints().get(1);
        assertEquals(OutlineConstraintType.PRIMARY_KEY, primaryKeyConstraint.getType());
        assertEquals("[user_id, name]", primaryKeyConstraint.getColumns().toString());

        assertEquals(2, table.getProperties().size());
        assertEquals("connector", table.getProperties().get(0).getKey());
        assertEquals("kafka", table.getProperties().get(0).getValue());
        assertEquals("test", table.getProperties().get(1).getKey());
        assertEquals("true", table.getProperties().get(1).getValue());
    }


    @Test
    public void testReverseTable() throws Exception {
        DialectNode dialectNode = new DialectNode("CREATE TABLE catalog.database.MyTable (\n" +
            "  `user_id` BIGINT COMMENT '23',\n" +
            "  `name` STRING NOT NULL,\n" +
            "  `age` VARCHAR(4) NOT NULL,\n" +
            "  `time` TIMESTAMP(4) WITH LOCAL TIME ZONE,\n" +
            "  `test` DECIMAL(20,4),\n" +
            "  `array` ARRAY<STRING>,\n" +
            "  `map` MAP<STRING,BIGINT>,\n" +
            "  `row` ROW<a STRING, b BIGINT>,\n" +
            "  `raw` RAW('a','b'),\n" +
            "  `timestamp` TIMESTAMP_LTZ(3) METADATA FROM `row`,  -- use column name as metadata key\n" +
            "  `cost` AS age * test,\n" +
            "   WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND,\n" +
            "   PRIMARY KEY (user_id, name) NOT ENFORCED \n" +
            ")\n" +
            "PARTITIONED BY (user_id, name)\n" +
            "WITH (\n" +
            "  'connector' = 'kafka',\n" +
            "  'test' = 'true'\n" +
            ");");
        ReverseContext context = ReverseContext.builder().build();
        BaseStatement createTableStatement = flinkTransformer.reverse(dialectNode, context);
        Table table = flinkTransformer.transformTable(createTableStatement, new TransformContext(null));

        Node result = flinkTransformer.reverseTable(table, null);
        assertTrue(result instanceof CreateTable);
        CreateTable createTable = (CreateTable) result;
        assertEquals(3, createTable.getQualifiedName().getParts().size());
        assertEquals("catalog.database.mytable", createTable.getQualifiedName().toString());
        assertEquals(11, createTable.getColumnDefines().size());
        ColumnDefinition userId = createTable.getColumnDefines().get(0);
        assertEquals("user_id", userId.getColName().getValue());
        assertEquals("BIGINT", userId.getDataType().toString());
        assertEquals("23", userId.getComment().getComment());
        ColumnDefinition age = createTable.getColumnDefines().get(2);
        assertEquals("age", age.getColName().getValue());
        assertEquals("VARCHAR(4)", age.getDataType().toString());
        assertEquals(true, age.getNotNull());
        ColumnDefinition test = createTable.getColumnDefines().get(4);
        assertEquals("DECIMAL(20,4)", test.getDataType().toString());
        ColumnDefinition array = createTable.getColumnDefines().get(5);
        assertEquals("ARRAY<STRING>", array.getDataType().toString());
        ColumnDefinition map = createTable.getColumnDefines().get(6);
        assertEquals("MAP<STRING,BIGINT>", map.getDataType().toString());
        ColumnDefinition row = createTable.getColumnDefines().get(7);
        assertEquals("ROW<a STRING,b BIGINT>", row.getDataType().toString());
        ColumnDefinition raw = createTable.getColumnDefines().get(8);
        assertEquals("RAW('a','b')", raw.getDataType().toString());

        ColumnDefinition timestamp = createTable.getColumnDefines().get(9);
        assertEquals("TIMESTAMP_LTZ(3)", timestamp.getDataType().toString());
        assertEquals(3, timestamp.getColumnProperties().size());
        assertEquals("metadata", timestamp.getColumnProperties().get(1).getName());
        assertEquals("true", timestamp.getColumnProperties().get(1).getValue());
        assertEquals("metadata_key", timestamp.getColumnProperties().get(2).getName());
        assertEquals("row", timestamp.getColumnProperties().get(2).getValue());

        ColumnDefinition cost = createTable.getColumnDefines().get(10);
        assertEquals(3, cost.getColumnProperties().size());
        assertEquals("computed", cost.getColumnProperties().get(1).getName());
        assertEquals("true", cost.getColumnProperties().get(1).getValue());
        assertEquals("computed_column_expression", cost.getColumnProperties().get(2).getName());
        assertEquals("age * test", cost.getColumnProperties().get(2).getValue());

        assertEquals(2, createTable.getConstraintStatements().size());
        PrimaryConstraint primaryConstraint = (PrimaryConstraint)createTable.getConstraintStatements().get(1);
        assertEquals("[user_id, name]", primaryConstraint.getColNames().toString());
        WaterMarkConstraint waterMarkConstraint = (WaterMarkConstraint)createTable.getConstraintStatements().get(0);
        assertEquals("order_time", waterMarkConstraint.getColumn().toString());
        assertEquals("order_time - INTERVAL '5' SECOND", waterMarkConstraint.getExpression().getValue());

        PartitionedBy partitionedBy = createTable.getPartitionedBy();
        assertEquals(2, partitionedBy.getColumnDefinitions().size());
        assertEquals("[user_id, name]", partitionedBy.getColumnDefinitions().stream()
            .map(ColumnDefinition::getColName).collect(Collectors.toList()).toString());

        assertEquals(2, createTable.getProperties().size());
        assertEquals("connector", createTable.getProperties().get(0).getName());
        assertEquals("kafka", createTable.getProperties().get(0).getValue());
        assertEquals("test", createTable.getProperties().get(1).getName());
        assertEquals("true", createTable.getProperties().get(1).getValue());
    }

    @Test
    public void testReverseByFlinkParser() throws Exception {
        DialectNode dialectNode = new DialectNode("CREATE TABLE catalog.database.MyTable (\n" +
            "  `user_id` BIGINT COMMENT '23',\n" +
            "  `name` STRING NOT NULL,\n" +
            "  `age` VARCHAR(4) NOT NULL,\n" +
            "  `time` TIMESTAMP(4) WITH LOCAL TIME ZONE,\n" +
            "  `test` DECIMAL(20,4),\n" +
            "  `array` ARRAY<STRING>,\n" +
            "  `map` MAP<STRING,BIGINT>,\n" +
            "  `row` ROW<a STRING, b BIGINT>,\n" +
            "  `raw` RAW('a','b'),\n" +
            "  `timestamp` TIMESTAMP_LTZ(3) METADATA FROM \"row\",  -- use column name as metadata key\n" +
            "  `cost` AS age * test,\n" +
            "   WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND,\n" +
            "   PRIMARY KEY (user_id, name) NOT ENFORCED \n" +
            ")\n" +
            "PARTITIONED BY (user_id, name)\n" +
            "WITH (\n" +
            "  'connector' = 'kafka',\n" +
            "  'test' = 'true'\n" +
            ");");
        Property property = new Property(FLINK_PARSE_MODE_KEY, FLINK_PARSE_MODE_SDK);
        ReverseContext context = ReverseContext.builder().property(property).build();
        BaseStatement reverse = flinkTransformer.reverse(dialectNode, context);
        assertTrue(reverse instanceof CreateTable);
        CreateTable createTable = (CreateTable) reverse;
        assertEquals(3, createTable.getQualifiedName().getParts().size());
        assertEquals("catalog.database.mytable", createTable.getQualifiedName().toString());
        assertEquals(11, createTable.getColumnDefines().size());
        ColumnDefinition userId = createTable.getColumnDefines().get(0);
        assertEquals("user_id", userId.getColName().getValue());
        assertEquals("BIGINT", userId.getDataType().toString());
        assertEquals("23", userId.getComment().getComment());
        ColumnDefinition age = createTable.getColumnDefines().get(2);
        assertEquals("age", age.getColName().getValue());
        assertEquals("VARCHAR(4)", age.getDataType().toString());
        assertEquals(true, age.getNotNull());
        ColumnDefinition test = createTable.getColumnDefines().get(4);
        assertEquals("DECIMAL(20,4)", test.getDataType().toString());
        ColumnDefinition array = createTable.getColumnDefines().get(5);
        assertEquals("ARRAY<STRING>", array.getDataType().toString());
        ColumnDefinition map = createTable.getColumnDefines().get(6);
        assertEquals("MAP<STRING,BIGINT>", map.getDataType().toString());
        ColumnDefinition row = createTable.getColumnDefines().get(7);
        assertEquals("ROW<a STRING,b BIGINT>", row.getDataType().toString());
        ColumnDefinition raw = createTable.getColumnDefines().get(8);
        assertEquals("RAW('a','b')", raw.getDataType().toString());

        ColumnDefinition timestamp = createTable.getColumnDefines().get(9);
        assertEquals("TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)", timestamp.getDataType().toString());
        assertEquals(2, timestamp.getColumnProperties().size());
        assertEquals("metadata", timestamp.getColumnProperties().get(0).getName());
        assertEquals("true", timestamp.getColumnProperties().get(0).getValue());
        assertEquals("metadata_key", timestamp.getColumnProperties().get(1).getName());
        assertEquals("row", timestamp.getColumnProperties().get(1).getValue());

        ColumnDefinition cost = createTable.getColumnDefines().get(10);
        assertEquals(2, cost.getColumnProperties().size());
        assertEquals("computed", cost.getColumnProperties().get(0).getName());
        assertEquals("true", cost.getColumnProperties().get(0).getValue());
        assertEquals("computed_column_expression", cost.getColumnProperties().get(1).getName());
        assertEquals("age*test", cost.getColumnProperties().get(1).getValueLiteral().getOrigin());

        assertEquals(2, createTable.getConstraintStatements().size());
        PrimaryConstraint primaryConstraint = (PrimaryConstraint)createTable.getConstraintStatements().get(0);
        assertEquals("[user_id, name]", primaryConstraint.getColNames().toString());
        WaterMarkConstraint waterMarkConstraint = (WaterMarkConstraint)createTable.getConstraintStatements().get(1);
        assertEquals("order_time", waterMarkConstraint.getColumn().toString());
        assertEquals("order_time-INTERVAL '5' SECOND", waterMarkConstraint.getExpression().getValue());

        PartitionedBy partitionedBy = createTable.getPartitionedBy();
        assertEquals(2, partitionedBy.getColumnDefinitions().size());
        assertEquals("[user_id, name]", partitionedBy.getColumnDefinitions().stream()
            .map(ColumnDefinition::getColName).collect(Collectors.toList()).toString());

        assertEquals(2, createTable.getProperties().size());
        assertEquals("connector", createTable.getProperties().get(0).getName());
        assertEquals("kafka", createTable.getProperties().get(0).getValue());
        assertEquals("test", createTable.getProperties().get(1).getName());
        assertEquals("true", createTable.getProperties().get(1).getValue());
    }

}

//Generated with love by TestMe :) Please report issues and submit feature requests at: http://weirddev.com/forum#!/testme