package com.aliyun.fastmodel.transform.spark.format;

import java.util.List;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.hive.format.HivePropertyKey;
import com.aliyun.fastmodel.transform.spark.builder.DefaultBuilder;
import com.aliyun.fastmodel.transform.spark.context.SparkTableFormat;
import com.aliyun.fastmodel.transform.spark.context.SparkTransformContext;
import com.aliyun.fastmodel.transform.spark.parser.SparkLanguageParser;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2023/2/13
 */
public class SparkVisitorTest {

    SparkVisitor sparkVisitor;

    @Before
    public void setUp() throws Exception {
        sparkVisitor = new SparkVisitor(SparkTransformContext.builder().build());
    }

    @Test
    public void testCreateTable() {
        List<ColumnDefinition> columns = Lists.newArrayList();
        columns.add(ColumnDefinition.builder().colName(new Identifier("c1")).dataType(DataTypeUtil.simpleType("bigint", null)).build());
        List<Property> properties = Lists.newArrayList();
        properties.add(new Property(SparkPropertyKey.CLUSTERED_BY.getValue(), "a, b"));
        properties.add(new Property(SparkPropertyKey.SORTED_BY.getValue(), "a desc, b asc"));
        properties.add(new Property(HivePropertyKey.LOCATION.getValue(), "hdfs://1234"));
        properties.add(new Property(HivePropertyKey.FIELDS_TERMINATED.getValue(), "\\n"));
        properties.add(new Property(HivePropertyKey.LINES_TERMINATED.getValue(), "\\n"));
        properties.add(new Property(SparkPropertyKey.NUMBER_BUCKETS.getValue(), "4"));
        properties.add(new Property("abc", "bcd"));
        CreateTable node = CreateTable.builder()
            .tableName(QualifiedName.of("abc"))
            .columns(columns)
            .partition(new PartitionedBy(columns))
            .properties(properties)
            .comment(new Comment("abc"))
            .build();
        sparkVisitor.visitCreateTable(node, 0);
        String s = sparkVisitor.getBuilder().toString();
        assertEquals(s, "CREATE TABLE abc\n"
            + "(\n"
            + "   c1 BIGINT\n"
            + ")\n"
            + "COMMENT 'abc'\n"
            + "PARTITIONED BY\n"
            + "(\n"
            + "   c1 BIGINT\n"
            + ")\n"
            + "CLUSTERED BY (a, b)\n"
            + "SORTED BY (a desc, b asc)\n"
            + "INTO 4 BUCKETS\n"
            + "ROW FORMAT DELIMITED\n"
            + "FIELDS TERMINATED BY '\\n'\n"
            + "LINES TERMINATED BY '\\n'\n"
            + "LOCATION 'hdfs://1234'\n"
            + "TBLPROPERTIES ('abc'='bcd')");
    }

    @Test
    public void testDataSourceFormat() {
        List<ColumnDefinition> columns = Lists.newArrayList();
        columns.add(ColumnDefinition.builder().colName(new Identifier("c1")).dataType(DataTypeUtil.simpleType("bigint", null)).build());
        List<Property> properties = Lists.newArrayList();
        properties.add(new Property(SparkPropertyKey.CLUSTERED_BY.getValue(), "a, b"));
        properties.add(new Property(SparkPropertyKey.SORTED_BY.getValue(), "a desc, b asc"));
        properties.add(new Property(HivePropertyKey.LOCATION.getValue(), "hdfs://1234"));
        properties.add(new Property(HivePropertyKey.FIELDS_TERMINATED.getValue(), "\\n"));
        properties.add(new Property(HivePropertyKey.LINES_TERMINATED.getValue(), "\\n"));
        properties.add(new Property(SparkPropertyKey.NUMBER_BUCKETS.getValue(), "4"));
        properties.add(new Property(SparkPropertyKey.USING.getValue(), "CSV"));
        properties.add(new Property("abc", "bcd"));
        SparkVisitor sparkVisitor = new SparkVisitor(SparkTransformContext.builder().tableFormat(SparkTableFormat.DATASOURCE_FORMAT).build());
        CreateTable node = CreateTable.builder()
            .tableName(QualifiedName.of("abc"))
            .columns(columns)
            .partition(new PartitionedBy(columns))
            .properties(properties)
            .comment(new Comment("comment"))
            .build();
        sparkVisitor.visitCreateTable(node, 0);
        String s = sparkVisitor.getBuilder().toString();
        assertEquals(s, "CREATE TABLE abc\n"
            + "(\n"
            + "   c1 BIGINT\n"
            + ")\n"
            + "USING CSV\n"
            + "PARTITIONED BY\n"
            + "(\n"
            + "   c1 BIGINT\n"
            + ")\n"
            + "CLUSTERED BY (a, b)\n"
            + "SORTED BY (a desc, b asc)\n"
            + "INTO 4 BUCKETS\n"
            + "LOCATION 'hdfs://1234'\n"
            + "COMMENT 'comment'\n"
            + "TBLPROPERTIES ('abc'='bcd')");
    }

    @Test
    public void testCreateTableWithReversedWord() {
        // 测试复杂数据类型: ARRAY, MAP, STRUCT
        String parseNode = "CREATE TABLE `default`.test_complex_table ("
            + "col_array ARRAY<STRING>, "
            + "col_map MAP<STRING, INT>, "
            + "col_struct STRUCT<field1: STRING, field2: INT>"
            + ") COMMENT 'Test table with complex data types'";

        SparkLanguageParser sparkLanguageParser = new SparkLanguageParser();
        Node o = sparkLanguageParser.parseNode(parseNode);
        DefaultBuilder defaultBuilder = new DefaultBuilder();
        DialectNode build = defaultBuilder.build((BaseStatement)o, SparkTransformContext.builder().database("default").build());

        // 验证解析结果
        assertEquals(build.getNode(), "CREATE TABLE `default`.test_complex_table\n"
            + "(\n"
            + "   col_array  ARRAY<STRING>,\n"
            + "   col_map    MAP<STRING,INT>,\n"
            + "   col_struct STRUCT<field1:STRING,field2:INT>\n"
            + ")\n"
            + "COMMENT 'Test table with complex data types'");
    }

    @Test
    public void testCreateTableWithoutReversedWord() {
        // 测试复杂数据类型: ARRAY, MAP, STRUCT
        String parseNode = "CREATE TABLE `dedefault`.test_complex_table ("
            + "col_array ARRAY<STRING>, "
            + "col_map MAP<STRING, INT>, "
            + "col_struct STRUCT<field1: STRING, field2: INT>"
            + ") COMMENT 'Test table with complex data types'";

        SparkLanguageParser sparkLanguageParser = new SparkLanguageParser();
        Node o = sparkLanguageParser.parseNode(parseNode);
        DefaultBuilder defaultBuilder = new DefaultBuilder();
        DialectNode build = defaultBuilder.build((BaseStatement)o, SparkTransformContext.builder().database("dedefault").build());

        // 验证解析结果
        assertEquals(build.getNode(), "CREATE TABLE dedefault.test_complex_table\n"
            + "(\n"
            + "   col_array  ARRAY<STRING>,\n"
            + "   col_map    MAP<STRING,INT>,\n"
            + "   col_struct STRUCT<field1:STRING,field2:INT>\n"
            + ")\n"
            + "COMMENT 'Test table with complex data types'");
    }

}