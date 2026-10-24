/*
 * Copyright 2021 Alibaba Group Holding Ltd.
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

package com.aliyun.fastmodel.transform.mc;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.datatype.Field;
import com.aliyun.fastmodel.core.tree.datatype.JsonDataType;
import com.aliyun.fastmodel.core.tree.statement.constants.TableDetailType;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateDimTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateDwsTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.parser.NodeParser;
import com.aliyun.fastmodel.transform.api.client.dto.constraint.Constraint;
import com.aliyun.fastmodel.transform.api.client.dto.constraint.OutlineConstraintType;
import com.aliyun.fastmodel.transform.api.client.dto.table.Column;
import com.aliyun.fastmodel.transform.api.client.dto.table.Table;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.mc.parser.tree.constraint.ClusteredByConstraint;
import com.google.common.collect.ImmutableList;
import org.apache.commons.io.IOUtils;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/3/18
 */
public class MaxComputeTransformerTest {

    @Test
    public void from() {
        MaxComputeTransformer maxComputeTransformer = new MaxComputeTransformer();
        BaseStatement from = maxComputeTransformer.reverse(new DialectNode("select  _dws_all.ds ds from _dws_all;"));
        assertNotNull(from);
    }

    @Test
    public void testCreate() {
        MaxComputeTransformer maxComputeTransformer = new MaxComputeTransformer();
        BaseStatement from = maxComputeTransformer.reverse(
            new DialectNode("create table b LIFECYCLE 1;"),
            ReverseContext.builder().reverseTableType(TableDetailType.DWS).build()
        );
        assertNotNull(from);
    }

    @Test
    public void testTransform() {
        String ddl = "CREATE TABLE IF NOT EXISTS example_type_table ("
            + "example_value STRING COMMENT 'Synthetic value',"
            + "example_label STRING COMMENT 'Synthetic label',"
            + "example_name STRING COMMENT 'Synthetic name',"
            + "example_timestamp STRING COMMENT 'Synthetic processing time') "
            + "COMMENT 'Synthetic type regression' "
            + "PARTITIONED BY (example_day STRING COMMENT 'Synthetic partition') LIFECYCLE 7;";
        MaxComputeTransformer transformer = new MaxComputeTransformer();
        CreateTable table = (CreateTable)transformer.reverse(new DialectNode(ddl),
            ReverseContext.builder().reverseTableType(TableDetailType.NORMAL_DIM).build());
        assertEquals(4, table.getColumnDefines().size());
        assertEquals("STRING", table.getColumnDefines().get(0).getDataType()
            .toString().replace(" ", ""));
    }

    @Test
    public void testReverseContent() throws IOException {
        String fml = IOUtils.resourceToString("/synthetic-reverse-table.sql", StandardCharsets.UTF_8);
        MaxComputeTransformer maxComputeTransformer = new MaxComputeTransformer();
        BaseStatement reverse = maxComputeTransformer.reverse(new DialectNode(fml),
            ReverseContext.builder().reverseTableType(TableDetailType.NORMAL_DIM).build()
        );
        assertNotNull(reverse);
        CreateTable table = (CreateTable)reverse;
        assertEquals(6, table.getColumnDefines().size());
        assertEquals("ARRAY<STRING>", table.getColumnDefines().get(3).getDataType()
            .toString().replace(" ", ""));
        assertEquals("ARRAY<STRING>", table.getColumnDefines().get(4).getDataType()
            .toString().replace(" ", ""));
    }

    @Test
    public void testTransformArray() {
        String ddl = "CREATE TABLE IF NOT EXISTS example_type_table ("
            + "example_value ARRAY<STRING> COMMENT 'Synthetic value',"
            + "example_label STRING COMMENT 'Synthetic label',"
            + "example_name STRING COMMENT 'Synthetic name',"
            + "example_timestamp STRING COMMENT 'Synthetic processing time') "
            + "COMMENT 'Synthetic type regression' "
            + "PARTITIONED BY (example_day STRING COMMENT 'Synthetic partition') LIFECYCLE 7;";
        MaxComputeTransformer transformer = new MaxComputeTransformer();
        CreateTable table = (CreateTable)transformer.reverse(new DialectNode(ddl),
            ReverseContext.builder().reverseTableType(TableDetailType.NORMAL_DIM).build());
        assertEquals(4, table.getColumnDefines().size());
        assertEquals("ARRAY<STRING>", table.getColumnDefines().get(0).getDataType()
            .toString().replace(" ", ""));
    }

    @Test
    public void testTransformRArray() {
        String ddl = "CREATE TABLE IF NOT EXISTS example_type_table ("
            + "example_value ARRAY<ARRAY<STRING>> COMMENT 'Synthetic value',"
            + "example_label STRING COMMENT 'Synthetic label',"
            + "example_name STRING COMMENT 'Synthetic name',"
            + "example_timestamp STRING COMMENT 'Synthetic processing time') "
            + "COMMENT 'Synthetic type regression' "
            + "PARTITIONED BY (example_day STRING COMMENT 'Synthetic partition') LIFECYCLE 7;";
        MaxComputeTransformer transformer = new MaxComputeTransformer();
        CreateTable table = (CreateTable)transformer.reverse(new DialectNode(ddl),
            ReverseContext.builder().reverseTableType(TableDetailType.NORMAL_DIM).build());
        assertEquals(4, table.getColumnDefines().size());
        assertEquals("ARRAY<ARRAY<STRING>>", table.getColumnDefines().get(0).getDataType()
            .toString().replace(" ", ""));
    }

    @Test
    public void testTransformMap() {
        String ddl = "CREATE TABLE IF NOT EXISTS example_nested_attributes ("
            + "example_id BIGINT COMMENT 'Synthetic identifier',"
            + "example_label STRING COMMENT 'Synthetic label',"
            + "example_score DOUBLE COMMENT 'Synthetic score',"
            + "example_attributes map<string,string> COMMENT 'Synthetic nested attributes') "
            + "COMMENT 'Synthetic nested type regression' "
            + "PARTITIONED BY (example_day STRING COMMENT 'Synthetic partition') LIFECYCLE 7;";
        MaxComputeTransformer transformer = new MaxComputeTransformer();
        CreateTable table = (CreateTable)transformer.reverse(new DialectNode(ddl),
            ReverseContext.builder().reverseTableType(TableDetailType.NORMAL_DIM).build());
        assertEquals(4, table.getColumnDefines().size());
        assertEquals("MAP<STRING,STRING>", table.getColumnDefines().get(3).getDataType()
            .toString().replace(" ", ""));
        assertNotNull(transformer.transform(table).getNode());
    }

    @Test
    public void testTransformComplex() {
        String fml = "create dim table example_struct_table (example_items array<struct<"
            + "example_id:bigint comment 'Synthetic identifier',example_label:string,"
            + "example_attributes:map<string,string>>>);";
        NodeParser nodeParser = new NodeParser();
        MaxComputeTransformer transformer = new MaxComputeTransformer();
        DialectNode transform = transformer.transform(nodeParser.parseNode(fml));
        assertEquals("CREATE TABLE example_struct_table\n"
            + "(\n"
            + "   example_items ARRAY<STRUCT<example_id:BIGINT COMMENT 'Synthetic identifier',"
            + "example_label:STRING,example_attributes:MAP<STRING,STRING>>>\n"
            + ")\n", transform.getNode());
    }

    @Test
    public void testTransformArrayMap() {
        String ddl = "CREATE TABLE IF NOT EXISTS example_nested_attributes ("
            + "example_id BIGINT COMMENT 'Synthetic identifier',"
            + "example_label STRING COMMENT 'Synthetic label',"
            + "example_score DOUBLE COMMENT 'Synthetic score',"
            + "example_attributes array<map<string,string>> COMMENT 'Synthetic nested attributes') "
            + "COMMENT 'Synthetic nested type regression' "
            + "PARTITIONED BY (example_day STRING COMMENT 'Synthetic partition') LIFECYCLE 7;";
        MaxComputeTransformer transformer = new MaxComputeTransformer();
        CreateTable table = (CreateTable)transformer.reverse(new DialectNode(ddl),
            ReverseContext.builder().reverseTableType(TableDetailType.NORMAL_DIM).build());
        assertEquals(4, table.getColumnDefines().size());
        assertEquals("ARRAY<MAP<STRING,STRING>>", table.getColumnDefines().get(3).getDataType()
            .toString().replace(" ", ""));
        assertNotNull(transformer.transform(table).getNode());
    }

    @Test
    public void testDecimal() {
        String ddl = "create table a (col decimal(38,38) comment 'comment', col2 char(2) comment 'comment') comment 'comment'";
        MaxComputeTransformer maxComputeTransformer = new MaxComputeTransformer();
        BaseStatement reverse = maxComputeTransformer.reverse(new DialectNode(ddl),
            ReverseContext.builder().reverseTableType(TableDetailType.NORMAL_DIM).build()
        );
        assertEquals(reverse.toString(), "CREATE DIM TABLE a \n"
            + "(\n"
            + "   col  DECIMAL(38,38) COMMENT 'comment',\n"
            + "   col2 CHAR(2) COMMENT 'comment'\n"
            + ")\n"
            + "COMMENT 'comment'");
    }

    @Test
    public void testReverseTable() {
        MaxComputeTransformer maxComputeTransformer = new MaxComputeTransformer();
        List<Constraint> constraints = ImmutableList.of(
            Constraint.builder().name("c1").columns(ImmutableList.of("abc")).type(OutlineConstraintType.PRIMARY_KEY).build()
        );
        Column columns = Column.builder()
            .name("abc")
            .dataType("string")
            .comment("comment")
            .build();
        Table table = Table.builder()
            .name("name")
            .database("project")
            .columns(ImmutableList.of(columns))
            .lifecycleSeconds(10000L)
            .comment("comment")
            .constraints(constraints)
            .build();
        Node node = maxComputeTransformer.reverseTable(table);
        assertNotNull(node);
        CreateTable createTable = (CreateTable)node;
        assertEquals(createTable.getColumnDefines().size(), 1);
        List<Property> properties = createTable.getProperties();
        assertEquals(properties.size(), 1);
    }

    @Test
    public void testReversJson() {
        String ddl = "create table a (col json comment 'comment') comment 'comment'";
        MaxComputeTransformer maxComputeTransformer = new MaxComputeTransformer();
        BaseStatement reverse = maxComputeTransformer.reverse(new DialectNode(ddl),
            ReverseContext.builder().reverseTableType(TableDetailType.NORMAL_DIM).build()
        );
        CreateDimTable createDimTable = (CreateDimTable)reverse;
        List<ColumnDefinition> columnDefines = createDimTable.getColumnDefines();
        assertEquals(1, columnDefines.size());
        assertEquals(JsonDataType.class, columnDefines.get(0).getDataType().getClass());
    }

    @Test
    public void testReversJsonWithParam() {
        String ddl = "create table a (col json<a:string> comment 'comment') comment 'comment'";
        MaxComputeTransformer maxComputeTransformer = new MaxComputeTransformer();
        BaseStatement reverse = maxComputeTransformer.reverse(new DialectNode(ddl),
            ReverseContext.builder().reverseTableType(TableDetailType.NORMAL_DIM).build()
        );
        CreateDimTable createDimTable = (CreateDimTable)reverse;
        List<ColumnDefinition> columnDefines = createDimTable.getColumnDefines();
        assertEquals(1, columnDefines.size());
        BaseDataType dataType = columnDefines.get(0).getDataType();
        assertEquals(JsonDataType.class, dataType.getClass());
        JsonDataType jsonDataType = (JsonDataType)dataType;
        List<Field> fields = jsonDataType.getFields();
        assertEquals(1, fields.size());
        Field field = fields.get(0);
        assertEquals("a", field.getName().getValue());
    }

    @Test
    public void testClusteredBy() {
        MaxComputeTransformer maxComputeTransformer = new MaxComputeTransformer();
        BaseStatement from = maxComputeTransformer.reverse(
                new DialectNode("CREATE TABLE test_odps\n" +
                        "(\n" +
                        "    id     INT NOT NULL  PRIMARY COMMENT '唯一标识'\n" +
                        "    ,name  STRING NOT NULL DEFAULT '默认' COMMENT '名称'\n" +
                        "    ,price DECIMAL(20,4) NOT NULL COMMENT '价格'\n" +
                        "    ,age   MAP<STRING,STRING> COMMENT '复杂'\n" +
                        ")\n" +
                        "COMMENT '测试'\n" +
                        "PARTITIONED BY \n" +
                        "(\n" +
                        "    pt     STRING COMMENT '日期'\n" +
                        ")\n" +
                        "CLUSTERED BY (id)\n" +
                        "SORTED BY (id ASC) INTO 5 BUCKETS\n" +
                        "LIFECYCLE 365\n" +
                        ";"),
                ReverseContext.builder().reverseTableType(TableDetailType.DWS).build()
        );
        assertTrue(from instanceof CreateDwsTable);
        CreateDwsTable createDwsTable = (CreateDwsTable) from;

        assertEquals(2, createDwsTable.getConstraintStatements().size());
        assertTrue(createDwsTable.getConstraintStatements().get(0) instanceof ClusteredByConstraint);
        ClusteredByConstraint clusteredByConstraint = (ClusteredByConstraint)createDwsTable.getConstraintStatements().get(0);
        assertEquals(ClusteredByConstraint.ClusterType.HASH, clusteredByConstraint.getClusterType());
        assertEquals(5, clusteredByConstraint.getBucketNum().getValue().intValue());
    }

    @Test
    public void testClusteredBy2() {
        MaxComputeTransformer maxComputeTransformer = new MaxComputeTransformer();
        BaseStatement from = maxComputeTransformer.reverse(
                new DialectNode("CREATE TABLE test_odps\n" +
                        "(\n" +
                        "    id     INT NOT NULL PRIMARY COMMENT '唯一标识'\n" +
                        "    ,name  STRING NOT NULL DEFAULT '默认' COMMENT '名称'\n" +
                        "    ,price DECIMAL(20,4) NOT NULL COMMENT '价格'\n" +
                        "    ,age   MAP<STRING,STRING> COMMENT '复杂'\n" +
                        ")\n" +
                        "COMMENT '测试'\n" +
                        "PARTITIONED BY \n" +
                        "(\n" +
                        "    pt     STRING COMMENT '日期'\n" +
                        ")\n" +
                        "CLUSTERED BY (id)\n" +
                        "SORTED BY (id ASC) INTO 5 BUCKETS\n" +
                        "LIFECYCLE 365\n" +
                        ";"),
                ReverseContext.builder().reverseTableType(TableDetailType.DWS).build()
        );
        DialectNode dialectNode = maxComputeTransformer.transform(from, TransformContext.builder().build());
        assertEquals("CREATE TABLE test_odps\n" +
            "(\n" +
            "   id    INT NOT NULL COMMENT '唯一标识',\n" +
            "   name  STRING NOT NULL COMMENT '名称',\n" +
            "   price DECIMAL(20,4) NOT NULL COMMENT '价格',\n" +
            "   age   MAP<STRING,STRING> COMMENT '复杂',\n" +
            "   PRIMARY KEY(id)\n" +
            ")\n" +
            "COMMENT '测试'\n" +
            "PARTITIONED BY\n" +
            "(\n" +
            "   pt STRING COMMENT '日期'\n" +
            ")\n" +
            "CLUSTERED BY (id)\n" +
            "SORTED BY (id ASC) INTO 5 BUCKETS\n" +
            "LIFECYCLE 365", dialectNode.getNode());
    }
}