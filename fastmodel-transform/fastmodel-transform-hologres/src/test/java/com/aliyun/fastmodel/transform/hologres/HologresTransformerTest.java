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

package com.aliyun.fastmodel.transform.hologres;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.literal.LongLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.core.tree.statement.CompositeStatement;
import com.aliyun.fastmodel.core.tree.statement.table.AddCols;
import com.aliyun.fastmodel.core.tree.statement.table.ChangeCol;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateDimTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableComment;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.PrimaryConstraint;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.aliyun.fastmodel.transform.api.builder.merge.exception.MergeException;
import com.aliyun.fastmodel.transform.api.client.dto.constraint.Constraint;
import com.aliyun.fastmodel.transform.api.client.dto.constraint.OutlineConstraintType;
import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.api.client.dto.table.Column;
import com.aliyun.fastmodel.transform.api.client.dto.table.Table;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.hologres.client.property.DictEncodingColumn;
import com.aliyun.fastmodel.transform.hologres.client.property.SegmentKey;
import com.aliyun.fastmodel.transform.hologres.context.HologresTransformContext;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import lombok.SneakyThrows;
import org.apache.commons.io.IOUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/3/7
 */
public class HologresTransformerTest {

    HologresTransformer hologresTransformer = new HologresTransformer();

    @Test
    public void testTransform() {
        CreateDimTable dimTable = CreateDimTable.builder().tableName(
            QualifiedName.of("a.b")
        ).build();
        DialectNode transform = hologresTransformer.transform(dimTable, HologresTransformContext.builder().build());
        assertNotNull(transform.getNode());
    }

    @Test
    public void testChangeColumn() {
        ChangeCol changeCol = new ChangeCol(QualifiedName.of("abc"), new Identifier("bcd"),
            ColumnDefinition.builder()
                .colName(new Identifier("bcd"))
                .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
                .build()
        );
        DialectNode transform = hologresTransformer.transform(changeCol, HologresTransformContext.builder().build());
        assertEquals(transform.getNode(), "ALTER TABLE abc CHANGE COLUMN bcd bcd BIGINT");
        assertFalse(transform.isExecutable());
    }

    @Test
    public void testChangeColumnRename() {
        ChangeCol changeCol = new ChangeCol(QualifiedName.of("abc"), new Identifier("bcd"),
            ColumnDefinition.builder()
                .colName(new Identifier("bde"))
                .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
                .build()
        );
        DialectNode transform = hologresTransformer.transform(changeCol, HologresTransformContext.builder().build());
        assertEquals(transform.getNode(), "BEGIN;\n"
            + "ALTER TABLE abc RENAME COLUMN bcd TO bde;\n"
            + "COMMIT;");
        assertTrue(transform.isExecutable());
    }

    @Test
    public void testChangeColumnSetDefault() {
        ChangeCol changeCol = new ChangeCol(QualifiedName.of("abc"), new Identifier("bcd"),
            ColumnDefinition.builder()
                .colName(new Identifier("bcd"))
                .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
                .defaultValue(new StringLiteral("bcd"))
                .build()
        );
        DialectNode transform = hologresTransformer.transform(changeCol, HologresTransformContext.builder().build());
        assertEquals("BEGIN;\n"
            + "ALTER TABLE abc ALTER COLUMN bcd SET DEFAULT 'bcd';\n"
            + "COMMIT;", transform.getNode());
        assertTrue(transform.isExecutable());
    }

    @Test
    public void testChangeColumnDefaultNumber() {
        ChangeCol changeCol = new ChangeCol(QualifiedName.of("abc"), new Identifier("bcd"),
            ColumnDefinition.builder()
                .colName(new Identifier("bcd"))
                .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
                .defaultValue(new LongLiteral("1"))
                .build()
        );
        DialectNode transform = hologresTransformer.transform(changeCol, HologresTransformContext.builder().build());
        assertEquals(transform.getNode(), "BEGIN;\n"
            + "ALTER TABLE abc ALTER COLUMN bcd SET DEFAULT 1;\n"
            + "COMMIT;");
        assertTrue(transform.isExecutable());
    }

    @Test
    public void testTransformMap() {
        HologresTransformer hologresTransformer = new HologresTransformer();
        CreateDimTable dimTable = CreateDimTable.builder().tableName(
            QualifiedName.of("a.b")
        ).columns(
            ImmutableList.of(
                ColumnDefinition.builder().dataType(DataTypeUtil.simpleType(DataTypeEnums.MAP))
                    .colName(new Identifier("col1")).build()
            )

        ).build();
        DialectNode transform = hologresTransformer.transform(dimTable, HologresTransformContext.builder().build());
        assertEquals("BEGIN;\nCREATE TABLE b (\n"
            + "   col1 JSON\n"
            + ");\n\nCOMMIT;", transform.getNode());
    }

    @Test
    public void testTransformAddColumn() {
        HologresTransformer hologresTransformer = new HologresTransformer();
        AddCols addCols = new AddCols(
            QualifiedName.of("abc"),
            Lists.newArrayList(
                ColumnDefinition.builder()
                    .colName(new Identifier("abc"))
                    .dataType(DataTypeUtil.simpleType("BIGINT", null))
                    .build()
            )
        );
        DialectNode transform = hologresTransformer.transform(addCols);
        assertEquals(transform.getNode(), "BEGIN;\n"
            + "ALTER TABLE IF EXISTS abc ADD COLUMN abc BIGINT;\n"
            + "COMMIT;");
        assertTrue(transform.isExecutable());
    }

    @Test
    public void testReverseCommentTable() {
        HologresTransformer hologresTransformer = new HologresTransformer();
        BaseStatement reverse = hologresTransformer.reverse(new DialectNode("COMMENT ON TABLE molin_db.aa_exist_1 IS NULL;"));
        assertNotNull(reverse);
        SetTableComment setTableComment = (SetTableComment)reverse;
        assertEquals(setTableComment.getComment(), new Comment(null));

    }

    @Test
    public void testOwner() {
        HologresTransformer hologresTransformer = new HologresTransformer();
        BaseStatement reverse = hologresTransformer.reverse(new DialectNode("ALTER TABLE molin_db.aa_exist_1 OWNER TO molin_db_developer;"));
        assertNull(reverse);
    }

    @Test
    public void testReverseTableComplexPrimary() {
        HologresTransformer hologresTransformer = new HologresTransformer();
        List<Column> primaryColumns = getPrimaryColumns();
        Table table = Table.builder()
            .columns(primaryColumns)
            .name("abc")
            .build();
        primaryColumns.add(Column.builder().name("c3").dataType("text").nullable(true).build());
        Node node = hologresTransformer.reverseTable(table, ReverseContext.builder().build());
        CreateTable createTable = (CreateTable)node;
        assertEquals(createTable.getConstraintStatements().size(), 1);
        DialectNode transform = hologresTransformer.transform(createTable);
        assertEquals(transform.getNode(), "BEGIN;\n"
            + "CREATE TABLE IF NOT EXISTS abc (\n"
            + "   c1 BIGINT NOT NULL,\n"
            + "   c2 BIGINT NOT NULL,\n"
            + "   c3 TEXT,\n"
            + "   PRIMARY KEY(c1,c2)\n"
            + ");\n\n"
            + "COMMIT;");
    }

    @Test
    public void testTransformTable() {
        HologresTransformer hologresTransformer = new HologresTransformer();
        List<ColumnDefinition> columnDefines = Lists.newArrayList();
        columnDefines.add(
            ColumnDefinition.builder()
                .colName(new Identifier("c1"))
                .notNull(false)
                .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
                .build()
        );
        columnDefines.add(
            ColumnDefinition.builder()
                .colName(new Identifier("c2"))
                .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
                .build()
        );
        List<BaseConstraint> constraints = ImmutableList.of(
            new PrimaryConstraint(new Identifier("c1"),
                Lists.newArrayList(new Identifier("c1"), new Identifier("c2")))
        );
        CreateTable createTable = CreateTable.builder()
            .tableName(QualifiedName.of("a.b"))
            .columns(columnDefines)
            .constraints(constraints)
            .build();
        Table table = hologresTransformer.transformTable(createTable, TransformContext.builder().build());
        List<Column> columns = table.getColumns();
        assertEquals(columns.size(), 2);
        assertEquals(columns.get(0).isPrimaryKey(), true);
        assertEquals(columns.get(1).isPrimaryKey(), true);
        assertEquals(columns.get(0).isNullable(), false);
        assertEquals(columns.get(1).isNullable(), false);
    }

    @Test
    public void testVarcharTable() {
        List<Column> columns = new ArrayList<>();
        columns.add(
            Column.builder()
                .name("n")
                .dataType("varchar")
                .length(1)
                .build()
        );
        Table table = Table.builder()
            .name("t_name")
            .columns(columns)
            .build();
        Node node = hologresTransformer.reverseTable(table);
        String node1 = hologresTransformer.transform((BaseStatement)node).getNode();
        assertEquals(node1, "BEGIN;\n"
            + "CREATE TABLE IF NOT EXISTS t_name (\n"
            + "   n VARCHAR(1) NOT NULL\n"
            + ");\n\n"
            + "COMMIT;");
    }

    @Test
    public void testVarchar() {
        List<Column> columns = new ArrayList<>();
        columns.add(
            Column.builder()
                .name("n")
                .dataType("varchar")
                .build()
        );
        Table table = Table.builder()
            .name("t_name")
            .columns(columns)
            .build();
        Node node = hologresTransformer.reverseTable(table);
        String node1 = hologresTransformer.transform((BaseStatement)node).getNode();
        assertEquals(node1, "BEGIN;\n"
            + "CREATE TABLE IF NOT EXISTS t_name (\n"
            + "   n VARCHAR NOT NULL\n"
            + ");\n\n"
            + "COMMIT;");
    }

    @Test
    public void testPrimaryAndPartitionKey() {
        List<Column> columns = new ArrayList<>();
        columns.add(
            Column.builder()
                .name("id")
                .primaryKey(true)
                .partitionKey(true)
                .partitionKeyIndex(1)
                .dataType("varchar")
                .build()
        );
        columns.add(
            Column.builder()
                .name("c_name")
                .primaryKey(true)
                .dataType("bigint")
                .build()
        );

        columns.add(
            Column.builder()
                .name("age")
                .dataType("bigint")
                .build()
        );
        Table table = Table.builder()
            .name("key_name")
            .columns(columns)
            .build();
        Node node = hologresTransformer.reverseTable(table);
        String node1 = hologresTransformer.transform((BaseStatement)node).getNode();
        assertEquals(node1, "BEGIN;\n"
            + "CREATE TABLE IF NOT EXISTS key_name (\n"
            + "   id     VARCHAR NOT NULL,\n"
            + "   c_name BIGINT NOT NULL,\n"
            + "   age    BIGINT NOT NULL,\n"
            + "   PRIMARY KEY(id,c_name)\n"
            + ") PARTITION BY LIST(id);\n\n"
            + "COMMIT;");
    }

    @Test
    public void testReversTableWithConstraint() {
        List<Constraint> constraints = new ArrayList<>();
        Constraint constraint = new Constraint();
        constraint.setType(OutlineConstraintType.PRIMARY_KEY);
        constraint.setColumns(Lists.newArrayList("c1", "c2"));
        constraints.add(constraint);
        List<BaseClientProperty> properties = Lists.newArrayList();
        properties.add(
            hologresTransformer.create(DictEncodingColumn.DICTIONARY_ENCODING_COLUMN,
                "c1,c2")
        );
        properties.add(
            hologresTransformer.create(SegmentKey.SEGMENT_KEY,
                "c1")
        );
        List<Column> normalColumns = getNormalColumns();
        normalColumns.add(Column.builder()
            .name("c3")
            .dataType("text")
            .build());
        Table table = Table.builder()
            .name("t1")
            .schema("s1")
            .columns(normalColumns)
            .constraints(constraints)
            .properties(properties)
            .build();
        Node node = hologresTransformer.reverseTable(table);
        CreateTable createTable = (CreateTable)node;
        int size = createTable.getConstraintStatements().size();
        assertEquals(1, size);
        BaseConstraint baseConstraint = createTable.getConstraintStatements().get(0);
        PrimaryConstraint primaryConstraint = (PrimaryConstraint)baseConstraint;
        assertEquals(primaryConstraint.getColNames().get(0), new Identifier("c1"));
        List<Property> properties1 = createTable.getProperties();
        assertEquals(2, properties1.size());
        DialectNode transform = hologresTransformer.transform(createTable);
        assertEquals(transform.getNode(), "BEGIN;\n"
            + "CREATE TABLE IF NOT EXISTS t1 (\n"
            + "   c1 BIGINT NOT NULL,\n"
            + "   c2 BIGINT NOT NULL,\n"
            + "   c3 TEXT NOT NULL,\n"
            + "   PRIMARY KEY(c1,c2)\n"
            + ");\n"
            + "CALL SET_TABLE_PROPERTY('t1', 'dictionary_encoding_columns', '\"c1:auto,c2:auto\"');\n"
            + "CALL SET_TABLE_PROPERTY('t1', 'segment_key', '\"c1\"');\n"
            + "COMMIT;");
    }

    private List<Column> getNormalColumns() {
        Column c1 = Column.builder()
            .name("c1")
            .dataType("bigint")
            .build();
        Column c2 = Column.builder()
            .name("c2")
            .dataType("bigint")
            .build();
        return Lists.newArrayList(c1, c2);
    }

    private List<Column> getPrimaryColumns() {
        Column c1 = Column.builder()
            .name("c1")
            .dataType("bigint")
            .primaryKey(true)
            .build();
        Column c2 = Column.builder()
            .name("c2")
            .dataType("bigint")
            .primaryKey(true)
            .build();
        return Lists.newArrayList(c1, c2);
    }

    @Test
    public void testDouble() {
        List<Column> columns = ImmutableList.of(
            Column.builder()
                .dataType("Double PRECISION")
                .name("c1")
                .build()
        );
        Table t = Table.builder()
            .name("abc")
            .columns(columns)
            .build();
        Node node = hologresTransformer.reverseTable(t);
        assertNotNull(node);
        CreateTable c = (CreateTable)node;
        assertEquals(c.getColumnDefines().size(), 1);
    }

    @Test
    public void testBooleanDefaultValue() {
        DialectNode dialectNode = new DialectNode(
            "begin;\n" +
                "CREATE TABLE public.tbl_default (    \n" +
                "  smallint_col smallint DEFAULT 0,    \n" +
                "  int_col int NOT NULL DEFAULT 0,    \n" +
                "  bigint_col bigint not null DEFAULT 0,    \n" +
                "  boolean_col boolean DEFAULT FALSE,    \n" +
                "  float_col real DEFAULT 0.0,    \n" +
                "  double_col double precision DEFAULT 0.0,    \n" +
                "  decimal_col decimal(2, 1) DEFAULT 0.0,    \n" +
                "  text_col text DEFAULT 'N',    \n" +
                "  char_col char(2) DEFAULT 'N',    \n" +
                "  varchar_col varchar(200) DEFAULT 'N',    \n" +
                "  timestamptz_col timestamptz DEFAULT now(),    \n" +
                "  date_col date DEFAULT now(),    \n" +
                "  timestamp_col timestamp DEFAULT now()\n" +
                ");\n" +
                "COMMENT ON TABLE public.\"tbl_default\" is '用户属性表';\n" +
                "COMMENT ON COLUMN public.\"tbl_default\".int_col is '身份证号';\n" +
                "COMMENT ON COLUMN public.\"tbl_default\".text_col is '姓名';\n" +
                "commit;"
        );
        ReverseContext build = ReverseContext.builder().merge(true).build();
        Node node = hologresTransformer.reverse(dialectNode, build);
        Table table = hologresTransformer.transformTable(node, TransformContext.builder().build());
        assertEquals("false", table.getColumns().get(3).getDefaultValue());
        assertEquals("now()", table.getColumns().get(10).getDefaultValue());
    }

    @Test
    public void testDynamicPartition() {
        DialectNode dialectNode = new DialectNode(
            "CREATE TABLE tbl1 (\n" +
                "    c1 text NOT NULL,\n" +
                "    c2 text \n" +
                ")\n" +
                "PARTITION BY LIST (c2)\n" +
                "WITH (\n" +
                "   auto_partitioning_enable = 'true',\n" +
                "   auto_partitioning_time_unit = 'DAY',\n" +
                "   auto_partitioning_time_zone = 'Asia/Shanghai',\n" +
                "   auto_partitioning_num_precreate = '3',\n" +
                "   auto_partitioning_num_retention = '2'\n" +
                ");"
        );
        ReverseContext build = ReverseContext.builder().merge(true).build();
        Node node = hologresTransformer.reverse(dialectNode, build);
        Table table = hologresTransformer.transformTable(node, TransformContext.builder().build());

        List<BaseClientProperty> properties = table.getProperties();
        assertEquals(5, properties.size());
        assertEquals("true", properties.get(0).getValue());
        assertEquals("DAY", properties.get(1).getValue());
        assertEquals("Asia/Shanghai", properties.get(2).getValue());
        assertEquals("3", properties.get(3).getValue());
        assertEquals("2", properties.get(4).getValue());
    }

    @Test
    public void testForeignTable() {
        {
            DialectNode dialectNode = new DialectNode(
                "CREATE FOREIGN TABLE src_pt(\n" +
                    "  id text, \n" +
                    "  pt text) \n" +
                    "SERVER odps_server \n" +
                    "OPTIONS(project_name '<odps_project>', table_name '<odps_table>');"
            );
            ReverseContext build = ReverseContext.builder().merge(true).build();
            Node node = hologresTransformer.reverse(dialectNode, build);
            Table table = hologresTransformer.transformTable(node, TransformContext.builder().build());

            assertEquals(true, table.isExternal());
            assertNotNull(table.getProperties());
            List<BaseClientProperty> properties = table.getProperties();
            assertEquals(3, properties.size());
            assertEquals("odps_server", properties.get(0).getValue());
            assertEquals("<odps_project>", properties.get(1).getValue());
            assertEquals("<odps_table>", properties.get(2).getValue());
        }

        {
            DialectNode dialectNode = new DialectNode(
                "CREATE FOREIGN TABLE src_pt(\n" +
                    "  id text,\n" +
                    "  pt text)\n" +
                    "SERVER odps_server\n" +
                    "OPTIONS(project_name '<odps_project>#<odps_schema>', table_name '<odps_table>');"
            );
            ReverseContext build = ReverseContext.builder().merge(true).build();
            Node node = hologresTransformer.reverse(dialectNode, build);
            Table table = hologresTransformer.transformTable(node, TransformContext.builder().build());

            assertEquals(true, table.isExternal());
            assertNotNull(table.getProperties());
            List<BaseClientProperty> properties = table.getProperties();
            assertEquals(3, properties.size());
            assertEquals("odps_server", properties.get(0).getValue());
            assertEquals("<odps_project>#<odps_schema>", properties.get(1).getValue());
            assertEquals("<odps_table>", properties.get(2).getValue());
        }
    }

    @Test
    public void testColumn() {
        DialectNode dialectNode = new DialectNode(
            "begin;\n" +
                "CREATE TABLE public.tbl_default (    \n" +
                "  smallint_col smallint DEFAULT 0,    \n" +
                "  int_col int NOT NULL DEFAULT 0,    \n" +
                "  bigint_col bigint not null DEFAULT 0,    \n" +
                "  boolean_col boolean DEFAULT FALSE,    \n" +
                "  float_col real DEFAULT 0.0,    \n" +
                "  double_col double precision DEFAULT 0.0,    \n" +
                "  decimal_col decimal(2, 1) DEFAULT 0.0,    \n" +
                "  text_col text DEFAULT 'N',    \n" +
                "  char_col char(2) DEFAULT 'N',    \n" +
                "  varchar_col varchar(200) DEFAULT 'N',    \n" +
                "  timestamptz_col timestamptz DEFAULT now(),    \n" +
                "  date_col date DEFAULT now(),    \n" +
                "  timestamp_col timestamp DEFAULT now()\n" +
                ");\n" +
                "COMMENT ON TABLE public.\"tbl_default\" is '用户属性表';\n" +
                "COMMENT ON COLUMN public.\"tbl_default\".int_col is '身份证号';\n" +
                "COMMENT ON COLUMN public.\"tbl_default\".text_col is '姓名';\n" +
                "commit;"
        );
        ReverseContext build = ReverseContext.builder().merge(true).build();
        Node node = hologresTransformer.reverse(dialectNode, build);
        Table table = hologresTransformer.transformTable(node, TransformContext.builder().build());

        Node reverseNode = hologresTransformer.reverseTable(table);
        String node1 = hologresTransformer.transform((BaseStatement)reverseNode,
            TransformContext.builder().schema(((CreateTable)reverseNode).getQualifiedName().getFirst()).build()).getNode();
        assertEquals("BEGIN;\n" +
            "CREATE TABLE public.tbl_default (\n" +
            "   smallint_col    SMALLINT DEFAULT 0,\n" +
            "   int_col         INTEGER NOT NULL DEFAULT 0,\n" +
            "   bigint_col      BIGINT NOT NULL DEFAULT 0,\n" +
            "   boolean_col     BOOLEAN DEFAULT false,\n" +
            "   float_col       REAL DEFAULT 0.0,\n" +
            "   double_col      DOUBLE PRECISION DEFAULT 0.0,\n" +
            "   decimal_col     DECIMAL(2,1) DEFAULT 0.0,\n" +
            "   text_col        TEXT DEFAULT 'N',\n" +
            "   char_col        CHAR(2) DEFAULT 'N',\n" +
            "   varchar_col     VARCHAR(200) DEFAULT 'N',\n" +
            "   timestamptz_col TIMESTAMP WITH TIME ZONE DEFAULT now(),\n" +
            "   date_col        DATE DEFAULT now(),\n" +
            "   timestamp_col   TIMESTAMP DEFAULT now()\n" +
            ");\n" +
            "COMMENT ON TABLE public.tbl_default IS '用户属性表';\n" +
            "COMMENT ON COLUMN public.tbl_default.int_col IS '身份证号';\n" +
            "COMMENT ON COLUMN public.tbl_default.text_col IS '姓名';\n" +
            "COMMIT;", node1);
    }

    @Test
    public void testProperties() {
        DialectNode dialectNode = new DialectNode(
            "BEGIN;\n" +
                "\n" +
                "CREATE TABLE tbl\n" +
                "(\n" +
                "    id             BIGINT NOT NULL\n" +
                "    ,name          TEXT NOT NULL\n" +
                "    ,age           BIGINT NOT NULL\n" +
                "    ,class         TEXT NOT NULL\n" +
                "    ,reg_timestamp TIMESTAMP WITH TIME ZONE NOT NULL\n" +
                "    ,PRIMARY KEY (id,age)\n" +
                ");\n" +
                "call set_table_property('tbl', 'orientation', 'column');\n" +
                "call set_table_property('tbl', 'table_group', 'default');\n" +
                "call set_table_property('tbl', 'distribution_key', 'id');\n" +
                "call set_table_property('tbl', 'clustering_key', 'age');\n" +
                "call set_table_property('tbl', 'event_time_column', 'reg_timestamp');\n" +
                "call set_table_property('tbl', 'bitmap_columns', 'name,class');\n" +
                "call set_table_property('tbl', 'dictionary_encoding_columns', 'class:auto');\n" +
                "call set_table_property('tbl', 'time_to_live_in_seconds', '36');\n" +
                "commit;"
        );
        ReverseContext build = ReverseContext.builder().merge(true).build();
        Node node = hologresTransformer.reverse(dialectNode, build);
        Table table = hologresTransformer.transformTable(node, TransformContext.builder().build());

        Node reverseNode = hologresTransformer.reverseTable(table);
        String node1 = hologresTransformer.transform((BaseStatement)reverseNode, TransformContext.builder().build()).getNode();
        assertEquals("BEGIN;\n" +
            "CREATE TABLE tbl (\n" +
            "   id            BIGINT NOT NULL,\n" +
            "   \"name\"        TEXT NOT NULL,\n" +
            "   age           BIGINT NOT NULL,\n" +
            "   \"class\"       TEXT NOT NULL,\n" +
            "   reg_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,\n" +
            "   PRIMARY KEY(id,age)\n" +
            ");\n" +
            "CALL SET_TABLE_PROPERTY('tbl', 'time_to_live_in_seconds', '36');\n" +
            "CALL SET_TABLE_PROPERTY('tbl', 'orientation', 'column');\n" +
            "CALL SET_TABLE_PROPERTY('tbl', 'table_group', 'default');\n" +
            "CALL SET_TABLE_PROPERTY('tbl', 'distribution_key', '\"id\"');\n" +
            "CALL SET_TABLE_PROPERTY('tbl', 'clustering_key', '\"age\"');\n" +
            "CALL SET_TABLE_PROPERTY('tbl', 'event_time_column', 'reg_timestamp');\n" +
            "CALL SET_TABLE_PROPERTY('tbl', 'bitmap_columns', '\"name,class\"');\n" +
            "CALL SET_TABLE_PROPERTY('tbl', 'dictionary_encoding_columns', '\"class:auto\"');\n" +
            "COMMIT;", node1);
    }

    @Test
    public void testDynamicPartition2() {
        DialectNode dialectNode = new DialectNode(
            "BEGIN;\n" +
                "\n" +
                "CREATE TABLE tbl1\n" +
                "(\n" +
                "    c1  TEXT NOT NULL\n" +
                "    ,c2 TEXT\n" +
                ")\n" +
                "PARTITION BY LIST \n" +
                "(\n" +
                "    c2  \n" +
                ");\n" +
                "CALL set_table_property ('tbl1', 'auto_partitioning.enable', 'true');\n" +
                "CALL set_table_property ('tbl1', 'auto_partitioning.time_unit', 'DAY');\n" +
                "CALL set_table_property ('tbl1', 'auto_partitioning.time_zone', 'Asia/Shanghai');\n" +
                "CALL set_table_property ('tbl1', 'auto_partitioning.num_precreate', '3');\n" +
                "CALL set_table_property ('tbl1', 'auto_partitioning.num_retention', '2');\n" +
                "COMMIT;"
        );
        ReverseContext build = ReverseContext.builder().merge(true).build();
        Node node = hologresTransformer.reverse(dialectNode, build);
        Table table = hologresTransformer.transformTable(node, TransformContext.builder().build());

        Node reverseNode = hologresTransformer.reverseTable(table);
        String node1 = hologresTransformer.transform((BaseStatement)reverseNode, TransformContext.builder().build()).getNode();
        assertEquals("BEGIN;\n" +
            "CREATE TABLE tbl1 (\n" +
            "   c1 TEXT NOT NULL,\n" +
            "   c2 TEXT\n" +
            ") PARTITION BY LIST(c2);\n" +
            "CALL SET_TABLE_PROPERTY('tbl1', 'auto_partitioning.enable', 'true');\n" +
            "CALL SET_TABLE_PROPERTY('tbl1', 'auto_partitioning.time_unit', 'DAY');\n" +
            "CALL SET_TABLE_PROPERTY('tbl1', 'auto_partitioning.time_zone', 'Asia/Shanghai');\n" +
            "CALL SET_TABLE_PROPERTY('tbl1', 'auto_partitioning.num_precreate', '3');\n" +
            "CALL SET_TABLE_PROPERTY('tbl1', 'auto_partitioning.num_retention', '2');\n" +
            "COMMIT;", node1);
    }

    @Test
    public void testForeignTable2() {
        DialectNode dialectNode = new DialectNode(
            "CREATE FOREIGN TABLE src_pt(\n" +
                "  id text, \n" +
                "  pt text) \n" +
                "SERVER odps_server \n" +
                "OPTIONS(project_name '<odps_project>', table_name '<odps_table>');"
        );
        ReverseContext build = ReverseContext.builder().merge(true).build();
        Node node = hologresTransformer.reverse(dialectNode, build);
        Table table = hologresTransformer.transformTable(node, TransformContext.builder().build());

        Node reverseNode = hologresTransformer.reverseTable(table);
        String node1 = hologresTransformer.transform((BaseStatement)reverseNode, TransformContext.builder().build()).getNode();
        assertEquals("BEGIN;\n" +
            "CREATE FOREIGN TABLE src_pt (\n" +
            "   id TEXT,\n" +
            "   pt TEXT\n" +
            ")\n" +
            "SERVER odps_server\n" +
            "OPTIONS(project_name '<odps_project>', table_name '<odps_table>');\n" +
            "COMMIT;", node1);
    }

    @Test(expected = MergeException.class)
    @SneakyThrows
    public void testParseReverse() {
        String value = IOUtils.resourceToString("/hologres/npe.txt", Charset.defaultCharset());
        CompositeStatement createTable = (CompositeStatement)hologresTransformer.reverse(new DialectNode(value),
            ReverseContext.builder().merge(true).build());
        assertEquals(7, createTable.getChildren().size());
    }
}

