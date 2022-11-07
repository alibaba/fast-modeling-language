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
import com.aliyun.fastmodel.core.tree.statement.table.AddCols;
import com.aliyun.fastmodel.core.tree.statement.table.ChangeCol;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateDimTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableComment;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.PrimaryConstraint;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
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
        assertEquals(transform.getNode(), "ALTER TABLE abc RENAME COLUMN bcd TO bde;");
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
        assertEquals(transform.getNode(), "ALTER TABLE abc ALTER COLUMN bcd SET DEFAULT 'bcd';");
        assertTrue(transform.isExecutable());
    }

    @Test
    public void testChangeColumndefaultNumber() {
        ChangeCol changeCol = new ChangeCol(QualifiedName.of("abc"), new Identifier("bcd"),
            ColumnDefinition.builder()
                .colName(new Identifier("bcd"))
                .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
                .defaultValue(new LongLiteral("1"))
                .build()
        );
        DialectNode transform = hologresTransformer.transform(changeCol, HologresTransformContext.builder().build());
        assertEquals(transform.getNode(), "ALTER TABLE abc ALTER COLUMN bcd SET DEFAULT 1;");
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
            + ");\n"
            + "CALL SET_TABLE_PROPERTY('b', 'orientation', 'column');\n"
            + "CALL SET_TABLE_PROPERTY('b', 'time_to_live_in_seconds', '3153600000');\nCOMMIT;", transform.getNode());
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
            + ");\n"
            + "CALL SET_TABLE_PROPERTY('abc', 'orientation', 'column');\n"
            + "CALL SET_TABLE_PROPERTY('abc', 'time_to_live_in_seconds', '3153600000');\n"
            + "COMMENT ON COLUMN abc.c1 IS NULL;\n"
            + "COMMENT ON COLUMN abc.c2 IS NULL;\n"
            + "COMMENT ON COLUMN abc.c3 IS NULL;\n"
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
            .name("name")
            .columns(columns)
            .build();
        Node node = hologresTransformer.reverseTable(table);
        String node1 = hologresTransformer.transform((BaseStatement)node).getNode();
        assertEquals(node1, "BEGIN;\n"
            + "CREATE TABLE IF NOT EXISTS name (\n"
            + "   n VARCHAR(1) NOT NULL\n"
            + ");\n"
            + "CALL SET_TABLE_PROPERTY('name', 'orientation', 'column');\n"
            + "CALL SET_TABLE_PROPERTY('name', 'time_to_live_in_seconds', '3153600000');\n"
            + "COMMENT ON COLUMN name.n IS NULL;\n"
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
            .name("name")
            .columns(columns)
            .build();
        Node node = hologresTransformer.reverseTable(table);
        String node1 = hologresTransformer.transform((BaseStatement)node).getNode();
        assertEquals(node1, "BEGIN;\n"
            + "CREATE TABLE IF NOT EXISTS name (\n"
            + "   n VARCHAR NOT NULL\n"
            + ");\n"
            + "CALL SET_TABLE_PROPERTY('name', 'orientation', 'column');\n"
            + "CALL SET_TABLE_PROPERTY('name', 'time_to_live_in_seconds', '3153600000');\n"
            + "COMMENT ON COLUMN name.n IS NULL;\n"
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
                .name("name")
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
            .name("name")
            .columns(columns)
            .build();
        Node node = hologresTransformer.reverseTable(table);
        String node1 = hologresTransformer.transform((BaseStatement)node).getNode();
        assertEquals(node1, "BEGIN;\n"
            + "CREATE TABLE IF NOT EXISTS name (\n"
            + "   id   VARCHAR NOT NULL,\n"
            + "   name BIGINT NOT NULL,\n"
            + "   age  BIGINT NOT NULL,\n"
            + "   PRIMARY KEY(id,name)\n"
            + ") PARTITION BY LIST(id);\n"
            + "CALL SET_TABLE_PROPERTY('name', 'orientation', 'column');\n"
            + "CALL SET_TABLE_PROPERTY('name', 'time_to_live_in_seconds', '3153600000');\n"
            + "COMMENT ON COLUMN name.id IS NULL;\n"
            + "COMMENT ON COLUMN name.name IS NULL;\n"
            + "COMMENT ON COLUMN name.age IS NULL;\n"
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
            + "CALL SET_TABLE_PROPERTY('t1', 'dictionary_encoding_columns', 'c1:auto,c2:auto');\n"
            + "CALL SET_TABLE_PROPERTY('t1', 'segment_key', 'c1');\n"
            + "COMMENT ON COLUMN t1.c1 IS NULL;\n"
            + "COMMENT ON COLUMN t1.c2 IS NULL;\n"
            + "COMMENT ON COLUMN t1.c3 IS NULL;\n"
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

}