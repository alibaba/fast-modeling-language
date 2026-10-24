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

package com.aliyun.fastmodel.transform.mc.format;

import java.util.Arrays;
import java.util.List;

import com.aliyun.fastmodel.core.tree.AliasedName;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.Row;
import com.aliyun.fastmodel.core.tree.expr.literal.LongLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.core.tree.statement.element.MultiComment;
import com.aliyun.fastmodel.core.tree.statement.insert.Insert;
import com.aliyun.fastmodel.core.tree.statement.table.AddCols;
import com.aliyun.fastmodel.core.tree.statement.table.ChangeCol;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateDimTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.DropCol;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.core.tree.statement.table.RenameCol;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableProperties;
import com.aliyun.fastmodel.core.tree.statement.table.UnSetTableProperties;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.PrimaryConstraint;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.aliyun.fastmodel.core.tree.util.IdentifierUtil;
import com.aliyun.fastmodel.core.tree.util.QueryUtil;
import com.aliyun.fastmodel.transform.mc.context.MaxComputeContext;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/2/7
 */
public class MaxComputeVisitorTest {

    private final MaxComputeVisitor maxComputeVisitor = new MaxComputeVisitor(MaxComputeContext.builder().build());

    @Test
    public void testVisitCreateTable() {
        CreateDimTable build = CreateDimTable.builder()
            .tableName(QualifiedName.of("a.b"))
            .columns(ImmutableList.of(
                ColumnDefinition.builder()
                    .colName(new Identifier("c1"))
                    .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
                    .build()
            ))
            .comment(new Comment("comment"))
            .build();
        maxComputeVisitor.visitCreateTable(build, 0);
        String string = maxComputeVisitor.getBuilder().toString();
        assertEquals("CREATE TABLE b\n"
            + "(\n"
            + "   c1 BIGINT\n"
            + ")\n"
            + "COMMENT 'comment'", string);
    }

    @Test
    public void testVisitCreateTableWithoutComment() {
        CreateDimTable build = CreateDimTable.builder()
            .tableName(QualifiedName.of("a.b"))
            .columns(ImmutableList.of(
                ColumnDefinition.builder()
                    .colName(new Identifier("c1"))
                    .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
                    .build()
            ))
            .build();
        maxComputeVisitor.visitCreateTable(build, 0);
        String string = maxComputeVisitor.getBuilder().toString();
        assertEquals("CREATE TABLE b\n"
                + "(\n"
                + "   c1 BIGINT\n"
                + ")\n"
            , string);
    }

    @Test
    public void testVisitCreateTableWithPartitionBy() {
        CreateDimTable build = CreateDimTable.builder()
            .tableName(QualifiedName.of("a.b"))
            .comment(new Comment("comment"))
            .columns(ImmutableList.of(
                ColumnDefinition.builder()
                    .colName(new Identifier("c2"))
                    .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
                    .aliasedName(new AliasedName("abc"))
                    .build()
            ))
            .partition(new PartitionedBy(ImmutableList.of(
                ColumnDefinition.builder()
                    .colName(new Identifier("c1"))
                    .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
                    .build()
            )))
            .build();
        maxComputeVisitor.visitCreateTable(build, 0);
        String string = maxComputeVisitor.getBuilder().toString();
        assertEquals("CREATE TABLE b\n"
                + "(\n"
                + "   c2 BIGINT\n"
                + ")\n"
                + "COMMENT 'comment'\n"
                + "PARTITIONED BY\n"
                + "(\n"
                + "   c1 BIGINT\n"
                + ")\n"
            , string);
    }

    @Test
    public void testInsert() {
        Row row = new Row(ImmutableList.of(new StringLiteral("abc"), new LongLiteral("1")));
        Insert insert = new Insert(false, QualifiedName.of("a.b"), null, QueryUtil
            .query(QueryUtil.values(row, row)),
            ImmutableList.of(new Identifier("col1"), new Identifier("col2")));
        maxComputeVisitor.visitInsert(insert, 0);
        String visitor = maxComputeVisitor.getBuilder().toString();
        assertEquals(visitor, "INSERT INTO TABLE b (col1,col2) \n"
            + "  VALUES \n"
            + "  ('abc', 1)\n"
            + ", ('abc', 1)\n");
    }

    @Test
    public void testInsertOverwrite() {
        Row row = new Row(ImmutableList.of(new StringLiteral("abc"), new LongLiteral("1")));
        Insert insert = new Insert(true, QualifiedName.of("a.b"), null, QueryUtil
            .query(QueryUtil.values(row, row)),
            ImmutableList.of(new Identifier("col1"), new Identifier("col2")));
        maxComputeVisitor.visitInsert(insert, 0);
        String visitor = maxComputeVisitor.getBuilder().toString();
        assertEquals(visitor, "INSERT OVERWRITE TABLE b VALUES \n"
            + "  ('abc', 1)\n"
            + ", ('abc', 1)\n");
    }

    @Test
    public void testVisitChangeCol() {
        ChangeCol changeColumn = new ChangeCol(
            QualifiedName.of("dim_shop"),
            new Identifier("c1"),
            ColumnDefinition.builder()
                .colName(new Identifier("c1"))
                .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
                .build()
        );
        Boolean aBoolean = maxComputeVisitor.visitChangeCol(changeColumn, 0);
        assertTrue(aBoolean);
        assertEquals(maxComputeVisitor.getBuilder().toString(), "ALTER TABLE dim_shop CHANGE COLUMN c1 c1 BIGINT");
    }

    @Test
    public void testVisitChangeColComment() {
        ChangeCol changeColumn = new ChangeCol(
            QualifiedName.of("dim_shop"),
            new Identifier("c1"),
            ColumnDefinition.builder()
                .colName(new Identifier("c1"))
                .comment(new Comment("comment"))
                .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
                .build()
        );
        Boolean aBoolean = maxComputeVisitor.visitChangeCol(changeColumn, 0);
        assertTrue(aBoolean);
        assertEquals(maxComputeVisitor.getBuilder().toString(), "ALTER TABLE dim_shop CHANGE COLUMN c1 c1 BIGINT COMMENT 'comment'");
    }

    @Test
    public void testVisitCreateTableWithAlias() {
        CreateDimTable build = CreateDimTable.builder()
            .tableName(QualifiedName.of("a.b"))
            .columns(ImmutableList.of(
                ColumnDefinition.builder()
                    .colName(new Identifier("c1"))
                    .aliasedName(new AliasedName("alias"))
                    .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
                    .build()
            ))
            .comment(new Comment("comment"))
            .build();
        maxComputeVisitor.visitCreateTable(build, 0);
        String string = maxComputeVisitor.getBuilder().toString();
        assertEquals("CREATE TABLE b\n"
            + "(\n"
            + "   c1 BIGINT\n"
            + ")\n"
            + "COMMENT 'comment'", string);
    }

    @Test
    public void testVisitChangeColChangeName() {
        ChangeCol changeColumn = new ChangeCol(
            QualifiedName.of("dim_shop"),
            new Identifier("c1"),
            ColumnDefinition.builder()
                .colName(new Identifier("c2"))
                .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
                .build()
        );
        Boolean aBoolean = maxComputeVisitor.visitChangeCol(changeColumn, 0);
        assertTrue(aBoolean);
        assertEquals(maxComputeVisitor.getBuilder().toString(), "ALTER TABLE dim_shop CHANGE COLUMN c1 c2 BIGINT");
    }

    @Test
    public void testVisitChangeColChangeComment() {
        ChangeCol changeColumn = new ChangeCol(
            QualifiedName.of("dim_shop"),
            new Identifier("c1"),
            ColumnDefinition.builder()
                .colName(new Identifier("c1"))
                .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
                .comment(new Comment("abc"))
                .build()
        );
        Boolean aBoolean = maxComputeVisitor.visitChangeCol(changeColumn, 0);
        assertTrue(aBoolean);
        assertEquals(maxComputeVisitor.getBuilder().toString(),
            "ALTER TABLE dim_shop CHANGE COLUMN c1 c1 BIGINT COMMENT 'abc'");
    }

    @Test
    public void testVisitCreateTableWithMulitComment() {
        CreateTable createTable = CreateTable.builder()
            .tableName(QualifiedName.of("abc"))
            .columnComments(Arrays.asList(
                new MultiComment(
                    ColumnDefinition.builder()
                        .colName(new Identifier("c1"))
                        .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
                        .comment(new Comment("comment"))
                        .aliasedName(new AliasedName("alias"))
                        .build()
                )
            ))
            .build();
        maxComputeVisitor.visitCreateTable(createTable, 0);
        assertEquals(maxComputeVisitor.getBuilder().toString(), "CREATE TABLE abc\n"
            + "--(\n"
            + "--   c1 BIGINT COMMENT 'comment'\n"
            + "--)\n");
    }

    @Test
    public void testVisitCode() {
        CreateDimTable createDimTable = CreateDimTable
            .builder()
            .tableName(QualifiedName.of("a.b"))
            .build();
        MaxComputeVisitor maxComputeVisitor = new MaxComputeVisitor(MaxComputeContext.builder().mcProject("autotest").mcSchema("default").build());
        maxComputeVisitor.visitCreateTable(createDimTable, 0);
        String s = maxComputeVisitor.getBuilder().toString();
        assertEquals(s, "CREATE TABLE autotest.default.b");
    }

    @Test
    public void testVisitAlter() {
        AddCols createDimTable = new AddCols(
            QualifiedName.of("b"),
            ImmutableList.of(
                ColumnDefinition.builder().dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT)).colName(new Identifier("b")).build()
            )
        );
        MaxComputeVisitor maxComputeVisitor = new MaxComputeVisitor(MaxComputeContext.builder().mcProject("autotest").mcSchema("default").build());
        maxComputeVisitor.visitAddCols(createDimTable, 0);
        String s = maxComputeVisitor.getBuilder().toString();
        assertEquals(s, "ALTER TABLE autotest.default.b ADD COLUMNS\n"
            + "(\n"
            + "   b BIGINT\n"
            + ")");
    }

    @Test
    public void testVisitChange() {
        ChangeCol createDimTable = new ChangeCol(
            QualifiedName.of("b"),
            new Identifier("c1"),
            ColumnDefinition.builder().dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT)).colName(new Identifier("b")).build()
        );
        MaxComputeVisitor maxComputeVisitor = new MaxComputeVisitor(MaxComputeContext.builder().mcProject("autotest").mcSchema("default").build());
        maxComputeVisitor.visitChangeCol(createDimTable, 0);
        String s = maxComputeVisitor.getBuilder().toString();
        assertEquals(s, "ALTER TABLE autotest.default.b CHANGE COLUMN c1 b BIGINT");
    }

    @Test
    public void testDropColumn() {
        DropCol createDimTable = new DropCol(
            QualifiedName.of("b"),
            new Identifier("c1")
        );
        MaxComputeVisitor maxComputeVisitor = new MaxComputeVisitor(MaxComputeContext.builder().mcProject("autotest").mcSchema("default").build());
        maxComputeVisitor.visitDropCol(createDimTable, 0);
        String s = maxComputeVisitor.getBuilder().toString();
        assertEquals(s, "ALTER TABLE autotest.default.b DROP COLUMN c1");
    }

    @Test
    public void testDropColumnWithoutSchema() {
        DropCol createDimTable = new DropCol(
            QualifiedName.of("b"),
            new Identifier("c1")
        );
        MaxComputeVisitor maxComputeVisitor = new MaxComputeVisitor(MaxComputeContext.builder().mcProject("autotest").build());
        maxComputeVisitor.visitDropCol(createDimTable, 0);
        String s = maxComputeVisitor.getBuilder().toString();
        assertEquals(s, "ALTER TABLE autotest.b DROP COLUMN c1");
    }

    @Test
    public void testDropColumnWithoutProjectSchema() {
        DropCol createDimTable = new DropCol(
            QualifiedName.of("b"),
            new Identifier("c1")
        );
        MaxComputeVisitor maxComputeVisitor = new MaxComputeVisitor(MaxComputeContext.builder().build());
        maxComputeVisitor.visitDropCol(createDimTable, 0);
        String s = maxComputeVisitor.getBuilder().toString();
        assertEquals(s, "ALTER TABLE b DROP COLUMN c1");
    }

    @Test
    public void testUnSetProperties() {
        MaxComputeVisitor maxComputeVisitor1 = new MaxComputeVisitor(MaxComputeContext.builder().build());
        UnSetTableProperties unSetTableProperties = new UnSetTableProperties(QualifiedName.of("abc"),
            Lists.newArrayList(MaxComputePropertyKey.LIFE_CYCLE.getValue()));
        Boolean aBoolean = maxComputeVisitor1.visitUnSetTableProperties(unSetTableProperties, 0);
        String s = maxComputeVisitor1.getBuilder().toString();
        assertEquals(s, "ALTER TABLE abc DISABLE LIFECYCLE");

        maxComputeVisitor1 = new MaxComputeVisitor(MaxComputeContext.builder().build());
        unSetTableProperties = new UnSetTableProperties(QualifiedName.of("abc"), Lists.newArrayList("not_exist"));
        maxComputeVisitor1.visitUnSetTableProperties(unSetTableProperties, 0);
        s = maxComputeVisitor1.getBuilder().toString();
        assertEquals(s, "ALTER TABLE abc UNSET PROPERTIES('not_exist')");
    }

    @Test
    public void testSetProperties() {
        MaxComputeVisitor maxComputeVisitor1 = new MaxComputeVisitor(MaxComputeContext.builder().build());
        SetTableProperties setTableProperties =
            new SetTableProperties(QualifiedName.of("abc"), Lists.newArrayList(new Property(MaxComputePropertyKey.LIFE_CYCLE.getValue(), "10")));
        maxComputeVisitor1.visitSetTableProperties(setTableProperties, 0);
        String s = maxComputeVisitor1.getBuilder().toString();
        assertEquals(s, "ALTER TABLE abc ENABLE LIFECYCLE;\n"
            + "ALTER TABLE abc SET LIFECYCLE 10");

    }

    @Test
    public void testTransactional() {
        MaxComputeVisitor maxComputeVisitor = new MaxComputeVisitor(MaxComputeContext.builder().build());
        List<Property> properties = Lists.newArrayList();
        properties.add(new Property(MaxComputePropertyKey.TRANSACTIONAL.getValue(), "true"));
        List<ColumnDefinition> columns = Lists.newArrayList(
            ColumnDefinition.builder()
                .colName(new Identifier("c1"))
                .notNull(true)
                .dataType(DataTypeUtil.simpleType("bigint", null))
                .build()
        );
        List<BaseConstraint> constraints = Lists.newArrayList(
            new PrimaryConstraint(IdentifierUtil.sysIdentifier(), Lists.newArrayList(new Identifier("c1")))
        );

        CreateTable createTable = CreateTable.builder()
            .tableName(QualifiedName.of("abc"))
            .columns(columns)
            .properties(properties)
            .constraints(constraints)
            .build();
        maxComputeVisitor.visitCreateTable(createTable, 0);
        String value = maxComputeVisitor.getBuilder().toString();
        assertEquals(value, "CREATE TABLE abc\n"
            + "(\n"
            + "   c1 BIGINT NOT NULL,\n"
            + "   PRIMARY KEY(c1)\n"
            + ")\n"
            + "TBLPROPERTIES ('transactional'='true')");
    }

    @Test
    public void testAppendMode() {
        MaxComputeVisitor maxComputeVisitor = new MaxComputeVisitor(MaxComputeContext.builder().build());
        List<Property> properties = Lists.newArrayList();
        properties.add(new Property(MaxComputePropertyKey.APPEND_MODE.getValue(), "true"));
        List<ColumnDefinition> columns = Lists.newArrayList(
            ColumnDefinition.builder()
                .colName(new Identifier("c1"))
                .dataType(DataTypeUtil.simpleType("bigint", null))
                .build()
        );

        CreateTable createTable = CreateTable.builder()
            .tableName(QualifiedName.of("abc"))
            .columns(columns)
            .properties(properties)
            .build();
        maxComputeVisitor.visitCreateTable(createTable, 0);
        String value = maxComputeVisitor.getBuilder().toString();
        assertEquals(value, "CREATE TABLE abc\n"
            + "(\n"
            + "   c1 BIGINT\n"
            + ")\n"
            + "TBLPROPERTIES ('append_mode'='true')");
    }

    @Test
    public void testWithoutTransactional() {
        MaxComputeVisitor maxComputeVisitor = new MaxComputeVisitor(MaxComputeContext.builder().build());
        List<Property> properties = Lists.newArrayList();
        List<ColumnDefinition> columns = Lists.newArrayList(
            ColumnDefinition.builder()
                .colName(new Identifier("c1"))
                .notNull(true)
                .dataType(DataTypeUtil.simpleType("bigint", null))
                .build()
        );
        List<BaseConstraint> constraints = Lists.newArrayList(
            new PrimaryConstraint(IdentifierUtil.sysIdentifier(), Lists.newArrayList(new Identifier("c1")))
        );

        CreateTable createTable = CreateTable.builder()
            .tableName(QualifiedName.of("abc"))
            .columns(columns)
            .properties(properties)
            .constraints(constraints)
            .build();
        maxComputeVisitor.visitCreateTable(createTable, 0);
        String value = maxComputeVisitor.getBuilder().toString();
        assertEquals(value, "CREATE TABLE abc\n" +
            "(\n" +
            "   c1 BIGINT NOT NULL,\n" +
            "   PRIMARY KEY(c1)\n" +
            ")\n");
    }

    @Test
    public void testVisitKeep() {
        MaxComputeVisitor maxComputeVisitor = new MaxComputeVisitor(MaxComputeContext.builder().prettyFormat(false).keepConstraint(true).build());
        List<BaseConstraint> constraints = Lists.newArrayList(
            new PrimaryConstraint(IdentifierUtil.sysIdentifier(), Lists.newArrayList(new Identifier("c1")))
        );
        List<ColumnDefinition> columns = Lists.newArrayList(
            ColumnDefinition.builder()
                .colName(new Identifier("c1"))
                .notNull(true)
                .dataType(DataTypeUtil.simpleType("bigint", null))
                .build(),
            ColumnDefinition.builder()
                .colName(new Identifier("c122222234234234234"))
                .notNull(true)
                .dataType(DataTypeUtil.simpleType("bigint", null))
                .build()
        );
        CreateTable createTable = CreateTable.builder()
            .tableName(QualifiedName.of("abc"))
            .columns(columns)
            .constraints(constraints)
            .build();

        maxComputeVisitor.visitCreateTable(createTable, 0);
        String value = maxComputeVisitor.getBuilder().toString();
        assertEquals(value, "CREATE TABLE abc\n"
            + "(\n"
            + "c1 BIGINT NOT NULL,\n"
            + "c122222234234234234 BIGINT NOT NULL,\n"
            + "PRIMARY KEY(c1)\n"
            + ")\n");
    }

    @Test
    public void testRenameColumn() {
        RenameCol renameCol = new RenameCol(QualifiedName.of("abc"), new Identifier("c1"), new Identifier("c2"));
        MaxComputeVisitor maxComputeVisitor1 = new MaxComputeVisitor(MaxComputeContext.builder()
            .mcSchema("schema1")
            .mcProject("project1")
            .build());
        maxComputeVisitor1.visitRenameCol(renameCol, 0);
        StringBuilder builder = maxComputeVisitor1.getBuilder();
        assertEquals("ALTER TABLE project1.schema1.abc CHANGE COLUMN c1 RENAME TO c2", builder.toString());
    }

    @Test
    public void testAppendTable() {
        MaxComputeVisitor maxComputeVisitor = new MaxComputeVisitor(MaxComputeContext.builder().build());
        List<Property> properties = Lists.newArrayList();
        properties.add(new Property(MaxComputePropertyKey.APPEND_MODE.getValue(), "true"));
        List<ColumnDefinition> columns = Lists.newArrayList(
            ColumnDefinition.builder()
                .colName(new Identifier("c1"))
                .dataType(DataTypeUtil.simpleType("bigint", null))
                .build()
        );

        CreateTable createTable = CreateTable.builder()
            .tableName(QualifiedName.of("abc"))
            .columns(columns)
            .properties(properties)
            .build();
        maxComputeVisitor.visitCreateTable(createTable, 0);
        String value = maxComputeVisitor.getBuilder().toString();
        assertEquals(value, "CREATE TABLE abc\n" +
            "(\n" +
            "   c1 BIGINT\n" +
            ")\n" +
            "TBLPROPERTIES ('append_mode'='true')");
    }
}