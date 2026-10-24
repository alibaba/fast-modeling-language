/*
 * Copyright 2020 Alibaba Group Holding Ltd.
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.aliyun.fastmodel.core.tree.BaseRelation;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.relation.querybody.BaseQueryBody;
import com.aliyun.fastmodel.core.tree.relation.querybody.QuerySpecification;
import com.aliyun.fastmodel.core.tree.relation.querybody.Table;
import com.aliyun.fastmodel.core.tree.relation.querybody.Union;
import com.aliyun.fastmodel.core.tree.statement.select.Query;
import com.aliyun.fastmodel.core.tree.statement.select.RowToCol;
import com.aliyun.fastmodel.core.tree.statement.select.Select;
import com.aliyun.fastmodel.core.tree.statement.select.item.SelectItem;
import com.aliyun.fastmodel.core.tree.statement.select.item.SingleColumn;
import com.aliyun.fastmodel.core.tree.statement.table.AddCols;
import com.aliyun.fastmodel.core.tree.statement.table.AddConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.ChangeCol;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateDimTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.DropConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.DropTable;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.core.tree.statement.table.RenameTable;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableComment;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableProperties;
import com.aliyun.fastmodel.core.tree.statement.table.UnSetTableProperties;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.PrimaryConstraint;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.aliyun.fastmodel.core.tree.util.QueryUtil;
import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.aliyun.fastmodel.transform.api.context.setting.ViewSetting;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.mc.context.MaxComputeContext;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2020/10/16
 */
public class McTransformerTest {
    MaxComputeTransformer mcTransformer = new MaxComputeTransformer();

    @Test
    public void testTransform() throws Exception {
        ColumnDefinition columnDefine = ColumnDefinition.builder().colName(new Identifier("col1")).dataType(
            DataTypeUtil.simpleType(DataTypeEnums.DOUBLE)).build();
        List<ColumnDefinition> ts = Collections.singletonList(columnDefine);
        Comment comment = new Comment("comment");
        List<ColumnDefinition> list = new ArrayList<>();
        ColumnDefinition typeStatement = ColumnDefinition.builder().colName(new Identifier("col2")).dataType(
            DataTypeUtil.simpleType(DataTypeEnums.DOUBLE)).build();
        list.add(typeStatement);
        CreateTable createTableStatement = CreateDimTable.builder().tableName(QualifiedName.of("tableName")).columns(
            ts
        ).comment(comment).partition(new PartitionedBy(list)).build();
        TransformContext context = MaxComputeContext.builder().build();
        DialectNode result = mcTransformer.transform(createTableStatement, context);
        assertNotNull(result);
        assertTrue(result.getNode().contains("CREATE"));
        assertTrue(result.getNode().contains("PARTITIONED"));
    }

    @Test
    public void testTransformFromUnion() {
        List<SelectItem> list = new ArrayList<>();
        SingleColumn singleColumn = new SingleColumn(new Identifier("pk1"), new Identifier("p1"));
        list.add(singleColumn);
        Select select = QueryUtil.selectAll(list);
        BaseQueryBody selectStatement1 = QuerySpecification.builder().select(select).from(getFromTableStatement())
            .build();
        Union union = new Union(ImmutableList.of(selectStatement1), false);
        Query statement = QueryUtil.query(union);
        DialectNode transform = mcTransformer.transform(statement);
        assertNotNull(transform.getNode());

    }

    @Test
    public void testSelectRowToCol() {
        RowToCol rowToColStatement = null;
        try {
            rowToColStatement = new RowToCol(QueryUtil.simpleQuery(QueryUtil.selectList(new Identifier("a"))));
            DialectNode transform = mcTransformer.transform(rowToColStatement, MaxComputeContext.builder().build());
            assertNotNull(transform);
        } catch (Exception e) {
            Assert.assertNotNull(e);
        }
    }

    private BaseRelation getFromTableStatement() {
        return new Table(QualifiedName.of("t1"));
    }

    @Test
    public void testTransformSelectStatement() {
        Query statement = getSelectStatement();
        DialectNode transform = mcTransformer.transform(statement, MaxComputeContext.builder().build());
        assertNotNull(transform);
    }

    private Query getSelectStatement() {
        return QueryUtil.simpleQuery(getSelectColStatement());
    }

    private Select getSelectColStatement() {
        return new Select(getAliasStatement(), false);
    }

    private List<SelectItem> getAliasStatement() {
        List<SelectItem> list = new ArrayList<>();
        SelectItem statement = new SingleColumn(new Identifier("t1"), new Identifier("a1"));
        list.add(statement);
        return list;
    }

    @Test
    public void testSetComment() {
        SetTableComment setTableComment = new SetTableComment(
            QualifiedName.of("a.b"),
            new Comment("comment")
        );
        DialectNode transform = mcTransformer.transform(setTableComment);
        String node = transform.getNode();
        assertEquals("ALTER TABLE b SET COMMENT 'comment'", node);
    }

    @Test
    public void testRename() {
        RenameTable renameTable = new RenameTable(
            QualifiedName.of("a.b"),
            QualifiedName.of("c.d")
        );
        DialectNode dialectNode = mcTransformer.transform(renameTable);
        assertEquals(
            "ALTER TABLE b RENAME TO d",
            dialectNode.getNode()
        );
    }

    @Test
    public void testAddCols() {
        List<ColumnDefinition> columnLists =
            ImmutableList.of(ColumnDefinition.builder().colName(new Identifier("col1")).dataType(
                DataTypeUtil.simpleType(DataTypeEnums.BIGINT)).build());
        AddCols addCols = new AddCols(
            QualifiedName.of("a.b"),
            columnLists
        );
        DialectNode dialectNode = mcTransformer.transform(addCols);
        assertEquals(dialectNode.getNode(),
            "ALTER TABLE b ADD COLUMNS\n"
                + "(\n"
                + "   col1 BIGINT\n"
                + ")");
    }

    @Test
    public void testSetProperties() {
        SetTableProperties setTableProperties = new SetTableProperties(
            QualifiedName.of("a.b"),
            ImmutableList.of(
                new Property("key", "value")
            )
        );
        DialectNode dialectNode = mcTransformer.transform(setTableProperties);
        assertFalse(dialectNode.isExecutable());
    }

    @Test
    public void testSetPropertiesLifecycle() {
        SetTableProperties setTableProperties = new SetTableProperties(
            QualifiedName.of("a.b"),
            ImmutableList.of(new Property("life_cycle", "5"))
        );
        DialectNode dialectNode = mcTransformer.transform(setTableProperties);
        String node = dialectNode.getNode();
        assertEquals(node, "ALTER TABLE b ENABLE LIFECYCLE;\n"
            + "ALTER TABLE b SET LIFECYCLE 5");
    }

    @Test
    public void testUnsetProperties() {
        UnSetTableProperties unSetTableProperties = new UnSetTableProperties(
            QualifiedName.of("a.b"),
            ImmutableList.of("a", "b", "c")
        );
        DialectNode dialectNode = mcTransformer.transform(unSetTableProperties);
        assertFalse(dialectNode.isExecutable());
    }

    @Test
    public void testDropConstraint() {
        DropConstraint dropConstraint = new DropConstraint(
            QualifiedName.of("a.b"),
            new Identifier("constraintName")
        );
        DialectNode dialectNode = mcTransformer.transform(dropConstraint);
        assertFalse(dialectNode.isExecutable());
    }

    @Test
    public void testAddConstraint() {
        AddConstraint addConstraint = new AddConstraint(
            QualifiedName.of("a.b"),
            new PrimaryConstraint(new Identifier("a"),
                ImmutableList.of(new Identifier("col1")))
        );
        DialectNode dialectNode = mcTransformer.transform(addConstraint);
        assertFalse(dialectNode.isExecutable());
    }

    @Test
    public void testDropTable() {
        DropTable dropTable = new DropTable(
            QualifiedName.of("a.b")
        );
        DialectNode transform = mcTransformer.transform(dropTable);
        assertEquals(transform.getNode(), "DROP TABLE b");
    }

    @Test
    public void testChangeCol() {
        ChangeCol changeCol = new ChangeCol(
            QualifiedName.of("abc"),
            new Identifier("c1"),
            ColumnDefinition.builder()
                .colName(new Identifier("c2"))
                .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
                .comment(new Comment("comment"))
                .build()
        );
        DialectNode transform = mcTransformer.transform(changeCol);
        assertEquals(transform.getNode(), "ALTER TABLE abc CHANGE COLUMN c1 c2 BIGINT COMMENT 'comment'");
    }

    @Test
    public void testIntTransform() {
        List<ColumnDefinition> columns = ImmutableList.of(
            ColumnDefinition.builder()
                .colName(new Identifier("c1"))
                .dataType(DataTypeUtil.simpleType(DataTypeEnums.INT))
                .comment(new Comment("comment"))
                .build(),
            ColumnDefinition.builder()
                .colName(new Identifier("c2"))
                .dataType(DataTypeUtil.simpleType(DataTypeEnums.DATE))
                .comment(new Comment("comment"))
                .build()
        );
        CreateTable createTable = CreateTable.builder()
            .columns(columns)
            .tableName(QualifiedName.of("abc"))
            .build();
        DialectNode transform = mcTransformer.transform(createTable);
        assertEquals(transform.getNode(), "CREATE TABLE abc\n"
            + "(\n"
            + "   c1 INT COMMENT 'comment',\n"
            + "   c2 DATE COMMENT 'comment'\n"
            + ")\n");
    }

    @Test
    public void testTransformView() {
        List<ColumnDefinition> columnDefines = ImmutableList.of(
            ColumnDefinition.builder()
                .colName(new Identifier("c1"))
                .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
                .build()
        );
        CreateTable createTable = CreateDimTable
            .builder()
            .tableName(QualifiedName.of("abc"))
            .columns(columnDefines)
            .build();
        DialectNode transform = mcTransformer.transform(createTable,
            TransformContext.builder().transformToView(ViewSetting.builder().transformToView(true).build()).build());
        assertEquals(transform.getNode(), "CREATE OR REPLACE VIEW abc\n"
            + "(\n"
            + "   c1\n"
            + ")\n"
            + "AS SELECT 0\n"
            + "\n");
    }

    @Test
    public void testTransformViewUseQueryCode() {
        List<ColumnDefinition> columnDefines = ImmutableList.of(
            ColumnDefinition.builder()
                .colName(new Identifier("c1"))
                .comment(new Comment("comment"))
                .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
                .build()
        );
        CreateTable createTable = CreateDimTable
            .builder()
            .tableName(QualifiedName.of("abc"))
            .columns(columnDefines)
            .build();
        DialectNode transform = mcTransformer.transform(createTable,
            TransformContext.builder().transformToView(
                ViewSetting.builder().transformToView(true).useQueryCode(true).queryCode("select a,b").build()).build());
        assertEquals(transform.getNode(), "CREATE OR REPLACE VIEW abc\n"
            + "(\n"
            + "   c1 COMMENT 'comment'\n"
            + ")\n"
            + "AS select a,b");
    }

    @Test
    public void testTransformDropTable() {
        DropTable dropTable = new DropTable(QualifiedName.of("abc"));
        DialectNode transform = mcTransformer.transform(dropTable,
            TransformContext.builder().transformToView(ViewSetting.builder().transformToView(true).build()).build());
        assertEquals(transform.getNode(), "DROP VIEW abc");
    }

    @Test
    public void testTransformDropTableMaterializedView() {
        DropTable dropTable = new DropTable(QualifiedName.of("abc"));
        DialectNode transform = mcTransformer.transform(dropTable,
            TransformContext.builder().transformToView(ViewSetting.builder().transformToMaterializedView(true).build()).build());
        assertEquals(transform.getNode(), "DROP MATERIALIZED VIEW abc");
    }

    @Test
    public void testTransformCreateTableMaterializedView() {
        List<ColumnDefinition> columns = ImmutableList.of(
            ColumnDefinition.builder()
                .colName(new Identifier("c1"))
                .dataType(DataTypeUtil.simpleType("bigint", null))
                .build()
        );
        CreateTable createTable = CreateTable.builder().tableName(QualifiedName.of("abc"))
            .columns(columns)
            .build();
        DialectNode transform = mcTransformer.transform(createTable,
            TransformContext.builder().transformToView(ViewSetting.builder().transformToMaterializedView(true).build()).build());
        assertEquals(transform.getNode(), "CREATE MATERIALIZED VIEW abc\n"
            + "(\n"
            + "   c1\n"
            + ")\n"
            + "AS SELECT 0\n"
            + "\n");
    }
}
