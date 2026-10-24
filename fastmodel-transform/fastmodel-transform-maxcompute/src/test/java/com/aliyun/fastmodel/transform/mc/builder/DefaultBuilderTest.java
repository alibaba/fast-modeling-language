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

package com.aliyun.fastmodel.transform.mc.builder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.DereferenceExpression;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.literal.LongLiteral;
import com.aliyun.fastmodel.core.tree.relation.querybody.Union;
import com.aliyun.fastmodel.core.tree.statement.select.Limit;
import com.aliyun.fastmodel.core.tree.statement.select.Query;
import com.aliyun.fastmodel.core.tree.statement.select.item.SelectItem;
import com.aliyun.fastmodel.core.tree.statement.select.item.SingleColumn;
import com.aliyun.fastmodel.core.tree.statement.select.order.NullOrdering;
import com.aliyun.fastmodel.core.tree.statement.select.order.OrderBy;
import com.aliyun.fastmodel.core.tree.statement.select.order.Ordering;
import com.aliyun.fastmodel.core.tree.statement.select.order.SortItem;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateDimTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.DropTable;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.core.tree.statement.table.RenameTable;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableComment;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.aliyun.fastmodel.core.tree.util.QueryUtil;
import com.aliyun.fastmodel.transform.api.builder.StatementBuilder;
import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.mc.context.MaxComputeContext;
import com.aliyun.fastmodel.transform.mc.format.MaxComputePropertyKey;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/2/8
 */
public class DefaultBuilderTest {

    private StatementBuilder defaultBuilder = new DefaultBuilder();

    @Test
    public void getFmlTemplate() {
        DefaultBuilder dropTableBuilder = new DefaultBuilder();
        DropTable dropTableStatement = new DropTable(QualifiedName.of("t1"));
        DialectNode build = dropTableBuilder.build(dropTableStatement, MaxComputeContext.builder().build());
        assertNotNull(build);
        String target = build.getNode();
        assertEquals("DROP TABLE t1", target);
    }

    @Test
    public void testCreateTableBuild() {
        CreateTable statement = getCreateTableStatement();
        TransformContext context = MaxComputeContext.builder().build();
        DialectNode result = defaultBuilder.build(statement, context);
        assertEquals(result.getNode(), "CREATE TABLE t1\n"
            + "(\n"
            + "   col1 BIGINT COMMENT 'comment1'\n"
            + ")\n"
            + "COMMENT 'comment'");
    }

    @Test
    public void testTransform() throws Exception {
        Comment comment = new Comment("comment");
        ColumnDefinition columnDefine = ColumnDefinition.builder().colName(new Identifier("col1")).dataType(
            DataTypeUtil.simpleType(DataTypeEnums.DOUBLE)
        ).comment(comment).build();
        List<ColumnDefinition> list = new ArrayList<>();
        ColumnDefinition typeStatement = ColumnDefinition.builder().colName(new Identifier("col2")).dataType(
            DataTypeUtil.simpleType(DataTypeEnums.DOUBLE)
        ).comment(new Comment("comment")).build();
        list.add(typeStatement);
        CreateTable createTableStatement = CreateDimTable.builder().tableName(QualifiedName.of("tableName")).columns(
                ImmutableList.of(columnDefine)
            ).partition(
                new PartitionedBy(list)
            ).properties(Lists.newArrayList(new Property(MaxComputePropertyKey.LIFE_CYCLE.getValue(), "10")))
            .build();
        TransformContext context = MaxComputeContext.builder().build();
        DialectNode result = defaultBuilder.build(createTableStatement, context);
        assertEquals(result.getNode(), "CREATE TABLE tableName\n"
            + "(\n"
            + "   col1 DOUBLE COMMENT 'comment'\n"
            + ")\n"
            + "PARTITIONED BY\n"
            + "(\n"
            + "   col2 DOUBLE COMMENT 'comment'\n"
            + ")\n"
            + "LIFECYCLE 10");
    }

    private CreateTable getCreateTableStatement() {
        List<ColumnDefinition> list = new ArrayList<>();
        ColumnDefinition columnDefine = ColumnDefinition.builder().colName(new Identifier("col1")).dataType(
            DataTypeUtil.simpleType(DataTypeEnums.BIGINT)
        ).comment(new Comment("comment1")).build();
        list.add(columnDefine);
        return CreateDimTable.builder().tableName(
            QualifiedName.of("t1")).columns(
            list).comment(
            new Comment("comment")
        ).build()
            ;
    }

    @Test
    public void testBuild() {
        Query statement = getSelectStatement();
        DialectNode result = defaultBuilder.build(statement, MaxComputeContext.builder().build());
        assertEquals("SELECT concat(t1.sku_code, t1_shop_code) sys_pk\n\n", result.getNode());
    }

    private Query getSelectStatement() {
        BaseExpression expression = QueryUtil.functionCall("concat",
            DereferenceExpression.from(QualifiedName.of("t1", "sku_code")),
            DereferenceExpression.from(QualifiedName.of("t1_shop_code")));
        return QueryUtil.simpleQuery(
            QueryUtil.selectAll(Collections.singletonList(new SingleColumn(expression, new Identifier("sys_pk")))));
    }

    @Test
    public void testBuildUnion() {
        Query statement = getUnionSelectStatement();
        DialectNode build = defaultBuilder.build(statement, MaxComputeContext.builder().build());
        assertNotNull(build);
        assertNotNull(build.getNode());
        assertTrue(build.getNode().contains("UNION"));
    }

    @Test
    public void testBuildWithSubQuery() {
        Query statement = getSelectStatement();
        DialectNode build = defaultBuilder.build(statement, MaxComputeContext.builder().build());
        assertNotNull(build);
        assertNotNull(build.getNode());
        assertTrue(build.getNode().contains("("));
    }

    @Test
    public void testBuildWithOrderBy() {
        Query statement = getSelectStatement();
        statement.setOrderBy(new OrderBy(getOrderRefColumnList()));
        DialectNode node = defaultBuilder.build(statement, MaxComputeContext.builder().build());
        assertTrue(node.getNode().contains("ORDER BY"));
    }

    @Test
    public void testBuildWithLimit() {
        Query statement = getSelectStatement();
        statement.setLimit(new Limit(new LongLiteral("10")));
        DialectNode build = defaultBuilder.build(statement, MaxComputeContext.builder().build());
        assertNotNull(build);
        assertNotNull(build.getNode());
        assertTrue(build.getNode().contains("LIMIT"));
    }

    private List<SortItem> getOrderRefColumnList() {
        return ImmutableList.of(new SortItem(new Identifier("t1.sku_code"), Ordering.DESC, NullOrdering.UNDEFINED));
    }

    private Query getUnionSelectStatement() {
        SelectItem selectItem = QueryUtil.aliasedName("t1", "a1");
        Union union = new Union(
            ImmutableList.of(
                QueryUtil.simpleQuery(QueryUtil.selectAll(Collections.singletonList(selectItem))).getQueryBody(),
                QueryUtil.simpleQuery(QueryUtil.selectAll(Collections.singletonList(selectItem))).getQueryBody()
            ), false
        );
        return QueryUtil.query(union);
    }

    @Test
    public void testRenameBuild() throws Exception {
        RenameTable source = new RenameTable(QualifiedName.of("name"), QualifiedName.of("next"));
        DialectNode result = defaultBuilder.build(source, MaxComputeContext.builder().build());
        assertEquals("ALTER TABLE name RENAME TO next", result.getNode());
    }

    @Test
    public void testSetTableCommentBuilder() {
        DialectNode comment = defaultBuilder.build(
            new SetTableComment(QualifiedName.of("a.b"), new Comment("comment")), MaxComputeContext.builder().build());
        assertEquals(comment.getNode(), "ALTER TABLE b SET COMMENT 'comment'");
    }

    @Test
    public void testSetTableCommentBuilderWithContext() {
        DialectNode comment = defaultBuilder.build(
            new SetTableComment(QualifiedName.of("a.b"), new Comment("comment")), MaxComputeContext.builder().mcProject("a").build());
        assertEquals(comment.getNode(), "ALTER TABLE a.b SET COMMENT 'comment'");
    }
}