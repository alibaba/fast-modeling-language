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

package com.aliyun.fastmodel.core.tree.util;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.aliyun.fastmodel.core.tree.BaseRelation;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.NodeLocation;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.ComparisonExpression;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.LogicalBinaryExpression;
import com.aliyun.fastmodel.core.tree.expr.atom.SearchedCaseExpression;
import com.aliyun.fastmodel.core.tree.expr.enums.ComparisonOperator;
import com.aliyun.fastmodel.core.tree.expr.enums.LogicalOperator;
import com.aliyun.fastmodel.core.tree.expr.literal.LongLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.core.tree.relation.querybody.BaseQueryBody;
import com.aliyun.fastmodel.core.tree.relation.querybody.QuerySpecification;
import com.aliyun.fastmodel.core.tree.relation.querybody.Table;
import com.aliyun.fastmodel.core.tree.statement.select.Limit;
import com.aliyun.fastmodel.core.tree.statement.select.Offset;
import com.aliyun.fastmodel.core.tree.statement.select.Query;
import com.aliyun.fastmodel.core.tree.statement.select.Select;
import com.aliyun.fastmodel.core.tree.statement.select.groupby.GroupBy;
import com.aliyun.fastmodel.core.tree.statement.select.item.AllColumns;
import com.aliyun.fastmodel.core.tree.statement.select.item.SelectItem;
import com.aliyun.fastmodel.core.tree.statement.select.item.SingleColumn;
import com.aliyun.fastmodel.core.tree.statement.select.order.NullOrdering;
import com.aliyun.fastmodel.core.tree.statement.select.order.OrderBy;
import com.aliyun.fastmodel.core.tree.statement.select.order.Ordering;
import com.aliyun.fastmodel.core.tree.statement.select.order.SortItem;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2020/11/4
 */
public class QueryUtilTest {

    @Test
    public void testIdentifier() throws Exception {
        Identifier result = QueryUtil.identifier("name");
        assertEquals(result.getValue(), "name");
    }

    @Test
    public void testUnAliasedName() throws Exception {
        SelectItem result = QueryUtil.unAliasedName("name");
        assertEquals(new SingleColumn(new Identifier("name")), result);
    }

    @Test
    public void testAliasedName() throws Exception {
        SelectItem result = QueryUtil.aliasedName("name", "alias");
        assertEquals(new SingleColumn(new Identifier("name"), new Identifier("alias")), result);
    }

    @Test
    public void testSelectList() throws Exception {
        LongLiteral longLiteral = new LongLiteral("123");
        Select result = QueryUtil.selectList(longLiteral);
        assertEquals(result.getSelectItems().size(), 1);
        List<SelectItem> selectItems = result.getSelectItems();
        SingleColumn selectItem = (SingleColumn)selectItems.get(0);
        assertEquals((LongLiteral)selectItem.getExpression(), longLiteral);
    }

    @Test
    public void testSelectList2() throws Exception {
        Select result = QueryUtil.selectList(Collections.singletonList(new LongLiteral("1234")));
        Select expected = new Select(Collections.singletonList(new SingleColumn(new LongLiteral("1234"))), false);
        assertEquals(
            expected, result);
    }

    @Test
    public void testSelectAll() throws Exception {
        Identifier r = new Identifier("r");
        AllColumns allColumns = new AllColumns(r);
        Select result = QueryUtil.selectAll(
            Arrays.<SelectItem>asList(allColumns));
        assertEquals(result.getSelectItems().size(), 1);
        SelectItem selectItem = result.getSelectItems().get(0);
        assertEquals(selectItem.getClass(), AllColumns.class);
        AllColumns allColumns1 = (AllColumns)selectItem;
        assertEquals(allColumns1.getTarget(), r);
    }

    @Test
    public void testSubQuery() throws Exception {
        BaseRelation result = QueryUtil.subQuery(QueryUtil.simpleQuery(QueryUtil.selectList(new StringLiteral("abc"))));
        List<? extends Node> children = result.getChildren();
        assertNotNull(children);
    }

    @Test
    public void testEqual() throws Exception {
        Identifier abc = new Identifier("abc");
        StringLiteral e = new StringLiteral("e");
        BaseExpression result = QueryUtil.equal(abc, e);
        assertEquals(result.getClass(), ComparisonExpression.class);
        ComparisonExpression comparisonExpression = (ComparisonExpression)result;
        BaseExpression left = comparisonExpression.getLeft();
        assertEquals(abc, left);
        assertEquals(e, comparisonExpression.getRight());
    }

    @Test
    public void testSimpleQuery() throws Exception {
        Query result = QueryUtil.simpleQuery(QueryUtil.selectList(new StringLiteral("abc")));
        assertNotNull(result);
        BaseQueryBody queryBody = result.getQueryBody();
        assertEquals(queryBody.getClass(), QuerySpecification.class);
    }

    @Test
    public void testSimpleQuery2() throws Exception {
        Query result = QueryUtil.simpleQuery(
            QueryUtil.selectList(new Identifier("bcd")),
            QueryUtil.table(QualifiedName.of("t1")));
        QuerySpecification querySpecification = (QuerySpecification)result.getQueryBody();
        BaseRelation from = querySpecification.getFrom();
        assertEquals(from.getClass(), Table.class);
    }

    @Test
    public void testSimpleQuery3() throws Exception {
        Query result = QueryUtil.simpleQuery(
            new Select(Arrays.<SelectItem>asList(getAllColumns()), true),
            getFrom(), getWhere());
        assertNotNull(result);
        QuerySpecification specification = (QuerySpecification)result.getQueryBody();
        BaseExpression where = specification.getWhere();
        assertEquals(where.getClass(), LogicalBinaryExpression.class);
    }

    private LogicalBinaryExpression getWhere() {
        return new LogicalBinaryExpression(
            LogicalOperator.OR,
            new ComparisonExpression(
                ComparisonOperator.EQUAL,
                new Identifier("field"),
                new LongLiteral("0")
            ),
            new ComparisonExpression(ComparisonOperator.EQUAL,
                new Identifier("field1"),
                new LongLiteral("1"))
        );
    }
   
    private QuerySpecification getFrom() {
        return new QuerySpecification(QueryUtil.selectList(new StringLiteral("string")), null, null, null, null, null,
            null,
            null, null);
    }

    @Test
    public void testSimpleQuery5() throws Exception {
        OrderBy orderBy = new OrderBy(
            Collections.singletonList(
                new SortItem(new Identifier("x"), Ordering.DESC, NullOrdering.UNDEFINED)));
        Offset offset = new Offset(new NodeLocation(0, 0), null);
        Limit limit = new Limit(new LongLiteral("123"));
        GroupBy groupBy = new GroupBy(new NodeLocation(0, 0), true, null);
        Query result = QueryUtil.simpleQuery(
            new Select(Collections.singletonList(getAllColumns()), true),
            getFrom(), getWhere(), groupBy, getHaving(),
            orderBy, offset, limit);
        assertNotNull(result);
        BaseQueryBody queryBody = result.getQueryBody();
        QuerySpecification querySpecification = (QuerySpecification)queryBody;
        BaseRelation from = querySpecification.getFrom();
        assertEquals(from, getFrom());

    }

    @Test
    public void testCaseWhen() {
        BaseExpression expression = QueryUtil.caseWhen(QueryUtil.equal(new Identifier("abc"), new StringLiteral("1")),
            new Identifier("b"));
        assertEquals(expression.getClass(), SearchedCaseExpression.class);
    }

    private BaseExpression getHaving() {
        return null;
    }

    private AllColumns getAllColumns() {
        return new AllColumns(new Identifier("x"));
    }

}
//Generated with love by TestMe :) Please report issues and submit feature requests at: http://weirddev
// .com/forum#!/testme