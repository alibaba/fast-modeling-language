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
import java.util.List;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.core.tree.BaseRelation;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.ComparisonExpression;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.Row;
import com.aliyun.fastmodel.core.tree.expr.atom.FunctionCall;
import com.aliyun.fastmodel.core.tree.expr.atom.SearchedCaseExpression;
import com.aliyun.fastmodel.core.tree.expr.atom.TableOrColumn;
import com.aliyun.fastmodel.core.tree.expr.atom.WhenClause;
import com.aliyun.fastmodel.core.tree.expr.enums.ComparisonOperator;
import com.aliyun.fastmodel.core.tree.relation.AliasedRelation;
import com.aliyun.fastmodel.core.tree.relation.querybody.BaseQueryBody;
import com.aliyun.fastmodel.core.tree.relation.querybody.QuerySpecification;
import com.aliyun.fastmodel.core.tree.relation.querybody.Table;
import com.aliyun.fastmodel.core.tree.relation.querybody.TableSubQuery;
import com.aliyun.fastmodel.core.tree.relation.querybody.Values;
import com.aliyun.fastmodel.core.tree.statement.select.Limit;
import com.aliyun.fastmodel.core.tree.statement.select.Offset;
import com.aliyun.fastmodel.core.tree.statement.select.Query;
import com.aliyun.fastmodel.core.tree.statement.select.Select;
import com.aliyun.fastmodel.core.tree.statement.select.groupby.GroupBy;
import com.aliyun.fastmodel.core.tree.statement.select.item.SelectItem;
import com.aliyun.fastmodel.core.tree.statement.select.item.SingleColumn;
import com.aliyun.fastmodel.core.tree.statement.select.order.OrderBy;
import com.google.common.collect.ImmutableList;

/**
 * 常用的查询的工具类
 * <p>
 * QueryUtil.simpleQuery
 * </p>
 *
 * @author panguanjing
 * @date 2020/11/3
 */
public class QueryUtil {
    private QueryUtil() {}

    public static Identifier identifier(String name) {
        return new Identifier(name);
    }

    public static SelectItem unAliasedName(String name) {
        return new SingleColumn(identifier(name));
    }

    public static SelectItem aliasedName(BaseExpression name, String alias) {
        return new SingleColumn(name, identifier(alias));
    }

    public static SelectItem aliasedName(String name, String alias) {
        return new SingleColumn(identifier(name), identifier(alias));
    }

    public static SelectItem aliasedName(TableOrColumn name, String alias) {
        return new SingleColumn(name, identifier(alias));
    }

    public static Select selectList(BaseExpression... expressions) {
        return selectList(Arrays.asList(expressions));
    }

    public static Select selectList(List<BaseExpression> expressionList) {
        ImmutableList.Builder<SelectItem> items = ImmutableList.builder();
        for (BaseExpression expression : expressionList) {
            items.add(new SingleColumn(expression));
        }
        return new Select(items.build(), false);
    }

    public static Select selectAll(List<SelectItem> items) {
        return new Select(items, false);
    }

    public static BaseRelation subQuery(Query query) {
        return new TableSubQuery(query);
    }

    public static BaseExpression equal(BaseExpression left, BaseExpression right) {
        return new ComparisonExpression(ComparisonOperator.EQUAL, left, right);
    }

    public static BaseExpression caseWhen(BaseExpression operand, BaseExpression result) {
        return new SearchedCaseExpression(ImmutableList.of(new WhenClause(operand, result)), null);
    }

    public static FunctionCall functionCall(String name, BaseExpression... arguments) {
        return new FunctionCall(QualifiedName.of(name), false, ImmutableList.copyOf(arguments), null, null,
            null, null);
    }

    public static BaseRelation aliased(BaseRelation relation, String alias, List<String> columnAliases) {
        return new AliasedRelation(
            relation,
            identifier(alias),
            columnAliases.stream()
                .map(QueryUtil::identifier)
                .collect(Collectors.toList()));
    }

    public static Query simpleQuery(Select select) {
        return simpleQuery(select, null, null, null, null, null, null, null);
    }

    public static Query simpleQuery(Select select, BaseRelation from) {
        return simpleQuery(select, from, null, null, null, null, null, null);
    }

    public static Query simpleQuery(Select select, BaseRelation from, BaseExpression where) {
        return simpleQuery(select, from, where, null, null, null, null, null);
    }

    public static Query simpleQuery(Select select, BaseRelation from, BaseExpression where, OrderBy orderBy) {
        return simpleQuery(select, from, where, null, null, orderBy, null, null);
    }

    public static Query simpleQuery(
        Select select,
        BaseRelation from,
        BaseExpression where,
        GroupBy groupBy,
        BaseExpression having,
        OrderBy orderBy,
        Offset offset,
        Limit limit) {
        QuerySpecification querySpecification = QuerySpecification.builder().select(select).from(from).where(where)
            .groupBy(groupBy).having(having).orderBy(orderBy).offset(offset).limit(limit).build();
        return query(querySpecification);
    }

    public static Query query(BaseQueryBody body) {
        return new Query(
            null,
            body,
            null,
            null,
            null);
    }

    public static Table table(QualifiedName name) {
        return new Table(name);
    }

    /**
     * values
     *
     * @param rows {@link Row}
     * @return {@link Values}
     */
    public static Values values(Row... rows) {
        return new Values(ImmutableList.copyOf(rows));
    }

    /**
     * row
     *
     * @param expressions {@link BaseExpression}
     * @return {@link Row}
     */
    public static Row row(BaseExpression... expressions) {
        return new Row(ImmutableList.copyOf(expressions));
    }

}
