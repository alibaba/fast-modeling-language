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

package com.aliyun.fastmodel.transform.mc.builder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.aliyun.fastmodel.core.tree.BaseRelation;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.atom.FunctionCall;
import com.aliyun.fastmodel.core.tree.expr.atom.TableOrColumn;
import com.aliyun.fastmodel.core.tree.relation.AliasedRelation;
import com.aliyun.fastmodel.core.tree.relation.querybody.BaseQueryBody;
import com.aliyun.fastmodel.core.tree.relation.querybody.QuerySpecification;
import com.aliyun.fastmodel.core.tree.relation.querybody.SetOperation;
import com.aliyun.fastmodel.core.tree.relation.querybody.TableSubQuery;
import com.aliyun.fastmodel.core.tree.statement.select.RowToCol;
import com.aliyun.fastmodel.core.tree.statement.select.Select;
import com.aliyun.fastmodel.core.tree.statement.select.groupby.GroupBy;
import com.aliyun.fastmodel.core.tree.statement.select.groupby.GroupingElement;
import com.aliyun.fastmodel.core.tree.statement.select.groupby.SimpleGroupBy;
import com.aliyun.fastmodel.core.tree.statement.select.item.SelectItem;
import com.aliyun.fastmodel.core.tree.statement.select.item.SingleColumn;
import com.aliyun.fastmodel.core.tree.util.QueryUtil;
import com.aliyun.fastmodel.transform.api.builder.BuilderAnnotation;
import com.aliyun.fastmodel.transform.api.builder.StatementBuilder;
import com.aliyun.fastmodel.transform.api.dialect.DialectName;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.mc.context.MaxComputeContext;
import com.aliyun.fastmodel.transform.mc.format.MaxComputeFormatter;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;

/**
 * QueryRowToColBuilder:
 *
 * @author panguanjing
 * @date 2020/10/20
 */
@BuilderAnnotation(values = RowToCol.class, dialect = DialectName.Constants.MAXCOMPUTE)
@AutoService(StatementBuilder.class)
public class RowToColBuilder extends BaseBuilder {

    @Override
    public DialectNode build(BaseStatement source, MaxComputeContext context) {
        RowToCol rowToCol = (RowToCol)source;
        BaseQueryBody body = rowToCol(rowToCol.getQuery().getQueryBody());
        return MaxComputeFormatter.formatSql(body, context);
    }

    private BaseQueryBody rowToCol(BaseQueryBody queryBody) {
        QuerySpecification querySpecification = (QuerySpecification)queryBody;
        Select select = toSelect(querySpecification);
        BaseRelation from = toFrom(querySpecification.getSelect().getSelectItems(), querySpecification.getFrom());
        GroupBy groupBy = toGroupBy(querySpecification);
        querySpecification.setSelect(select);
        querySpecification.setFrom(from);
        querySpecification.setGroupBy(groupBy);
        return querySpecification;
    }

    private GroupBy toGroupBy(QuerySpecification querySpecification) {
        GroupBy groupByStatement = querySpecification.getGroupBy();
        List<GroupingElement> groupingElements = groupByStatement.getGroupingElements();
        GroupBy newGroup = new GroupBy();
        List<GroupingElement> newGroupElements = new ArrayList<>();
        for (GroupingElement groupingElement : groupingElements) {
            AliasedRelation from = (AliasedRelation)querySpecification.getFrom();
            List<BaseExpression> expressions = groupingElement.getExpressions();
            for (BaseExpression baseExpression : expressions) {
                if (baseExpression instanceof TableOrColumn) {
                    TableOrColumn identifier = (TableOrColumn)baseExpression;
                    QualifiedName qualifiedName = identifier.getQualifiedName();
                    identifier.setQualifiedName(
                        QualifiedName.of(from.getAlias().getValue(), qualifiedName.getSuffix()));
                }
            }
            SimpleGroupBy simpleGroupBy = new SimpleGroupBy(expressions);
            newGroupElements.add(simpleGroupBy);
        }
        newGroup.setDistinct(false);
        newGroup.setGroupingElements(newGroupElements);
        return newGroup;
    }

    private BaseRelation toFrom(List<SelectItem> src, BaseRelation querySpecification) {
        if (querySpecification instanceof SetOperation) {
            SetOperation setOperation = (SetOperation)querySpecification;
            List<BaseRelation> relations = setOperation.getRelations();
            for (BaseRelation baseRelation : relations) {
                toFrom(src, baseRelation);
            }
        } else {
            if (querySpecification instanceof TableSubQuery) {
                TableSubQuery tableSubQuery = (TableSubQuery)querySpecification;
                BaseQueryBody queryBody = tableSubQuery.getQuery().getQueryBody();
                toFrom(src, queryBody);
            } else if (querySpecification instanceof QuerySpecification) {
                setSelectItem(src, (QuerySpecification)querySpecification);
            } else if (querySpecification instanceof AliasedRelation) {
                AliasedRelation aliasedRelation = (AliasedRelation)querySpecification;
                toFrom(src, aliasedRelation.getRelation());
                return aliasedRelation;
            }
        }
        return querySpecification;
    }

    private void setSelectItem(List<SelectItem> src, QuerySpecification selectStatement) {
        Select originSelectStatement = selectStatement.getSelect();
        List<SelectItem> list = originSelectStatement.getSelectItems();
        List<SelectItem> newSelectItem = toNewAliasStatement(list, src);
        originSelectStatement.setSelectItems(newSelectItem);
    }

    /**
     * pk1, pk2, pk3, dim_1, dim_2, dim3, ind1, ind2, ind3
     * AS
     */
    private Select toSelect(QuerySpecification querySpecification) {
        Select originColStatement = querySpecification.getSelect();
        GroupBy groupByStatement = querySpecification.getGroupBy();
        List<GroupingElement> groupByField = groupByStatement.getGroupingElements();
        List<String> pks = new ArrayList<>();
        for (GroupingElement groupingElement : groupByField) {
            for (BaseExpression groupBy : groupingElement.getExpressions()) {
                TableOrColumn tableOrColumn = (TableOrColumn)groupBy;
                pks.add(tableOrColumn.getQualifiedName().getSuffix());
            }
        }
        AliasedRelation aliasedRelation = (AliasedRelation)querySpecification.getFrom();
        String alias = aliasedRelation.getAlias().getValue();
        List<SelectItem> list = new ArrayList<>();
        for (SelectItem selectItem : originColStatement.getSelectItems()) {
            SingleColumn singleColumn = (SingleColumn)selectItem;
            Identifier alias1 = singleColumn.getAlias();
            TableOrColumn expression = new TableOrColumn(
                QualifiedName.of(alias, singleColumn.getExpression().toString()));
            if (alias1 != null) {
                String expr = singleColumn.getAlias().toString();
                if (pks.contains(expr)) {
                    SelectItem build = new SingleColumn(expression, new Identifier(alias));
                    list.add(build);
                }
            } else {
                if (pks.contains(singleColumn.getExpression().toString())) {
                    SelectItem build = new SingleColumn(expression, (Identifier)singleColumn.getExpression());
                    list.add(build);
                } else {
                    FunctionCall functionCall = new FunctionCall(
                        QualifiedName.of("max"),
                        false,
                        ImmutableList
                            .of(new TableOrColumn(QualifiedName.of(alias, singleColumn.getExpression().toString()))
                            ), null, null, null, null);
                    SelectItem build = new SingleColumn(functionCall, (Identifier)singleColumn.getExpression());
                    list.add(build);
                }
            }
        }
        return QueryUtil.selectAll(list);
    }

    private List<SelectItem> toNewAliasStatement(List<SelectItem> opSelectAlias,
                                                 List<SelectItem> originAlias) {
        List<SelectItem> newSelectItem = new ArrayList<>();
        Map<String, SelectItem> newMap = new HashMap<>(opSelectAlias.size());
        for (SelectItem a : opSelectAlias) {
            SingleColumn singleColumn = (SingleColumn)a;
            BaseExpression expression = singleColumn.getExpression();
            newMap.put(expression.toString(), singleColumn);
            Identifier alias = singleColumn.getAlias();
            if (alias != null) {
                newMap.put(alias.toString(), singleColumn);
            }
        }
        for (SelectItem origin : originAlias) {
            SingleColumn singleColumn = (SingleColumn)origin;
            BaseExpression identifier = singleColumn.getExpression();
            String key = singleColumn.getAlias() == null ? identifier.toString() : singleColumn.getAlias().toString();
            SelectItem statement = newMap.get(key);
            if (statement != null) {
                newSelectItem.add(statement);
            } else {
                SelectItem selectItem = new SingleColumn(new Identifier("NULL"),
                    (Identifier)singleColumn.getExpression());
                newSelectItem.add(selectItem);
            }
        }
        return newSelectItem;
    }

}
