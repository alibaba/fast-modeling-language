/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.hologres.parser.visitor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.core.tree.BaseRelation;
import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.ComparisonExpression;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.atom.FunctionCall;
import com.aliyun.fastmodel.core.tree.expr.atom.TableOrColumn;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.core.tree.expr.window.Window;
import com.aliyun.fastmodel.core.tree.relation.AliasedRelation;
import com.aliyun.fastmodel.core.tree.relation.Join;
import com.aliyun.fastmodel.core.tree.relation.join.JoinCriteria;
import com.aliyun.fastmodel.core.tree.relation.join.JoinOn;
import com.aliyun.fastmodel.core.tree.relation.querybody.BaseQueryBody;
import com.aliyun.fastmodel.core.tree.relation.querybody.QuerySpecification;
import com.aliyun.fastmodel.core.tree.relation.querybody.Table;
import com.aliyun.fastmodel.core.tree.relation.querybody.TableSubQuery;
import com.aliyun.fastmodel.core.tree.statement.select.Hint;
import com.aliyun.fastmodel.core.tree.statement.select.Offset;
import com.aliyun.fastmodel.core.tree.statement.select.Query;
import com.aliyun.fastmodel.core.tree.statement.select.Select;
import com.aliyun.fastmodel.core.tree.statement.select.With;
import com.aliyun.fastmodel.core.tree.statement.select.groupby.GroupBy;
import com.aliyun.fastmodel.core.tree.statement.select.groupby.GroupingElement;
import com.aliyun.fastmodel.core.tree.statement.select.groupby.SimpleGroupBy;
import com.aliyun.fastmodel.core.tree.statement.select.item.AllColumns;
import com.aliyun.fastmodel.core.tree.statement.select.item.SelectItem;
import com.aliyun.fastmodel.core.tree.statement.select.item.SingleColumn;
import com.aliyun.fastmodel.core.tree.statement.select.order.OrderBy;
import com.aliyun.fastmodel.core.tree.statement.select.order.SortItem;
import com.aliyun.fastmodel.core.tree.statement.select.sort.SortBy;
import com.aliyun.fastmodel.transform.hologres.context.HologresTransformContext;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * Query visitor
 * https://yuque.antfin-inc.com/docs/share/06c91e54-aacf-4a34-b593-762a606b6a22?#pYDCk
 *
 * @author panguanjing
 * @date 2022/6/18
 */
public class HologresRewriteVisitor implements IAstVisitor<Node, Void> {

    public static final String SUB_QUERY_PREFIX = "sub";

    public static final String MAX_PT = "max_pt";

    /**
     * 子查询的深度
     */
    private int subQueryDepth = 0;

    /**
     * 子查询的map
     */
    private final Map<Integer, Identifier> subAliasMap;

    /**
     * 查询列表的内容
     * key: subQueryDepth
     * value : List<Identifier>
     */
    private final Multimap<Integer, Identifier> aliasMap;

    private final Map<String, String> functionMap;
    /**
     * hologresTransformContext
     */
    private final HologresTransformContext hologresTransformContext;

    public HologresRewriteVisitor() {
        this(HologresTransformContext.builder().build());
    }

    public HologresRewriteVisitor(HologresTransformContext hologresTransformContext) {
        this.hologresTransformContext = hologresTransformContext;
        subAliasMap = Maps.newHashMap();
        functionMap = initFunctionMap();
        aliasMap = ArrayListMultimap.create();
    }

    private Map<String, String> initFunctionMap() {
        Map<String, String> maps = Maps.newHashMap();
        maps.put("datepart", "to_char");
        maps.put("from_unixtime", "to_timestamp");
        return maps;
    }

    @Override
    public Node visitQuery(Query query, Void context) {
        return new Query((With)process(query.getWith()), (BaseQueryBody)process(query.getQueryBody()), (OrderBy)process(query.getOrderBy()),
            (Offset)process(query.getOffset()), process(query.getLimit()));
    }

    @Override
    public Node visitQuerySpecification(QuerySpecification querySpecification, Void context) {
        //notice: must process from node with order
        BaseRelation relation = (BaseRelation)process(querySpecification.getFrom());
        Select select = (Select)process(querySpecification.getSelect());
        BaseExpression process = (BaseExpression)process(querySpecification.getWhere());
        SortBy sortBy = querySpecification.getSortBy();
        OrderBy orderBy = (OrderBy)process(querySpecification.getOrderBy());
        if (orderBy == null && sortBy != null) {
            orderBy = (OrderBy)process(sortBy);
        }
        return new QuerySpecification(select, toHints(querySpecification.getHints()), relation, process,
            (GroupBy)process(querySpecification.getGroupBy()), (BaseExpression)process(querySpecification.getHaving()),
            orderBy,
            null,
            null,
            null,
            (Offset)process(querySpecification.getOffset()),
            process(querySpecification.getLimit()));
    }

    @Override
    public Node visitSortBy(SortBy sortBy, Void context) {
        List<SortItem> sortItems = null;
        if (sortBy.getSortItems() != null) {
            sortItems = sortBy.getSortItems().stream().map(s -> (SortItem)process(s)).collect(Collectors.toList());
        }
        return new OrderBy(
            sortItems
        );
    }

    @Override
    public Node visitTableSubQuery(TableSubQuery tableSubQuery, Void context) {
        TableSubQuery subQuery = new TableSubQuery((Query)process(tableSubQuery.getQuery()));
        subQueryDepth++;
        Identifier alias = new Identifier(SUB_QUERY_PREFIX + subQueryDepth);
        subAliasMap.put(subQueryDepth, alias);
        return new AliasedRelation(subQuery, alias);
    }

    @Override
    public Node visitGroupBy(GroupBy groupBy, Void context) {
        List<GroupingElement> groupingElements = groupBy.getGroupingElements();
        List<GroupingElement> processElements = null;
        if (groupingElements != null) {
            processElements = groupingElements.stream().map(e -> (GroupingElement)process(e)).collect(Collectors.toList());
        }
        return new GroupBy(groupBy.isDistinct(), processElements);
    }

    @Override
    public Node visitOrderBy(OrderBy orderBy, Void context) {
        List<SortItem> sortItems = null;
        if (orderBy.getSortItems() != null) {
            sortItems = orderBy.getSortItems().stream().map(s -> (SortItem)process(s)).collect(Collectors.toList());
        }
        return new OrderBy(sortItems);
    }

    @Override
    public Node visitSortItem(SortItem sortItem, Void context) {
        return new SortItem((BaseExpression)process(sortItem.getSortKey()), sortItem.getOrdering(), sortItem.getNullOrdering());
    }

    @Override
    public Node visitSimpleGroupBy(SimpleGroupBy simpleGroupBy, Void context) {
        List<BaseExpression> columns = simpleGroupBy.getColumns();
        List<BaseExpression> processColumns = null;
        if (columns != null) {
            processColumns = columns.stream().map(c -> (BaseExpression)process(c)).collect(Collectors.toList());
        }
        return new SimpleGroupBy(processColumns);
    }

    @Override
    public Node visitSelect(Select select, Void context) {
        List<SelectItem> selectItems = select.getSelectItems().stream().map(s -> (SelectItem)process(s)).collect(Collectors.toList());
        return new Select(selectItems, select.isDistinct());
    }

    @Override
    public Node visitAllColumns(AllColumns allColumns, Void context) {
        Identifier identifier = subAliasMap.get(subQueryDepth);
        if (identifier == null) {
            return new AllColumns(allColumns.getTarget(), allColumns.getAliases());
        }
        //if source allColumn has alias
        if (allColumns.getTarget() != null) {
            return new AllColumns(allColumns.getTarget(), allColumns.getAliases());
        }
        TableOrColumn target = new TableOrColumn(QualifiedName.of(Lists.newArrayList(identifier)));
        return new AllColumns(target, allColumns.getAliases());
    }

    @Override
    public Node visitSingleColumn(SingleColumn singleColumn, Void context) {
        //if single column has alias
        BaseExpression expression = singleColumn.getExpression();
        expression = (BaseExpression)process(expression);
        Identifier alias = singleColumn.getAlias();
        //store the alias
        if (alias != null) {
            aliasMap.put(this.subQueryDepth, alias);
        }
        return new SingleColumn(singleColumn.getLocation(), expression, alias, singleColumn.isExistAs());
    }

    @Override
    public Node visitTableOrColumn(TableOrColumn tableOrColumn, Void context) {
        QualifiedName qualifiedName = tableOrColumn.getQualifiedName();
        if (qualifiedName.isJoinPath()) {
            return new TableOrColumn(tableOrColumn.getQualifiedName(), tableOrColumn.getVarType());
        }
        //if column is alias , then direct return
        if (isAliasName(qualifiedName)) {
            return new TableOrColumn(tableOrColumn.getQualifiedName(), tableOrColumn.getVarType());
        }
        //first read the sub query
        Identifier identifier = subAliasMap.get(subQueryDepth);
        if (identifier != null) {
            return new TableOrColumn(QualifiedName.of(identifier.getValue(),
                tableOrColumn.getQualifiedName().getOriginalParts().stream().map(Identifier::getValue).toArray(String[]::new)));
        }
        //return the default
        return new TableOrColumn(tableOrColumn.getQualifiedName(), tableOrColumn.getVarType());
    }

    @Override
    public Node visitWindow(Window window, Void context) {
        //hologres
        return new Window(
            Collections.emptyList(),
            window.getOrderBy(),
            window.getFrame()
        );
    }

    /**
     * 判断是否为别名
     *
     * @param qualifiedName 需要判断table or column
     * @return true if is alias
     */
    private boolean isAliasName(QualifiedName qualifiedName) {
        Collection<Identifier> identifiers = aliasMap.get(subQueryDepth);
        if (CollectionUtils.isEmpty(identifiers)) {
            return false;
        }
        List<String> value = identifiers.stream().map(Identifier::getValue).collect(Collectors.toList());
        return value.stream().anyMatch(f -> {
            String string = qualifiedName.toString();
            return StringUtils.equalsIgnoreCase(f, string);
        });
    }

    @Override
    public Node visitComparisonExpression(ComparisonExpression comparisonExpression, Void context) {
        BaseExpression left = comparisonExpression.getLeft();
        BaseExpression processLeft = (BaseExpression)process(left);
        BaseExpression processRight = (BaseExpression)process(comparisonExpression.getRight());
        return new ComparisonExpression(comparisonExpression.getOperator(), processLeft, processRight);
    }

    @Override
    public Node visitFunctionCall(FunctionCall functionCall, Void context) {
        QualifiedName funcName = functionCall.getFuncName();
        if (Objects.equals(funcName, QualifiedName.of(MAX_PT))) {
            return maxPtFunction(funcName, functionCall.isDistinct(), functionCall.getArguments());
        }
        List<BaseExpression> arguments = functionCall.getArguments();
        if (arguments != null) {
            arguments = arguments.stream().map(arg -> (BaseExpression)process(arg)).collect(Collectors.toList());
        }
        QualifiedName newFuncName = rewrite(funcName);
        //if function name is max_pt, then remove project 
        Window window = functionCall.getWindow();
        return new FunctionCall(newFuncName, functionCall.isDistinct(), arguments, (Window)process(window), functionCall.getNullTreatment(),
            functionCall.getFilter(), functionCall.getOrderBy());
    }

    /**
     * maxpt function
     *
     * @param funcName  函数名
     * @param arguments 参数列表
     * @return FunctionCall
     */
    private Node maxPtFunction(QualifiedName funcName, boolean isDistinct, List<BaseExpression> arguments) {
        if (CollectionUtils.isEmpty(arguments)) {
            return new FunctionCall(funcName, isDistinct, arguments);
        }
        if (arguments.size() > 1) {
            return new FunctionCall(funcName, isDistinct, arguments);
        }
        BaseExpression baseExpression = arguments.get(0);
        if (!(baseExpression instanceof StringLiteral)) {
            return new FunctionCall(funcName, isDistinct, arguments);
        }
        StringLiteral stringLiteral = (StringLiteral)baseExpression;
        QualifiedName qualifiedName = QualifiedName.of(stringLiteral.getValue());
        return new FunctionCall(funcName, isDistinct, Lists.newArrayList(new StringLiteral(qualifiedName.getSuffix())));
    }

    private QualifiedName rewrite(QualifiedName funcName) {
        for (String key : functionMap.keySet()) {
            if (Objects.equals(funcName, QualifiedName.of(key))) {
                return QualifiedName.of(functionMap.get(key));
            }
        }
        return funcName;
    }

    private List<Hint> toHints(List<Hint> hints) {
        if (hints == null) {
            return null;
        }
        List<Hint> list = new ArrayList<>();
        for (Hint h : hints) {
            Hint hint = (Hint)process(h);
            list.add(hint);
        }
        return list;
    }

    @Override
    public Node visitJoin(Join join, Void context) {
        JoinCriteria criteria = join.getCriteria();
        BaseRelation left = join.getLeft();
        BaseRelation process = (BaseRelation)process(left);
        //if left is table, need add alias
        BaseRelation leftRelation = getBaseRelation(process);

        //if criteria
        JoinCriteria joinCriteria = null;
        if (criteria instanceof JoinOn) {
            JoinOn joinOn = (JoinOn)criteria;
            joinCriteria = new JoinOn((BaseExpression)process(joinOn.getExpression()));
        } else {
            joinCriteria = criteria;
        }
        BaseRelation right = (BaseRelation)process(join.getRight());
        BaseRelation rightRelation = getBaseRelation(right);
        return new Join(join.getJoinToken(), leftRelation, rightRelation, joinCriteria);
    }

    private BaseRelation getBaseRelation(BaseRelation relation) {
        if (relation instanceof Table) {
            subQueryDepth++;
            Identifier identifier = new Identifier(SUB_QUERY_PREFIX + subQueryDepth);
            BaseRelation baseRelation = new AliasedRelation(relation, identifier);
            subAliasMap.put(subQueryDepth, identifier);
            return baseRelation;
        }
        return relation;
    }

    @Override
    public Node visitAliasedRelation(AliasedRelation aliasedRelation, Void context) {
        return new AliasedRelation((BaseRelation)process(aliasedRelation.getRelation()), aliasedRelation.getAlias(),
            aliasedRelation.getColumnNames());
    }

    @Override
    public Node visitTable(Table node, Void indent) {
        //remove the project
        boolean joinPath = node.getName().isJoinPath();
        //如果仍然保留，那么直接返回
        if (hologresTransformContext.getQuerySetting().isKeepSchemaName()) {
            return node;
        }
        if (joinPath) {
            return new Table(node.getLocation(), QualifiedName.of(node.getName().getSuffix()));
        }
        return node;
    }

    @Override
    public Node visitNode(Node node, Void context) {
        return node;
    }

}
