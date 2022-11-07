/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.core.tree;

import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeParameter;
import com.aliyun.fastmodel.core.tree.datatype.Field;
import com.aliyun.fastmodel.core.tree.datatype.GenericDataType;
import com.aliyun.fastmodel.core.tree.datatype.NumericParameter;
import com.aliyun.fastmodel.core.tree.datatype.RowDataType;
import com.aliyun.fastmodel.core.tree.datatype.TypeParameter;
import com.aliyun.fastmodel.core.tree.expr.AllRows;
import com.aliyun.fastmodel.core.tree.expr.ArithmeticBinaryExpression;
import com.aliyun.fastmodel.core.tree.expr.ArithmeticUnaryExpression;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.BitOperationExpression;
import com.aliyun.fastmodel.core.tree.expr.ComparisonExpression;
import com.aliyun.fastmodel.core.tree.expr.CustomExpression;
import com.aliyun.fastmodel.core.tree.expr.DereferenceExpression;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.IsConditionExpression;
import com.aliyun.fastmodel.core.tree.expr.LogicalBinaryExpression;
import com.aliyun.fastmodel.core.tree.expr.Parameter;
import com.aliyun.fastmodel.core.tree.expr.Row;
import com.aliyun.fastmodel.core.tree.expr.atom.Cast;
import com.aliyun.fastmodel.core.tree.expr.atom.CoalesceExpression;
import com.aliyun.fastmodel.core.tree.expr.atom.ExistsPredicate;
import com.aliyun.fastmodel.core.tree.expr.atom.Extract;
import com.aliyun.fastmodel.core.tree.expr.atom.Floor;
import com.aliyun.fastmodel.core.tree.expr.atom.FunctionCall;
import com.aliyun.fastmodel.core.tree.expr.atom.GroupingOperation;
import com.aliyun.fastmodel.core.tree.expr.atom.IfExpression;
import com.aliyun.fastmodel.core.tree.expr.atom.InPredicate;
import com.aliyun.fastmodel.core.tree.expr.atom.IntervalExpression;
import com.aliyun.fastmodel.core.tree.expr.atom.ListExpression;
import com.aliyun.fastmodel.core.tree.expr.atom.SearchedCaseExpression;
import com.aliyun.fastmodel.core.tree.expr.atom.SimpleCaseExpression;
import com.aliyun.fastmodel.core.tree.expr.atom.SubQueryExpression;
import com.aliyun.fastmodel.core.tree.expr.atom.TableOrColumn;
import com.aliyun.fastmodel.core.tree.expr.atom.WhenClause;
import com.aliyun.fastmodel.core.tree.expr.literal.BaseLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.BooleanLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.CurrentDate;
import com.aliyun.fastmodel.core.tree.expr.literal.CurrentTimestamp;
import com.aliyun.fastmodel.core.tree.expr.literal.DateLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.DecimalLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.DoubleLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.EscapeStringLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.HexLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.IntervalLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.ListStringLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.LongLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.NullLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.TimestampLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.TimestampLocalTzLiteral;
import com.aliyun.fastmodel.core.tree.expr.similar.BetweenPredicate;
import com.aliyun.fastmodel.core.tree.expr.similar.InListExpression;
import com.aliyun.fastmodel.core.tree.expr.similar.LikePredicate;
import com.aliyun.fastmodel.core.tree.expr.similar.NotExpression;
import com.aliyun.fastmodel.core.tree.expr.window.FrameBound;
import com.aliyun.fastmodel.core.tree.expr.window.Window;
import com.aliyun.fastmodel.core.tree.expr.window.WindowFrame;
import com.aliyun.fastmodel.core.tree.relation.AliasedRelation;
import com.aliyun.fastmodel.core.tree.relation.Join;
import com.aliyun.fastmodel.core.tree.relation.Lateral;
import com.aliyun.fastmodel.core.tree.relation.SampledRelation;
import com.aliyun.fastmodel.core.tree.relation.querybody.BaseQueryBody;
import com.aliyun.fastmodel.core.tree.relation.querybody.Except;
import com.aliyun.fastmodel.core.tree.relation.querybody.Intersect;
import com.aliyun.fastmodel.core.tree.relation.querybody.QuerySpecification;
import com.aliyun.fastmodel.core.tree.relation.querybody.SetOperation;
import com.aliyun.fastmodel.core.tree.relation.querybody.Table;
import com.aliyun.fastmodel.core.tree.relation.querybody.TableSubQuery;
import com.aliyun.fastmodel.core.tree.relation.querybody.Union;
import com.aliyun.fastmodel.core.tree.relation.querybody.Values;
import com.aliyun.fastmodel.core.tree.statement.CompositeStatement;
import com.aliyun.fastmodel.core.tree.statement.misc.Call;
import com.aliyun.fastmodel.core.tree.statement.misc.EmptyStatement;
import com.aliyun.fastmodel.core.tree.statement.misc.Use;
import com.aliyun.fastmodel.core.tree.statement.select.Hint;
import com.aliyun.fastmodel.core.tree.statement.select.Limit;
import com.aliyun.fastmodel.core.tree.statement.select.Offset;
import com.aliyun.fastmodel.core.tree.statement.select.Query;
import com.aliyun.fastmodel.core.tree.statement.select.RowToCol;
import com.aliyun.fastmodel.core.tree.statement.select.Select;
import com.aliyun.fastmodel.core.tree.statement.select.With;
import com.aliyun.fastmodel.core.tree.statement.select.WithQuery;
import com.aliyun.fastmodel.core.tree.statement.select.groupby.Cube;
import com.aliyun.fastmodel.core.tree.statement.select.groupby.GroupBy;
import com.aliyun.fastmodel.core.tree.statement.select.groupby.GroupingElement;
import com.aliyun.fastmodel.core.tree.statement.select.groupby.GroupingSets;
import com.aliyun.fastmodel.core.tree.statement.select.groupby.Rollup;
import com.aliyun.fastmodel.core.tree.statement.select.groupby.SimpleGroupBy;
import com.aliyun.fastmodel.core.tree.statement.select.item.AllColumns;
import com.aliyun.fastmodel.core.tree.statement.select.item.SelectItem;
import com.aliyun.fastmodel.core.tree.statement.select.item.SingleColumn;
import com.aliyun.fastmodel.core.tree.statement.select.order.OrderBy;
import com.aliyun.fastmodel.core.tree.statement.select.order.SortItem;
import com.aliyun.fastmodel.core.tree.statement.select.sort.ClusterBy;
import com.aliyun.fastmodel.core.tree.statement.select.sort.DistributeBy;
import com.aliyun.fastmodel.core.tree.statement.select.sort.SortBy;
import com.aliyun.fastmodel.core.tree.statement.show.ConditionElement;
import com.aliyun.fastmodel.core.tree.statement.show.LikeCondition;
import com.aliyun.fastmodel.core.tree.statement.show.WhereCondition;

/**
 * AST visitor interface
 *
 * @author panguanjing
 * @date 2022/6/7
 */
public interface IAstVisitor<R, C> {
    /**
     * 处理节点，提供方便的方法
     *
     * @param node
     * @return
     */
    default R process(Node node) {
        return process(node, null);
    }

    /**
     * 处理节点
     *
     * @param node
     * @param context
     * @return
     */
    default R process(Node node, C context) {
        if (node == null) {
            return null;
        }
        return node.accept(this, context);
    }

    /**
     * visit Node
     *
     * @param node    Node
     * @param context context
     * @return R
     */
    default R visitNode(Node node, C context) {
        return null;
    }

    /**
     * visit comment
     *
     * @param comment comment
     * @param context context
     * @return R
     */
    default R visitComment(Comment comment, C context) {
        return visitNode(comment, context);
    }

    /**
     * visit property
     *
     * @param property property
     * @param context  context
     * @return R
     */
    default R visitProperty(Property property, C context) {
        return visitNode(property, context);
    }

    /**
     * visit qualified Name
     *
     * @param qualifiedName
     * @param context
     * @return
     */
    default R visitQualifiedName(QualifiedName qualifiedName, C context) {
        return visitNode(qualifiedName, context);
    }

    /**
     * visit alias name
     *
     * @param aliasedName
     * @param context
     * @return
     */
    default R visitAliasedName(AliasedName aliasedName, C context) {
        return visitNode(aliasedName, context);
    }

    /**
     * visitRelation
     *
     * @param baseRelation 关系对象
     * @param context      上下文
     * @return R
     */
    default R visitRelation(BaseRelation baseRelation, C context) {
        return visitNode(baseRelation, context);
    }

    /**
     * visit list node
     *
     * @param listNode
     * @param context
     * @return
     */
    default R visitListNode(ListNode listNode, C context) {
        return visitNode(listNode, context);
    }

    /**
     * visit expression
     *
     * @param expression expression
     * @param context    context
     * @return R
     */
    default R visitExpression(BaseExpression expression, C context) {
        return visitNode(expression, context);
    }

    /**
     * visit type Parameter
     *
     * @param typeParameter 类型参数
     * @param context       上下文
     * @return R
     */
    default R visitTypeParameter(TypeParameter typeParameter, C context) {
        return visitDataTypeParameter(typeParameter, context);
    }

    /**
     * visit identifier
     *
     * @param identifier 标识符
     * @param context    context
     * @return R
     */
    default R visitIdentifier(Identifier identifier, C context) {
        return visitExpression(identifier, context);
    }

    /**
     * visit genericDataType
     *
     * @param genericDataType genericDataType
     * @param context         Context
     * @return R
     */
    default R visitGenericDataType(GenericDataType genericDataType, C context) {
        return visitDataType(genericDataType, context);
    }

    /**
     * visit dataType
     *
     * @param baseDataType baseDataType
     * @param context      context
     * @return R
     */
    default R visitDataType(BaseDataType baseDataType, C context) {
        return visitExpression(baseDataType, context);
    }

    /**
     * visit Num
     *
     * @param numericParameter 数字参数
     * @param context          上下文
     * @return R
     */
    default R visitNumericTypeParameter(NumericParameter numericParameter, C context) {
        return visitDataTypeParameter(numericParameter, context);
    }

    /**
     * visit Row data Type
     *
     * @param rowDataType 行类型
     * @param context     上下文
     * @return R
     */
    default R visitRowDataType(RowDataType rowDataType, C context) {
        return visitDataType(rowDataType, context);
    }

    /**
     * visitSetOperation
     *
     * @param setOperation 设置操作
     * @param context      上下文
     * @return R
     */
    default R visitSetOperation(SetOperation setOperation, C context) {
        return visitQueryBody(setOperation, context);
    }

    /**
     * visit table
     *
     * @param table
     * @param context
     * @return
     */
    default R visitTable(Table table, C context) {
        return visitQueryBody(table, context);
    }

    /**
     * visit query
     *
     * @param query
     * @param context
     * @return
     */
    default R visitQuery(Query query, C context) {
        return visitStatement(query, context);
    }

    /**
     * visit union
     *
     * @param union
     * @param context
     * @return
     */
    default R visitUnion(Union union, C context) {
        return visitSetOperation(union, context);
    }

    /**
     * visit values
     *
     * @param values
     * @param context
     * @return
     */
    default R visitValues(Values values, C context) {
        return visitQueryBody(values, context);
    }

    /**
     * visit dataTypeParameter
     *
     * @param dataTypeParameter dataTypeParameter
     * @param context           context
     * @return R
     */
    default R visitDataTypeParameter(DataTypeParameter dataTypeParameter, C context) {
        return visitNode(dataTypeParameter, context);
    }

    /**
     * visit Row Field
     *
     * @param field
     * @param context
     * @return R
     */
    default R visitRowField(Field field, C context) {
        return visitNode(field, context);
    }

    /**
     * visit QueryBody
     *
     * @param queryBody 查询对象
     * @param context   上下文
     * @return R
     */
    default R visitQueryBody(BaseQueryBody queryBody, C context) {
        return visitRelation(queryBody, context);
    }

    /**
     * visit Sort Item
     *
     * @param sortItem 排序
     * @param context  上下文
     * @return R
     */
    default R visitSortItem(SortItem sortItem, C context) {
        return visitNode(sortItem, context);
    }

    /**
     * visit OrderBy
     *
     * @param orderBy 排序
     * @param context 上下文
     * @return R
     */
    default R visitOrderBy(OrderBy orderBy, C context) {
        return visitNode(orderBy, context);
    }

    /**
     * visit Grouping Element
     *
     * @param groupingElement 排序元素
     * @param context         上下文
     * @return R
     */
    default R visitGroupingElement(GroupingElement groupingElement, C context) {
        return visitNode(groupingElement, context);
    }

    /**
     * visit limit
     *
     * @param limit   限制内容
     * @param context 上下文
     * @return R
     */
    default R visitLimit(Limit limit, C context) {
        return visitNode(limit, context);
    }

    /**
     * 偏移量
     *
     * @param offset  偏移量
     * @param context 上下文
     * @return R
     */
    default R visitOffset(Offset offset, C context) {
        return visitNode(offset, context);
    }

    /**
     * visit join
     *
     * @param join
     * @param context
     * @return
     */
    default R visitJoin(Join join, C context) {
        return visitRelation(join, context);
    }

    /**
     * visit selectItem
     *
     * @param selectItem selectItem
     * @param context    context
     * @return R
     */
    default R visitSelectItem(SelectItem selectItem, C context) {
        return visitNode(selectItem, context);
    }

    /**
     * visit alias relation
     *
     * @param aliasedRelation alias relation
     * @param context         context
     * @return R
     */
    default R visitAliasedRelation(AliasedRelation aliasedRelation, C context) {
        return visitRelation(aliasedRelation, context);
    }

    /**
     * visit statement
     *
     * @param abstractStatement
     * @param context
     * @return
     */
    default R visitStatement(BaseStatement abstractStatement, C context) {
        return visitNode(abstractStatement, context);
    }

    /**
     * visit composite Statement
     *
     * @param compositeStatement composite
     * @param context            context
     * @return R
     */
    default R visitCompositeStatement(CompositeStatement compositeStatement, C context) {
        return visitStatement(compositeStatement, context);
    }

    /**
     * empty statement
     * example : ;
     *
     * @param emptyStatement
     * @param context
     * @return
     */
    default R visitEmptyStatement(EmptyStatement emptyStatement, C context) {
        return visitStatement(emptyStatement, context);
    }

    /**
     * visit row to col
     *
     * @param rowToCol
     * @param context
     * @return
     */
    default R visitRowToCol(RowToCol rowToCol, C context) {
        return visitStatement(rowToCol, context);
    }

    /**
     * use
     *
     * @param use
     * @param context
     * @return
     */
    default R visitUse(Use use, C context) {
        return visitStatement(use, context);
    }

    /**
     * visit call
     *
     * @param call
     * @param context
     * @return
     */
    default R visitCall(Call call, C context) {
        return visitStatement(call, context);
    }

    /**
     * visit single column
     *
     * @param singleColumn
     * @param context
     * @return
     */
    default R visitSingleColumn(SingleColumn singleColumn, C context) {
        return visitSelectItem(singleColumn, context);
    }

    /**
     * visit all columns
     *
     * @param allColumns all columns expr
     * @param context    context
     * @return R
     */
    default R visitAllColumns(AllColumns allColumns, C context) {
        return visitSelectItem(allColumns, context);
    }

    /**
     * visit table sub query
     *
     * @param tableSubQuery table sub query
     * @param context       context
     * @return R
     */
    default R visitTableSubQuery(TableSubQuery tableSubQuery, C context) {
        return visitQueryBody(tableSubQuery, context);
    }

    /**
     * visit with
     *
     * @param with    with expr
     * @param context context
     * @return R
     */
    default R visitWith(With with, C context) {
        return visitNode(with, context);
    }

    /**
     * visit query spec
     *
     * @param querySpecification query spec
     * @param context            context
     * @return R
     */
    default R visitQuerySpecification(QuerySpecification querySpecification, C context) {
        return visitQueryBody(querySpecification, context);
    }

    /**
     * visit simple group by
     *
     * @param simpleGroupBy expr
     * @param context       context
     * @return R
     */
    default R visitSimpleGroupBy(SimpleGroupBy simpleGroupBy, C context) {
        return visitGroupingElement(simpleGroupBy, context);
    }

    /**
     * visit group by
     *
     * @param groupBy expr
     * @param context context
     * @return R
     */
    default R visitGroupBy(GroupBy groupBy, C context) {
        return visitNode(groupBy, context);
    }

    /**
     * visit lateral
     *
     * @param lateral lateral
     * @param context context
     * @return R
     */
    default R visitLateral(Lateral lateral, C context) {
        return visitRelation(lateral, context);
    }

    /**
     * visit cube
     *
     * @param cube    cube
     * @param context context
     * @return R
     */
    default R visitCube(Cube cube, C context) {
        return visitGroupingElement(cube, context);
    }

    /**
     * visit rollup
     *
     * @param rollup  rollup
     * @param context context
     * @return R
     */
    default R visitRollup(Rollup rollup, C context) {
        return visitGroupingElement(rollup, context);
    }

    /**
     * visit groupingSets
     *
     * @param groupingSets group
     * @param context      context
     * @return R
     */
    default R visitGroupingSets(GroupingSets groupingSets, C context) {
        return visitGroupingElement(groupingSets, context);
    }

    /**
     * visit sample Relation
     *
     * @param sampledRelation sampleRelation
     * @param context         context
     * @return R
     */
    default R visitSampledRelation(SampledRelation sampledRelation, C context) {
        return visitRelation(sampledRelation, context);
    }

    /**
     * visit Parameter
     *
     * @param parameter parameter
     * @param context   context
     * @return R
     */
    default R visitParameter(Parameter parameter, C context) {
        return visitExpression(parameter, context);
    }

    /**
     * visit select
     *
     * @param select
     * @param context
     * @return
     */
    default R visitSelect(Select select, C context) {
        return visitNode(select, context);
    }

    /**
     * visit timestamp local
     *
     * @param timestampLocalTzLiteral
     * @param context
     * @return
     */
    default R visitTimestampLocalTzLiteral(TimestampLocalTzLiteral timestampLocalTzLiteral,
                                           C context) {
        return visitExpression(timestampLocalTzLiteral, context);
    }

    /**
     * visit subquery expression
     *
     * @param subQueryExpression
     * @param context
     * @return
     */
    default R visitSubQueryExpression(SubQueryExpression subQueryExpression, C context) {
        return visitExpression(subQueryExpression, context);
    }

    /**
     * visit exisit predicate
     *
     * @param existsPredicate
     * @param context
     * @return
     */
    default R visitExistsPredicate(ExistsPredicate existsPredicate, C context) {
        return visitExpression(existsPredicate, context);
    }

    /**
     * visit window
     *
     * @param context
     * @param window
     * @return
     */
    default R visitWindow(Window window, C context) {
        return visitNode(window, context);
    }

    /**
     * visit window frame
     *
     * @param windowFrame
     * @param context
     * @return
     */
    default R visitWindowFrame(WindowFrame windowFrame, C context) {
        return visitNode(windowFrame, context);
    }

    /**
     * visit frame bound
     *
     * @param frameBound
     * @param context
     * @return
     */
    default R visitFrameBound(FrameBound frameBound, C context) {
        return visitNode(frameBound, context);
    }

    /**
     * grouping operation
     *
     * @param groupingOperation
     * @param context
     * @return
     */
    default R visitGroupingOperation(GroupingOperation groupingOperation, C context) {
        return visitExpression(groupingOperation, context);
    }

    /**
     * condition visit
     *
     * @param conditionElement
     * @param context
     * @return
     */
    default R visitConditionElement(ConditionElement conditionElement, C context) {
        return visitNode(conditionElement, context);
    }

    /**
     * like condition
     *
     * @param likeCondition
     * @param context
     * @return
     */
    default R visitLikeCondition(LikeCondition likeCondition, C context) {
        return visitConditionElement(likeCondition, context);
    }

    /**
     * visit hint
     *
     * @param hint    Hint
     * @param context Context
     * @return R
     */
    default R visitHint(Hint hint, C context) {
        return visitNode(hint, context);
    }

    /**
     * visit with query
     *
     * @param withQuery
     * @param context
     * @return
     */
    default R visitWithQuery(WithQuery withQuery, C context) {
        return visitNode(withQuery, context);
    }

    /**
     * visit logical binary expr
     *
     * @param logicalBinaryExpression logical expr
     * @param context                 context
     * @return R
     */
    default R visitLogicalBinaryExpression(LogicalBinaryExpression logicalBinaryExpression, C context) {
        return visitExpression(logicalBinaryExpression, context);
    }

    /**
     * visit searched case expr
     *
     * @param searchedCaseExpression searched case expr
     * @param context                context
     * @return R
     */
    default R visitSearchedCaseExpression(SearchedCaseExpression searchedCaseExpression, C context) {
        return visitExpression(searchedCaseExpression, context);
    }

    /**
     * visit dereference expression
     *
     * @param dereferenceExpression expr
     * @param context               context
     * @return R
     */
    default R visitDereferenceExpression(DereferenceExpression dereferenceExpression, C context) {
        return visitExpression(dereferenceExpression, context);
    }

    /**
     * visit if expression
     *
     * @param ifExpression if
     * @param context      context
     * @return R
     */
    default R visitIfExpression(IfExpression ifExpression, C context) {
        return visitExpression(ifExpression, context);
    }

    /**
     * visit coalesce expr
     *
     * @param coalesceExpression expr
     * @param context            context
     * @return R
     */
    default R visitCoalesceExpression(CoalesceExpression coalesceExpression, C context) {
        return visitExpression(coalesceExpression, context);
    }

    /**
     * visit except
     *
     * @param except  except
     * @param context 上下文
     * @return R
     */
    default R visitExcept(Except except, C context) {
        return visitSetOperation(except, context);
    }

    /**
     * visit row
     *
     * @param row     Row
     * @param context context
     * @return R
     */
    default R visitRow(Row row, C context) {
        return visitExpression(row, context);
    }

    /**
     * visit current date
     *
     * @param currentDate currentDate
     * @param context     context
     * @return R
     */
    default R visitCurrentDate(CurrentDate currentDate, C context) {
        return visitExpression(currentDate, context);
    }

    /**
     * visit current timestamps
     *
     * @param currentTimestamp currentTimeStamp
     * @param context          context
     * @return R
     */
    default R visitCurrentTimestamp(CurrentTimestamp currentTimestamp, C context) {
        return visitExpression(currentTimestamp, context);
    }

    /**
     * visit Bit Operation Expression
     *
     * @param bitOperationExpression bitOperationExpression
     * @param context                context
     * @return R
     */
    default R visitBitOperationExpression(BitOperationExpression bitOperationExpression,
                                          C context) {
        return visitExpression(bitOperationExpression, context);
    }

    /**
     * visit tableOrColumn
     *
     * @param tableOrColumn table Or Column
     * @param context       context
     * @return R
     */
    default R visitTableOrColumn(TableOrColumn tableOrColumn, C context) {
        return visitExpression(tableOrColumn, context);
    }

    /**
     * visit allRows
     *
     * @param allRows allRows
     * @param context context
     * @return R
     */
    default R visitAllRows(AllRows allRows, C context) {
        return visitExpression(allRows, context);
    }

    /**
     * visitTimestampLiteral
     *
     * @param timestampLiteral
     * @param context
     * @return
     */
    default R visitTimestampLiteral(TimestampLiteral timestampLiteral, C context) {
        return visitLiteral(timestampLiteral, context);
    }

    /**
     * visit is condition
     *
     * @param isConditionExpression
     * @param context
     * @return
     */
    default R visitIsConditionExpression(IsConditionExpression isConditionExpression, C context) {
        return visitExpression(isConditionExpression, context);
    }

    /**
     * visit like predicate
     *
     * @param likePredicate
     * @param context
     * @return
     */
    default R visitLikePredicate(LikePredicate likePredicate, C context) {
        return visitExpression(likePredicate, context);
    }

    /**
     * visit custom expression
     *
     * @param customExpression
     * @param context
     * @return
     */
    default R visitCustomExpression(CustomExpression customExpression, C context) {
        return visitExpression(customExpression, context);
    }

    /**
     * 字面量
     *
     * @param baseLiteral 字面量
     * @param context     上下文
     * @return R
     */
    default R visitLiteral(BaseLiteral baseLiteral, C context) {
        return visitExpression(baseLiteral, context);
    }

    /**
     * 布尔字面量
     *
     * @param booleanLiteral 布尔字面量
     * @param context        上下文
     * @return R
     */
    default R visitBooleanLiteral(BooleanLiteral booleanLiteral, C context) {
        return visitLiteral(booleanLiteral, context);
    }

    /**
     * visit long literal
     *
     * @param longLiteral LongLiteral
     * @param context     context
     * @return R
     */
    default R visitLongLiteral(LongLiteral longLiteral, C context) {
        return visitLiteral(longLiteral, context);
    }

    /**
     * visit string literal
     *
     * @param stringLiteral 字符串
     * @param context       上下文
     * @return R
     */
    default R visitStringLiteral(StringLiteral stringLiteral, C context) {
        return visitLiteral(stringLiteral, context);
    }

    /**
     * visit double literal
     *
     * @param doubleLiteral 双精度浮点型
     * @param context       上下文天
     * @return R
     */
    default R visitDoubleLiteral(DoubleLiteral doubleLiteral, C context) {
        return visitLiteral(doubleLiteral, context);
    }

    /**
     * visit date 字变量
     *
     * @param dateLiteral date
     * @param context     context
     * @return R
     */
    default R visitDateLiteral(DateLiteral dateLiteral, C context) {
        return visitLiteral(dateLiteral, context);
    }

    /**
     * visit interval literal
     *
     * @param intervalLiteral interval literal
     * @param context         context
     * @return R
     */
    default R visitIntervalLiteral(IntervalLiteral intervalLiteral, C context) {
        return visitLiteral(intervalLiteral, context);
    }

    /**
     * visit null literal
     *
     * @param nullLiteral null literal
     * @param context     context
     * @return R
     */
    default R visitNullLiteral(NullLiteral nullLiteral, C context) {
        return visitLiteral(nullLiteral, context);
    }

    /**
     * visit number literal
     *
     * @param decimalLiteral number literal
     * @param context        context
     * @return R
     */
    default R visitDecimalLiteral(DecimalLiteral decimalLiteral, C context) {
        return visitLiteral(decimalLiteral, context);
    }

    /**
     * visit list string literal
     *
     * @param listStringLiteral list string literal
     * @param context           context
     * @return R
     */
    default R visitListStringLiteral(ListStringLiteral listStringLiteral, C context) {
        return visitLiteral(listStringLiteral, context);
    }

    /**
     * visit between predicate
     *
     * @param betweenPredicate between predicate
     * @param context          context
     * @return R
     */
    default R visitBetweenPredicate(BetweenPredicate betweenPredicate, C context) {
        return visitExpression(betweenPredicate, context);
    }

    /**
     * visit in List Expression
     *
     * @param inListExpression 在列表中
     * @param context          上下文
     * @return R
     */
    default R visitInListExpression(InListExpression inListExpression, C context) {
        return visitExpression(inListExpression, context);
    }

    /**
     * visit not expression
     *
     * @param notExpression not expression
     * @param context       context
     * @return R
     */
    default R visitNotExpression(NotExpression notExpression, C context) {
        return visitExpression(notExpression, context);
    }

    /**
     * visit comparison expression
     *
     * @param comparisonExpression comparison expression
     * @param context              context
     * @return R
     */
    default R visitComparisonExpression(ComparisonExpression comparisonExpression, C context) {
        return visitExpression(comparisonExpression, context);
    }

    /**
     * visit arithmeticBinaryExpression expression
     *
     * @param arithmeticBinaryExpression arithmeticBinaryExpression
     * @param context                    context
     * @return R
     */
    default R visitArithmeticBinaryExpression(ArithmeticBinaryExpression arithmeticBinaryExpression, C context) {
        return visitExpression(arithmeticBinaryExpression, context);
    }

    /**
     * visit arithmeticUnaryExpression
     *
     * @param arithmeticUnaryExpression arithmeticUnaryExpression
     * @param context                   context
     * @return R
     */
    default R visitArithmeticUnaryExpression(ArithmeticUnaryExpression arithmeticUnaryExpression, C context) {
        return visitExpression(arithmeticUnaryExpression, context);
    }

    /**
     * visit cast
     *
     * @param cast    cast
     * @param context context
     * @return R
     */
    default R visitCast(Cast cast, C context) {
        return visitExpression(cast, context);
    }

    /**
     * visit extract
     *
     * @param extract extract
     * @param context context
     * @return R
     */
    default R visitExtract(Extract extract, C context) {
        return visitExpression(extract, context);
    }

    /**
     * visit function call
     *
     * @param functionCall functionCall
     * @param context      context
     * @return R
     */
    default R visitFunctionCall(FunctionCall functionCall, C context) {
        return visitExpression(functionCall, context);
    }

    /**
     * visit list expr
     *
     * @param listExpression
     * @param context
     * @return
     */
    default R visitListExpr(ListExpression listExpression, C context) {
        return visitExpression(listExpression, context);
    }

    /**
     * visit interval expr
     *
     * @param intervalExpression
     * @param context
     * @return
     */
    default R visitIntervalExpr(IntervalExpression intervalExpression, C context) {
        return visitExpression(intervalExpression, context);
    }

    /**
     * visit floor expr
     *
     * @param floor
     * @param context
     * @return
     */
    default R visitFloorExpr(Floor floor, C context) {
        return visitExpression(floor, context);
    }

    /**
     * visit when clause
     *
     * @param whenClause
     * @param context
     * @return
     */
    default R visitWhenClause(WhenClause whenClause, C context) {
        return visitExpression(whenClause, context);
    }

    /**
     * visit simple case
     *
     * @param simpleCaseExpression
     * @param context
     * @return
     */
    default R visitSimpleCaseExpression(SimpleCaseExpression simpleCaseExpression, C context) {
        return visitExpression(simpleCaseExpression, context);
    }

    /**
     * visit in predicate
     *
     * @param inPredicate
     * @param context
     * @return
     */
    default R visitInPredicate(InPredicate inPredicate, C context) {
        return visitExpression(inPredicate, context);
    }

    /**
     * visit intersect
     *
     * @param intersect intersect
     * @param context   context
     * @return R
     */
    default R visitIntersect(Intersect intersect, C context) {
        return visitSetOperation(intersect, context);
    }

    /**
     * visit where condition
     *
     * @param whereCondition
     * @param context
     * @return
     */
    default R visitWhereCondition(WhereCondition whereCondition, C context) {
        return visitConditionElement(whereCondition, context);
    }

    /**
     * visit escape string literal
     *
     * @param escapeStringLiteral
     * @param context
     * @return
     */
    default R visitEscapeStringLiteral(EscapeStringLiteral escapeStringLiteral, C context) {
        return visitStringLiteral(escapeStringLiteral, context);
    }

    /**
     * visit sort by
     *
     * @param sortBy
     * @param context
     * @return
     */
    default R visitSortBy(SortBy sortBy, C context) {
        return visitNode(sortBy, context);
    }

    /**
     * visit distribute by
     *
     * @param distributeBy
     * @param context
     * @return
     */
    default R visitDistributeBy(DistributeBy distributeBy, C context) {
        return visitNode(distributeBy, context);
    }

    /**
     * visit cluster by
     *
     * @param clusterBy
     * @param context
     * @return
     */
    default R visitClusterBy(ClusterBy clusterBy, C context) {
        return visitNode(clusterBy, context);
    }

    /**
     * hexLiteral
     *
     * @param hexLiteral
     * @param context
     * @return
     */
    default R visitHexLiteral(HexLiteral hexLiteral, C context) {
        return visitLiteral(hexLiteral, context);
    }

}
