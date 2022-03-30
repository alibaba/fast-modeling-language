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

package com.aliyun.fastmodel.core.formatter;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.datatype.Field;
import com.aliyun.fastmodel.core.tree.datatype.GenericDataType;
import com.aliyun.fastmodel.core.tree.datatype.NumericParameter;
import com.aliyun.fastmodel.core.tree.datatype.RowDataType;
import com.aliyun.fastmodel.core.tree.datatype.TypeParameter;
import com.aliyun.fastmodel.core.tree.expr.AllRows;
import com.aliyun.fastmodel.core.tree.expr.ArithmeticBinaryExpression;
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
import com.aliyun.fastmodel.core.tree.expr.atom.ExistsPredicate;
import com.aliyun.fastmodel.core.tree.expr.atom.Extract;
import com.aliyun.fastmodel.core.tree.expr.atom.Floor;
import com.aliyun.fastmodel.core.tree.expr.atom.FunctionCall;
import com.aliyun.fastmodel.core.tree.expr.atom.IfExpression;
import com.aliyun.fastmodel.core.tree.expr.atom.InPredicate;
import com.aliyun.fastmodel.core.tree.expr.atom.IntervalExpression;
import com.aliyun.fastmodel.core.tree.expr.atom.ListExpression;
import com.aliyun.fastmodel.core.tree.expr.atom.SearchedCaseExpression;
import com.aliyun.fastmodel.core.tree.expr.atom.SimpleCaseExpression;
import com.aliyun.fastmodel.core.tree.expr.atom.SubQueryExpression;
import com.aliyun.fastmodel.core.tree.expr.atom.TableOrColumn;
import com.aliyun.fastmodel.core.tree.expr.atom.WhenClause;
import com.aliyun.fastmodel.core.tree.expr.atom.datetime.DateTimeAddEndExpression;
import com.aliyun.fastmodel.core.tree.expr.atom.datetime.DateTimeAddExpression;
import com.aliyun.fastmodel.core.tree.expr.atom.datetime.DateTimeAddStartExpression;
import com.aliyun.fastmodel.core.tree.expr.enums.LikeCondition;
import com.aliyun.fastmodel.core.tree.expr.enums.NullTreatment;
import com.aliyun.fastmodel.core.tree.expr.enums.VarType;
import com.aliyun.fastmodel.core.tree.expr.literal.BooleanLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.DateLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.DecimalLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.DoubleLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.IntervalLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.ListStringLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.LongLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.NullLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.TimestampLiteral;
import com.aliyun.fastmodel.core.tree.expr.similar.BetweenPredicate;
import com.aliyun.fastmodel.core.tree.expr.similar.InListExpression;
import com.aliyun.fastmodel.core.tree.expr.similar.LikePredicate;
import com.aliyun.fastmodel.core.tree.expr.similar.NotExpression;
import com.aliyun.fastmodel.core.tree.expr.window.FrameBound;
import com.aliyun.fastmodel.core.tree.expr.window.Window;
import com.aliyun.fastmodel.core.tree.expr.window.WindowFrame;
import com.aliyun.fastmodel.core.tree.statement.rule.strategy.VolInterval;
import com.aliyun.fastmodel.core.tree.statement.select.item.AllColumns;
import com.aliyun.fastmodel.core.tree.statement.select.order.OrderBy;
import com.aliyun.fastmodel.core.tree.statement.select.order.SortItem;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

/**
 * 表达式visitor
 *
 * @author panguanjing
 * @date 2021/4/13
 */
public class ExpressionVisitor extends AstVisitor<String, Void> {
    public static final String COUNT = "count";

    private static final ThreadLocal<DecimalFormat> DOUBLE_FORMATTER = ThreadLocal.withInitial(
        () -> new DecimalFormat("0.###################E0###", new DecimalFormatSymbols(Locale.US)));

    @Override
    public String visitExpression(BaseExpression expression, Void context) {
        throw new UnsupportedOperationException(
            format("not yet implemented: %s.visit%s", getClass().getName(), expression.getClass().getSimpleName()));
    }

    @Override
    public String visitFunctionCall(FunctionCall node, Void context) {
        StringBuilder builder = new StringBuilder();

        String arguments = joinExpressions(node.getArguments());
        if (node.getArguments().isEmpty() && COUNT.equalsIgnoreCase(node.getFuncName().getSuffix())) {
            arguments = "*";
        }
        if (node.isDistinct()) {
            arguments = "DISTINCT " + arguments;
        }

        builder.append(formatName(node.getFuncName()))
            .append('(').append(arguments);

        if (node.getOrderBy() != null) {
            builder.append(' ').append(formatOrderBy(node.getOrderBy()));
        }
        builder.append(')');

        NullTreatment nullTreatment = node.getNullTreatment();
        if (nullTreatment != null) {
            if (nullTreatment == NullTreatment.IGNORE) {
                builder.append(" IGNORE NULLS");
            }
            if (nullTreatment == NullTreatment.RESPECT) {
                builder.append(" RESPECT NULLS");
            }
        }

        if (node.getFilter() != null) {
            builder.append(" FILTER ").append(visitFilter(node.getFilter(), context));
        }

        if (node.getWindow() != null) {
            builder.append(" OVER ").append(visitWindow(node.getWindow(), context));
        }

        return builder.toString();
    }

    protected String formatName(QualifiedName name) {
        return name.getOriginalParts().stream()
            .map(this::process)
            .collect(joining("."));
    }

    private String visitFilter(BaseExpression node, Void context) {
        return append("WHERE " + process(node, context), node.isParenthesized());
    }

    @Override
    public String visitWindow(Window node, Void context) {
        List<String> parts = new ArrayList<>();

        if (!node.getPartitionBy().isEmpty()) {
            parts.add("PARTITION BY " + joinExpressions(node.getPartitionBy()));
        }
        if (node.getOrderBy() != null) {
            parts.add(formatOrderBy(node.getOrderBy()));
        }
        if (node.getFrame() != null) {
            parts.add(process(node.getFrame(), context));
        }

        return '(' + Joiner.on(' ').join(parts) + ')';
    }

    @Override
    public String visitWindowFrame(WindowFrame node, Void context) {
        StringBuilder builder = new StringBuilder();

        builder.append(node.getType().toString()).append(' ');

        if (node.getEnd() != null) {
            builder.append("BETWEEN ")
                .append(process(node.getStart(), context))
                .append(" AND ")
                .append(process(node.getEnd(), context));
        } else {
            builder.append(process(node.getStart(), context));
        }

        return builder.toString();
    }

    @Override
    public String visitFrameBound(FrameBound frameBound, Void context) {
        switch (frameBound.getType()) {
            case UNBOUNDED_PRECEDING:
                return "UNBOUNDED PRECEDING";
            case PRECEDING:
                return process(frameBound.getValue(), context) + " PRECEDING";
            case CURRENT_ROW:
                return "CURRENT ROW";
            case FOLLOWING:
                return process(frameBound.getValue(), context) + " FOLLOWING";
            case UNBOUNDED_FOLLOWING:
                return "UNBOUNDED FOLLOWING";
            default:
                break;
        }
        throw new IllegalArgumentException("unhandled type: " + frameBound.getType());
    }

    @Override
    public String visitInListExpression(InListExpression node, Void context) {
        return process(node.getLeft(), context) + " IN " + "(" + joinExpressions(node.getInListExpr()) + ")";
    }

    @Override
    public String visitCast(Cast node, Void context) {
        return "CAST" +
            "(" + process(node.getExpression(), context) + " AS " + process(node.getDataType(), context) + ")";
    }

    @Override
    public String visitSearchedCaseExpression(SearchedCaseExpression node, Void context) {
        ImmutableList.Builder<String> parts = ImmutableList.builder();
        parts.add("CASE");
        for (WhenClause whenClause : node.getWhenClauseList()) {
            parts.add(process(whenClause, context));
        }

        BaseExpression baseExpression = node.getDefaultValue();
        if (baseExpression != null) {
            parts.add("ELSE").add(process(baseExpression, context));
        }
        parts.add("END");

        return "(" + Joiner.on(' ').join(parts.build()) + ")";
    }

    @Override
    public String visitNullLiteral(NullLiteral node, Void context) {
        return "null";
    }

    @Override
    public String visitParameter(Parameter node, Void context) {
        return "?";
    }

    @Override
    public String visitAllRows(AllRows node, Void context) {
        return "ALL";
    }

    @Override
    public String visitDoubleLiteral(DoubleLiteral doubleLiteral, Void context) {
        return DOUBLE_FORMATTER.get().format(doubleLiteral.getValue());

    }

    @Override
    public String visitGenericDataType(GenericDataType node, Void context) {
        StringBuilder result = new StringBuilder();
        DataTypeEnums typeName = node.getTypeName();
        if (typeName == DataTypeEnums.CUSTOM) {
            String format = getCustomDataTypeFormat(node);
            result.append(format);
        } else {
            result.append(StringUtils.upperCase(node.getName().getValue()));
        }
        boolean argNotEmpty = node.getArguments() != null && !node.getArguments().isEmpty();
        if (typeName == DataTypeEnums.ARRAY || typeName == DataTypeEnums.MAP
            || typeName == DataTypeEnums.STRUCT) {
            if (argNotEmpty) {
                result.append(node.getArguments().stream()
                    .map(this::process)
                    .collect(Collectors.joining(", ", "<", ">")));
            }
        } else {
            if (argNotEmpty) {
                result.append(node.getArguments().stream()
                    .map(this::process)
                    .collect(Collectors.joining(", ", "(", ")")));
            }
        }
        return result.toString();
    }

    /**
     * 提供自定义样式的格式的展示格式化处理
     * @param node
     * @return
     */
    protected String getCustomDataTypeFormat(GenericDataType node) {
        return format("%s('%s')", DataTypeEnums.CUSTOM.name(), node.getName().getValue());
    }

    @Override
    public String visitRowDataType(RowDataType rowDataType, Void context) {
        StringBuilder builder = new StringBuilder();
        builder.append(rowDataType.getTypeName().name());
        builder.append("<");
        List<Field> fields = rowDataType.getFields();
        String s = fields.stream().map(x -> {
            return formatField(x);
        }).collect(joining(","));
        builder.append(s);
        builder.append(">");
        return builder.toString();
    }

    private String formatField(Field field) {
        StringBuilder s = new StringBuilder(process(field.getName()));
        s.append(":").append(process(field.getDataType()));
        if (field.getComment() != null) {
            s.append(" COMMENT ").append(formatStringLiteral(field.getComment().getComment()));
        }
        return s.toString();
    }

    @Override
    public String visitNumericTypeParameter(NumericParameter numericParameter, Void context) {
        return numericParameter.getValue();
    }

    @Override
    public String visitTypeParameter(TypeParameter typeParameter, Void context) {
        return process(typeParameter.getType());
    }

    @Override
    public String visitTimestampLiteral(TimestampLiteral node, Void context) {
        return "TIMESTAMP '" + node.getTimestampFormat() + "'";
    }

    @Override
    public String visitLongLiteral(LongLiteral node, Void context) {
        return Long.toString(node.getValue());
    }

    @Override
    public String visitIntervalLiteral(IntervalLiteral node, Void context) {
        StringBuilder builder = new StringBuilder()
            .append("INTERVAL ")
            .append(" '").append(node.getValue()).append("' ")
            .append(node.getFromDateTime());

        if (node.getToDateTime() != null) {
            builder.append(" TO ").append(node.getToDateTime());
        }
        return builder.toString();
    }

    @Override
    public String visitIdentifier(Identifier node, Void context) {
        if (!node.isDelimited()) {
            return node.getValue();
        } else {
            return '`' + node.getValue() + '`';
        }
    }

    @Override
    public String visitIsConditionExpression(IsConditionExpression isConditionExpression, Void context) {
        return process(isConditionExpression.getExpression()) + " IS " + isConditionExpression
            .getIsType().getCode();
    }

    @Override
    public String visitBitOperationExpression(BitOperationExpression node, Void context) {
        String s = formatBinaryExpression(node.getOperator().getCode(), node.getLeft(), node.getRight());
        return append(s, node.isParenthesized());
    }

    private String append(String expression, boolean parenthesized) {
        StringBuilder sb = new StringBuilder();
        if (parenthesized) {
            sb.append('(');
        }
        sb.append(expression);
        if (parenthesized) {
            sb.append(')');
        }
        return sb.toString();
    }

    @Override
    public String visitLogicalBinaryExpression(LogicalBinaryExpression node, Void context) {
        String s = formatBinaryExpression(node.getOperator().getCode(), node.getLeft(), node.getRight());
        return append(s, node.isParenthesized());
    }

    @Override
    public String visitNotExpression(NotExpression node, Void context) {
        String s = "NOT (" + process(node.getValue(), context) + ")";
        return append(s, node.isParenthesized());
    }

    @Override
    public String visitComparisonExpression(ComparisonExpression comparisonExpression, Void context) {
        String s = formatBinaryExpression(comparisonExpression.getOperator().getCode(),
            comparisonExpression.getLeft(), comparisonExpression.getRight());
        return append(s, comparisonExpression.isParenthesized());
    }

    @Override
    public String visitSubQueryExpression(SubQueryExpression subQueryExpression, Void context) {
        String s = FastModelFormatter.formatNode(subQueryExpression.getQuery());
        return append(s, subQueryExpression.isParenthesized());
    }

    @Override
    public String visitExistsPredicate(ExistsPredicate existsPredicate, Void context) {
        String s = "EXISTS (" + FastModelFormatter.formatNode(existsPredicate.getSubQuery()) + ")";
        return append(s, existsPredicate.isParenthesized());
    }

    @Override
    public String visitListExpr(ListExpression listExpression, Void context) {
        String s = joinExpressions(listExpression.getExpressionList());
        return append(s, listExpression.isParenthesized());
    }

    @Override
    public String visitIfExpression(IfExpression ifExpression, Void context) {
        StringBuilder builder = new StringBuilder();
        builder.append("IF(")
            .append(process(ifExpression.getCondition(), context))
            .append(", ")
            .append(process(ifExpression.getTrueValue(), context));
        if (ifExpression.getFalseValue() != null) {
            builder.append(", ")
                .append(process(ifExpression.getFalseValue(), context));
        }
        builder.append(")");
        return builder.toString();
    }

    @Override
    public String visitArithmeticBinaryExpression(ArithmeticBinaryExpression node,
                                                  Void context) {
        String s = formatBinaryExpression(node.getOperator().getValue(), node.getLeft(), node.getRight());
        return append(s, node.isParenthesized());
    }

    private String formatBinaryExpression(String operator, BaseExpression left, BaseExpression right) {
        StringBuilder sb = new StringBuilder();
        String leftProcess = process(left, null);
        sb.append(leftProcess);
        sb.append(' ').append(operator).append(' ');
        String rightProcess = process(right, null);
        sb.append(rightProcess);
        return sb.toString();
    }

    @Override
    public String visitFloorExpr(Floor floor, Void context) {
        StringBuilder sb = new StringBuilder();
        sb.append("FLOOR ").append('(');
        sb.append(process(floor.getExpression(), context));
        if (floor.getFloorDateQualifiers() != null) {
            sb.append(" TO ");
            sb.append(floor.getFloorDateQualifiers().name());
        }
        sb.append(')');
        return sb.toString();
    }

    @Override
    public String visitTableOrColumn(TableOrColumn tableOrColumn, Void context) {
        VarType varType = tableOrColumn.getVarType();
        if (varType == null) {
            return formatName(tableOrColumn.getQualifiedName());
        }
        if (varType == VarType.MACRO) {
            return "#" + formatName(tableOrColumn.getQualifiedName()) + "#";
        }
        return "${" + formatName(tableOrColumn.getQualifiedName()) + "}";
    }

    @Override
    public String visitAllColumns(AllColumns node, Void context) {
        StringBuilder builder = new StringBuilder();
        if (node.getTarget() != null) {
            builder.append(process(node.getTarget(), context));
            builder.append(".*");
        } else {
            builder.append("*");
        }

        if (!node.getAliases().isEmpty()) {
            builder.append(" AS (");
            Joiner.on(", ").appendTo(builder, node.getAliases().stream()
                .map(alias -> process(alias, context))
                .collect(toList()));
            builder.append(")");
        }

        return builder.toString();
    }

    @Override
    public String visitCustomExpression(CustomExpression customExpression, Void context) {
        return customExpression.getOrigin();
    }

    @Override
    public String visitSimpleCaseExpression(SimpleCaseExpression node, Void context) {
        ImmutableList.Builder<String> parts = ImmutableList.builder();
        parts.add("CASE")
            .add(process(node.getOperand(), context));
        for (WhenClause whenClause : node.getWhenClauses()) {
            parts.add(process(whenClause, context));
        }
        if (node.getDefaultValue() != null) {
            parts.add("ELSE").add(process(node.getDefaultValue(), context));
        }
        parts.add("END");
        if (node.isParenthesized()) {
            return "(" + Joiner.on(' ').join(parts.build()) + ")";
        } else {
            return Joiner.on(' ').join(parts.build());
        }
    }

    @Override
    public String visitWhenClause(WhenClause node, Void context) {
        return "WHEN " + process(node.getOperand(), context) + " THEN " + process(node.getResult(), context);
    }

    @Override
    public String visitRow(Row row, Void context) {
        return row.getItems().stream()
            .map(this::process)
            .collect(Collectors.joining(", ", "(", ")"));
    }

    @Override
    public String visitDecimalLiteral(DecimalLiteral decimalLiteral, Void context) {
        return decimalLiteral.getNumber();
    }

    @Override
    public String visitBetweenPredicate(BetweenPredicate node, Void context) {
        String process = process(node.getValue(), context);
        if (process != null) {
            StringBuilder stringBuilder = new StringBuilder();
            if (node.isParenthesized()) {
                stringBuilder.append('(');
            }
            String s = process + " BETWEEN " +
                process(node.getMin(), context) + " AND " + process(node.getMax(), context);
            stringBuilder.append(s);
            if (node.isParenthesized()) {
                stringBuilder.append(')');
            }
            return stringBuilder.toString();
        } else {
            StringBuilder stringBuilder = new StringBuilder();
            if (node.isParenthesized()) {
                stringBuilder.append('(');
            }
            String code = " BETWEEN " + process(node.getMin(), context) + " AND " + process(node.getMax(), context);
            stringBuilder.append(code);
            if (node.isParenthesized()) {
                stringBuilder.append(')');
            }
            return stringBuilder.toString();
        }
    }

    @Override
    public String visitInPredicate(InPredicate node, Void context) {
        boolean parenthesized = node.isParenthesized();
        StringBuilder stringBuilder = new StringBuilder();
        if (parenthesized) {
            stringBuilder.append("(");
        }
        String process = process(node.getValue(), context);
        stringBuilder.append(process);
        stringBuilder.append(" IN ");
        BaseExpression valueList = node.getValueList();
        stringBuilder.append("(");
        stringBuilder.append(process(valueList, context));
        stringBuilder.append(")");
        if (parenthesized) {
            stringBuilder.append(")");
        }
        return stringBuilder.toString();
    }

    @Override
    public String visitExtract(Extract node, Void context) {
        return "EXTRACT(" + node.getDateTimeEnum().name() + " FROM " + process(node.getExpression(), context) + ")";
    }

    @Override
    public String visitListStringLiteral(ListStringLiteral listStringLiteral, Void context) {
        return Joiner.on(" ").join(
            listStringLiteral.getStringLiteralList().stream().map(e -> process(e, context)).iterator());
    }

    @Override
    public String visitLikePredicate(LikePredicate likePredicate, Void context) {
        StringBuilder builder = new StringBuilder();
        boolean parenthesized = likePredicate.isParenthesized();
        if (parenthesized) {
            builder.append('(');
        }
        LikeCondition condition = likePredicate.getCondition();
        builder.append(process(likePredicate.getLeft(), context))
            .append(" ")
            .append(likePredicate.getOperator().name())
            .append(" ");
        if (condition != null) {
            builder.append(condition.name()).append(" ");
        }
        builder.append(process(likePredicate.getTarget(), context));
        if (parenthesized) {
            builder.append(')');
        }
        return builder.toString();
    }

    @Override
    public String visitIntervalExpr(IntervalExpression intervalExpression, Void context) {
        BaseExpression baseExpression = intervalExpression.getBaseExpression();
        if (baseExpression != null) {
            return "INTERVAL " + "(" + process(baseExpression) + ")" + intervalExpression.getIntervalQualifiers()
                .getCode();
        }
        Boolean interval = intervalExpression.getInterval();
        if (interval != null && interval) {
            return "INTERVAL " + process(intervalExpression.getIntervalValue()) + " " + intervalExpression
                .getIntervalQualifiers().getCode();
        } else {
            return "(" + process(intervalExpression.getIntervalValue()) + ") " + intervalExpression
                .getIntervalQualifiers().getCode();
        }
    }

    @Override
    public String visitBooleanLiteral(BooleanLiteral node, Void context) {
        return String.valueOf(node.isValue());
    }

    @Override
    public String visitStringLiteral(StringLiteral node, Void context) {
        return formatStringLiteral(node.getValue());
    }

    private String joinExpressions(List<BaseExpression> expressions) {
        return Joiner.on(", ").join(expressions.stream()
            .map((e) -> process(e, null))
            .iterator());
    }

    @Override
    public String visitDereferenceExpression(DereferenceExpression node, Void context) {
        String baseString = process(node.getBase(), context);
        return baseString + "." + process(node.getField());
    }

    @Override
    public String visitDateLiteral(DateLiteral dateLiteral, Void context) {
        return "DATE " + formatStringLiteral(dateLiteral.getValue());
    }

    @Override
    public String visitDateTimeAddExpression(DateTimeAddExpression dateTimeAddExpression, Void context) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("DATETIME_ADD(");
        append(dateTimeAddExpression, stringBuilder);
        stringBuilder.append(")");
        return stringBuilder.toString();
    }

    @Override
    public String visitVolInterval(VolInterval volInterval, Void context) {
        return "[" + volInterval.getStart() + "," + volInterval.getEnd() + "]";
    }

    private void append(DateTimeAddExpression dateTimeAddExpression, StringBuilder stringBuilder) {
        stringBuilder.append(process(dateTimeAddExpression.getDateTimeExpression()));
        stringBuilder.append(",");
        stringBuilder.append(" ").append(process(dateTimeAddExpression.getIntervalExpression()));
        if (dateTimeAddExpression.getStartDate() != null) {
            stringBuilder.append(",").append(process(dateTimeAddExpression.getStartDate()));
        }
    }

    @Override
    public String visitDateTimeAddEndExpression(DateTimeAddEndExpression dateTimeAddEndExpression, Void context) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("DATETIME_ADD_END(");
        append(dateTimeAddEndExpression, stringBuilder);
        stringBuilder.append(")");
        return stringBuilder.toString();
    }

    @Override
    public String visitDateTimeAddStartException(DateTimeAddStartExpression dateTimeAddStartExpression, Void context) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("DATETIME_ADD_START(");
        append(dateTimeAddStartExpression, stringBuilder);
        stringBuilder.append(")");
        return stringBuilder.toString();
    }

    public String formatStringLiteral(String s) {
        if (s == null) {
            return "''";
        }
        s = s.replace("'", "''");
        return "'" + s + "'";
    }

    public String formatOrderBy(OrderBy orderBy) {
        return "ORDER BY " + formatSortItems(orderBy.getSortItems());
    }

    private String formatSortItems(List<SortItem> sortItems) {
        return Joiner.on(", ").join(sortItems.stream()
            .map(sortItemFormatterFunction())
            .iterator());
    }

    private Function<SortItem, String> sortItemFormatterFunction() {
        return input -> {
            StringBuilder builder = new StringBuilder();

            builder.append(process(input.getSortKey()));

            switch (input.getOrdering()) {
                case ASC:
                    builder.append(" ASC");
                    break;
                case DESC:
                    builder.append(" DESC");
                    break;
                default:
                    throw new UnsupportedOperationException("unknown ordering: " + input.getOrdering());
            }

            switch (input.getNullOrdering()) {
                case FIRST:
                    builder.append(" NULLS FIRST");
                    break;
                case LAST:
                    builder.append(" NULLS LAST");
                    break;
                case UNDEFINED:
                    // no op
                    break;
                default:
                    throw new UnsupportedOperationException("unknown null ordering: " + input.getNullOrdering());
            }

            return builder.toString();
        };
    }

}


