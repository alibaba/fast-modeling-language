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

package com.aliyun.fastmodel.transform.adbpg.format;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.statement.table.AddCols;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.transform.adbpg.context.AdbPostgreSQLTransformContext;
import com.aliyun.fastmodel.transform.adbpg.parser.tree.AdbPostgreSQLPartitionBy;
import com.aliyun.fastmodel.transform.adbpg.parser.tree.partition.desc.AdbPostgreSQLRangeElement;
import com.aliyun.fastmodel.transform.adbpg.parser.tree.partition.desc.PartitionIntervalExpression;
import com.aliyun.fastmodel.transform.adbpg.parser.tree.partition.desc.PartitionValueExpression;
import com.aliyun.fastmodel.transform.adbpg.parser.visitor.AdbPostgreSQLVisitor;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.desc.DistributeNonKeyConstraint;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.desc.NonKeyConstraint;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.sub.BaseSubPartition;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.sub.SubListPartition;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.sub.SubRangePartition;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.sub.element.BasePartitionElement;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.sub.element.BaseSubPartitionElement;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.sub.element.ListPartitionElement;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.sub.element.RangePartitionElement;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.sub.element.SubListPartitionElement;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.sub.element.SubPartitionList;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.sub.element.SubRangePartitionElement;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.sub.template.SubListTemplatePartition;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.sub.template.SubRangeTemplatePartition;
import com.aliyun.fastmodel.transform.postgresql.client.property.PostgreSQLPropertyKey;
import com.aliyun.fastmodel.transform.postgresql.format.PostgreSQLOutVisitor;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

import static java.util.stream.Collectors.joining;

/**
 * AdbPostgreSQLOutVisitors
 *
 * @author panguanjing
 * @date 2024/10/18
 */
public class AdbPostgreSQLOutVisitor extends PostgreSQLOutVisitor implements AdbPostgreSQLVisitor<Boolean, Integer> {

    AdbPostgreSQLTransformContext context;

    public AdbPostgreSQLOutVisitor(AdbPostgreSQLTransformContext context) {
        this.context = context;
    }

    @Override
    public Boolean visitCreateTable(CreateTable node, Integer indent) {
        boolean columnEmpty = node.isColumnEmpty();
        boolean executable = !columnEmpty;
        builder.append("CREATE TABLE ");
        if (node.isNotExists()) {
            builder.append("IF NOT EXISTS ");
        }
        String tableCode = getCode(node.getQualifiedName());
        builder.append(tableCode);
        String elementIndent = indentString(indent + 1);
        List<ColumnDefinition> columnDefines = node.getColumnDefines();
        if (!columnEmpty) {
            builder.append(" (\n");
            String columnList = formatColumnList(columnDefines, elementIndent);
            builder.append(columnList);
            if (!node.isConstraintEmpty()) {
                appendConstraint(node, indent);
            }
            builder.append("\n").append(")");
        }
        appendWithClause(node);
        if (!node.isConstraintEmpty()) {
            List<BaseConstraint> baseConstraints = node.getConstraintStatements().stream()
                .filter(c -> c instanceof NonKeyConstraint).collect(Collectors.toList());
            for (BaseConstraint constraint : baseConstraints) {
                builder.append("\n");
                process(constraint, indent);
            }
        }
        if (!node.isPartitionEmpty()) {
            process(node.getPartitionedBy(), indent);
        }
        builder.append(";");
        return executable;
    }

    @Override
    protected void appendWithClause(CreateTable node) {
        if (CollectionUtils.isEmpty(node.getProperties())) {
            return;
        }
        List<Property> properties = node.getProperties().stream()
            .filter(x -> Objects.nonNull(AdbPgPropertyKey.getByValue(x.getName())))
            .collect(Collectors.toList());
        if (CollectionUtils.isEmpty(properties)) {
            return;
        }

        if (!isEndNewLine(builder.toString())) {
            builder.append(StringUtils.LF);
        }
        builder.append("WITH ( ");
        String propertiesStr = properties.stream()
            .filter(x -> Objects.nonNull(AdbPgPropertyKey.getByValue(x.getName())))
            .map(p -> String.format("%s = %s", p.getName(), p.getValue()))
            .collect(joining(","));
        builder.append(propertiesStr);
        builder.append(" )");
    }

    @Override
    public Boolean visitDistributeKeyConstraint(DistributeNonKeyConstraint distributeKeyConstraint, Integer context) {
        if (BooleanUtils.isTrue(distributeKeyConstraint.getRandom())) {
            builder.append("DISTRIBUTED RANDOMLY");
        } else if (BooleanUtils.isTrue(distributeKeyConstraint.getReplicated())) {
            builder.append("DISTRIBUTED REPLICATED");
        } else {
            builder.append("DISTRIBUTED BY ");
            builder.append("(");
            String list = distributeKeyConstraint.getColumns().stream()
                .map(this::formatExpression)
                .collect(Collectors.joining(","));
            builder.append(list);
            builder.append(")");
        }
        if (!isEndNewLine(builder.toString())) {
            builder.append("\n");
        }
        return true;
    }

    @Override
    public Boolean visitPartitionedBy(PartitionedBy partitionedBy, Integer context) {
        return visitAdbPostgreSQLPartitionBy((AdbPostgreSQLPartitionBy)partitionedBy, context);
    }

    /**
     * PARTITION BY (BISON_LIST | RANGE) OPEN_PAREN part_params CLOSE_PAREN  subpartition_option* (partition_element_list)?
     *
     * @param adbPostgreSQLPartitionBy
     * @param context
     * @return
     */
    @Override
    public Boolean visitAdbPostgreSQLPartitionBy(AdbPostgreSQLPartitionBy adbPostgreSQLPartitionBy, Integer context) {
        String value = adbPostgreSQLPartitionBy.getPartitionByType().getValue();
        builder.append("PARTITION BY ").append(value).append(" (");
        if (adbPostgreSQLPartitionBy.getColumnDefinitions() != null) {
            String columnList = adbPostgreSQLPartitionBy.getColumnDefinitions().stream()
                .map(c -> formatExpression(c.getColName())).collect(Collectors.joining(","));
            builder.append(columnList);
        }
        builder.append(")");
        List<BaseSubPartition> subPartitions = adbPostgreSQLPartitionBy.getSubPartitions();
        if (subPartitions != null) {
            builder.append("\n");
            Iterator<BaseSubPartition> iterator = subPartitions.iterator();
            if (iterator.hasNext()) {
                process(iterator.next());
                while (iterator.hasNext()) {
                    builder.append("\n");
                    process(iterator.next());
                }
            }
        }

        List<BasePartitionElement> partitionElements = adbPostgreSQLPartitionBy.getPartitionElements();
        if (partitionElements != null) {
            if (!isEndNewLine(builder.toString())) {
                builder.append("\n");
            }
            builder.append("(");
            Iterator<BasePartitionElement> iterator = partitionElements.iterator();
            if (iterator.hasNext()) {
                process(iterator.next());
                while (iterator.hasNext()) {
                    builder.append("\n,");
                    process(iterator.next());
                }
            }
            builder.append(")");
        }
        return true;
    }

    // SUBPARTITION BY (RANGE|BISON_LIST) OPEN_PAREN a_expr CLOSE_PAREN
    @Override
    public Boolean visitSubListPartition(SubListPartition subListPartition, Integer context) {
        builder.append("SUBPARTITION BY LIST (");
        builder.append(formatExpression(subListPartition.getExpression()));
        builder.append(")");
        return true;
    }

    //SUBPARTITION BY (RANGE|BISON_LIST) OPEN_PAREN a_expr CLOSE_PAREN
    @Override
    public Boolean visitSubRangePartition(SubRangePartition subRangePartition, Integer context) {
        builder.append("SUBPARTITION BY RANGE (");
        builder.append(formatExpression(subRangePartition.getExpression()));
        builder.append(")");
        return true;
    }

    //SUBPARTITION BY (RANGE | BISON_LIST) OPEN_PAREN a_expr CLOSE_PAREN SUBPARTITION TEMPLATE opt_subpartition_list
    @Override
    public Boolean visitSubListTemplatePartition(SubListTemplatePartition subListTemplatePartition, Integer context) {
        builder.append("SUBPARTITION BY LIST (");
        builder.append(formatExpression(subListTemplatePartition.getExpression()));
        builder.append(")");
        builder.append(" SUBPARTITION TEMPLATE ");
        if (subListTemplatePartition.getSubPartitionList() != null) {
            process(subListTemplatePartition.getSubPartitionList());
        }
        return true;
    }

    //partition_element:
    //    DEFAULT PARTITION colid
    //    | (PARTITION colid)? list_element opt_subpartition_list
    //    | (PARTITION colid)? range_element opt_subpartition_list
    @Override
    public Boolean visitListPartitionElement(ListPartitionElement listPartitionElement, Integer context) {
        Boolean defaultExpr = listPartitionElement.getDefaultExpr();
        if (BooleanUtils.isTrue(defaultExpr)) {
            builder.append("DEFAULT PARTITION ");
            builder.append(formatName(listPartitionElement.getQualifiedName()));
            return true;
        }
        if (listPartitionElement.getQualifiedName() != null) {
            builder.append("PARTITION ");
            builder.append(formatName(listPartitionElement.getQualifiedName()));
        }

        // VALUES OPEN_PAREN list_expr CLOSE_PAREN
        builder.append(" VALUES (");
        List<BaseExpression> expressionList = listPartitionElement.getExpressionList();
        String e = expressionList.stream().map(this::formatExpression).collect(Collectors.joining(","));
        builder.append(e);
        builder.append(")");
        return true;
    }

    //SUBPARTITION BY (RANGE | BISON_LIST) OPEN_PAREN a_expr CLOSE_PAREN SUBPARTITION TEMPLATE opt_subpartition_list
    @Override
    public Boolean visitSubRangeTemplatePartition(SubRangeTemplatePartition subRangeTemplatePartition, Integer context) {
        builder.append("SUBPARTITION BY RANGE (");
        builder.append(formatExpression(subRangeTemplatePartition.getExpression()));
        builder.append(")");
        builder.append(" SUBPARTITION TEMPLATE ");
        if (subRangeTemplatePartition.getSubPartitionList() != null) {
            builder.append("\n");
            process(subRangeTemplatePartition.getSubPartitionList());
        }
        return true;
    }

    // subpartition_element (COMMA subpartition_element)*
    @Override
    public Boolean visitSubPartitionList(SubPartitionList subPartitionList, Integer context) {
        builder.append("(");
        List<BaseSubPartitionElement> subPartitionElementList = subPartitionList.getSubPartitionElementList();
        Iterator<BaseSubPartitionElement> iterator = subPartitionElementList.iterator();
        if (iterator.hasNext()) {
            process(iterator.next());
            while (iterator.hasNext()) {
                builder.append(",");
                process(iterator.next());
            }
        }
        builder.append(")");
        return true;
    }

    /**
     * subpartition_element
     * :DEFAULT SUBPARTITION colid
     * | (SUBPARTITION colid)? list_element  (OPEN_PAREN subpartition_element CLOSE_PAREN)?
     * | (SUBPARTITION colid)? range_element (OPEN_PAREN subpartition_element CLOSE_PAREN)?
     * ;
     *
     * @param subListPartitionElement
     * @param context
     * @return
     */
    @Override
    public Boolean visitListSubPartitionElement(SubListPartitionElement subListPartitionElement, Integer context) {
        if (BooleanUtils.isTrue(subListPartitionElement.getDefaultListExpr())) {
            builder.append("DEFAULT SUBPARTITION ");
            builder.append(formatName(subListPartitionElement.getQualifiedName()));
            return true;
        }
        if (subListPartitionElement.getQualifiedName() != null) {
            builder.append("SUBPARTITION ");
            builder.append(formatName(subListPartitionElement.getQualifiedName()));
        }
        builder.append(" VALUES (");
        List<BaseExpression> expressionList = subListPartitionElement.getExpressionList();
        String e = expressionList.stream().map(this::formatExpression).collect(Collectors.joining(","));
        builder.append(e);
        builder.append(")");
        return true;
    }

    // (SUBPARTITION colid)? range_element (OPEN_PAREN subpartition_element CLOSE_PAREN)?
    @Override
    public Boolean visitRangeSubPartitionElement(SubRangePartitionElement subRangePartitionElement, Integer context) {
        if (subRangePartitionElement.getName() != null) {
            builder.append("SUBPARTITION ");
            builder.append(formatName(subRangePartitionElement.getName()));
        }
        process(subRangePartitionElement.getSingleRangePartition());
        return true;
    }

    @Override
    public Boolean visitRangePartitionElement(RangePartitionElement rangePartitionElement, Integer context) {
        process(rangePartitionElement.getSingleRangePartition());
        return true;
    }

    //     START  start=partition_value_expr opt_inclusive (END_P end=partition_value_expr opt_inclusive)? (EVERY
    //     every=partition_value_interval_expr)?
    //    | END_P end=partition_value_expr opt_inclusive (EVERY every=partition_value_interval_expr)?
    @Override
    public Boolean visitAdbPostgreSQLRangeElement(AdbPostgreSQLRangeElement adbPostgreSQLRangeElement, Integer context) {
        if (adbPostgreSQLRangeElement.getStart() != null) {
            builder.append("START ");
            process(adbPostgreSQLRangeElement.getStart());
        }
        if (adbPostgreSQLRangeElement.getEnd() != null) {
            builder.append("END ");
            process(adbPostgreSQLRangeElement.getEnd());
        }
        if (adbPostgreSQLRangeElement.getEvery() != null) {
            builder.append("EVERY ");
            process(adbPostgreSQLRangeElement.getEvery());
        }
        return true;
    }

    /**
     * OPEN_PAREN typename? a_expr CLOSE_PAREN
     *
     * @param partitionValueExpression
     * @param context
     * @return
     */
    @Override
    public Boolean visitPartitionValueExpression(PartitionValueExpression partitionValueExpression, Integer context) {
        builder.append("(");
        if (partitionValueExpression.getBaseDataType() != null) {
            builder.append(formatExpression(partitionValueExpression.getBaseDataType()));
            builder.append(" ");
        }
        String expression = formatExpression(partitionValueExpression.getBaseExpression());
        builder.append(expression);
        builder.append(")");
        return true;
    }

    /**
     * partition_value_interval_expr:
     * OPEN_PAREN typename? (a_expr | (INTERVAL StringConstant)) CLOSE_PAREN
     * ;
     *
     * @param partitionIntervalExpression
     * @param context
     * @return
     */
    @Override
    public Boolean visitPartitionIntervalExpression(PartitionIntervalExpression partitionIntervalExpression, Integer context) {
        builder.append("(");
        if (partitionIntervalExpression.getBaseDataType() != null) {
            builder.append(formatExpression(partitionIntervalExpression.getBaseDataType()));
            builder.append(" ");
        }
        if (partitionIntervalExpression.getBaseExpression() != null) {
            String expression = formatExpression(partitionIntervalExpression.getBaseExpression());
            builder.append(expression);
        } else {
            builder.append("INTERVAL ");
            builder.append(formatExpression(partitionIntervalExpression.getStringLiteral()));
        }
        builder.append(")");
        return true;
    }
}
