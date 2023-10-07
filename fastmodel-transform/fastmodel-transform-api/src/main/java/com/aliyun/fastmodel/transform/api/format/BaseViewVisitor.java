/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.api.format;

import java.util.List;
import java.util.OptionalInt;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.core.formatter.FastModelVisitor;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.statement.select.Query;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.DropTable;
import com.aliyun.fastmodel.core.tree.util.QueryUtil;
import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.aliyun.fastmodel.transform.api.context.setting.ViewSetting;
import com.aliyun.fastmodel.transform.api.util.StringJoinUtil;
import com.google.common.collect.Lists;
import lombok.Getter;

import static java.util.stream.Collectors.joining;

/**
 * base view visitor for create view and drop view
 *
 * @author panguanjing
 * @date 2022/8/8
 */
public abstract class BaseViewVisitor<T extends TransformContext> extends FastModelVisitor {
    /**
     * context
     */
    @Getter
    protected final T context;

    public BaseViewVisitor(T context) {
        this.context = context;
    }

    @Override
    public Boolean visitCreateTable(CreateTable createTable, Integer indent) {
        builder.append("CREATE OR REPLACE VIEW ");
        if (createTable.isNotExists()) {
            builder.append("IF NOT EXISTS ");
        }
        builder.append(getCode(createTable.getQualifiedName()));
        String elementIndent = indentString(indent + 1);
        builder.append(newLine("("));
        boolean columnNotEmpty = !createTable.isColumnEmpty();
        List<ColumnDefinition> allColumnDefinition = Lists.newArrayList();
        if (columnNotEmpty) {
            allColumnDefinition.addAll(createTable.getColumnDefines());
        }
        if (!createTable.isPartitionEmpty()) {
            allColumnDefinition.addAll(createTable.getPartitionedBy().getColumnDefinitions());
        }
        builder.append(formatColumnList(allColumnDefinition, elementIndent));
        builder.append(newLine(")"));
        if (createTable.getCommentValue() != null) {
            builder.append(formatComment(createTable.getComment(), isEndNewLine(builder.toString())));
        } else if (createTable.getAliasedNameValue() != null) {
            Comment comment = new Comment(createTable.getAliasedNameValue());
            builder.append(formatComment(comment, isEndNewLine(builder.toString())));
        }
        if (!isEndNewLine(builder.toString())) {
            builder.append("\n");
        }
        builder.append("AS ");
        ViewSetting transformToView = context.getViewSetting();
        if (transformToView.isUseQueryCode()) {
            builder.append(transformToView.getQueryCode());
        } else {
            Query query = transformToView.getQuery();
            if (query != null) {
                process(query, indent);
            } else {
                if (!allColumnDefinition.isEmpty()) {
                    query = sampleQuery(allColumnDefinition);
                    process(query, indent);
                }
            }
        }
        return !allColumnDefinition.isEmpty();
    }

    @Override
    public Boolean visitDropTable(DropTable dropTable, Integer context) {
        builder.append("DROP VIEW ");
        if (dropTable.isExists()) {
            builder.append("IF EXISTS ");
        }
        builder.append(getCode(dropTable.getQualifiedName()));
        return true;
    }

    /**
     * 根据列信息，生成样例数据
     *
     * @param columnDefines
     * @return
     */
    protected
    Query sampleQuery(List<ColumnDefinition> columnDefines) {
        List<BaseExpression> baseExpressions = columnDefines.stream().map(this::toSampleExpression).collect(Collectors.toList());
        return QueryUtil.simpleQuery(QueryUtil.selectList(baseExpressions));
    }

    /**
     * generator sample expression
     *
     * @param columnDefinition
     * @return {@link BaseExpression}
     */
    public BaseExpression toSampleExpression(ColumnDefinition columnDefinition) {
        BaseSampleDataProvider instance = getProvider();
        return instance.getSimpleData(columnDefinition.getDataType());
    }

    /**
     * 默认实现
     *
     * @return BaseSampleDataProvider
     */
    protected BaseSampleDataProvider getProvider() {
        return BaseSampleDataProvider.getInstance();
    }

    @Override
    protected String formatColumnList(List<ColumnDefinition> list, String elementIndent) {
        OptionalInt max = list.stream().map(ColumnDefinition::getColName).mapToInt(x -> formatExpression(x).length()).max();
        return list.stream().map(element -> elementIndent + formatColumnDefinition(element, max.getAsInt())).collect(joining(",\n"));
    }

    @Override
    protected String formatColumnDefinition(ColumnDefinition column, Integer max) {
        StringBuilder sb = new StringBuilder().append(formatColName(column.getColName(), max));
        if (column.getComment() != null) {
            sb.append(formatComment(column.getComment()));
        }
        return sb.toString();
    }

    @Override
    protected String getCode(QualifiedName qualifiedName) {
        QualifiedName tableName = StringJoinUtil.join(this.context.getDatabase(), this.context.getSchema(), qualifiedName.getSuffix());
        return tableName.toString();
    }
}
