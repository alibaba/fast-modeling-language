/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.api.builder.merge.impl;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.SetColComment;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableAliasedName;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableComment;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableProperties;
import com.aliyun.fastmodel.core.tree.statement.table.UnSetTableProperties;
import com.aliyun.fastmodel.core.tree.util.PropertyUtil;
import com.aliyun.fastmodel.transform.api.builder.merge.MergeBuilder;
import com.aliyun.fastmodel.transform.api.builder.merge.MergeResult;
import com.aliyun.fastmodel.transform.api.builder.merge.exception.MergeErrorCode;
import com.aliyun.fastmodel.transform.api.builder.merge.exception.MergeException;
import com.google.common.collect.Lists;

/**
 * CreateTableMergeBuilder
 *
 * @author panguanjing
 * @date 2022/6/29
 */
public class CreateTableMergeBuilder implements MergeBuilder {

    @Override
    public BaseStatement getMainStatement(List<BaseStatement> baseStatements) {
        List<BaseStatement> baseStatementStream = baseStatements.stream().filter(
            x -> x instanceof CreateTable
        ).collect(Collectors.toList());
        if (baseStatementStream.isEmpty()) {
            throw new MergeException(MergeErrorCode.ERR_CREATE_TABLE_NOT_EXISTS);
        }
        if (baseStatementStream.size() > 1) {
            throw new MergeException(MergeErrorCode.ERR_TOO_MANY_CREATE_TABLES);
        }
        return baseStatementStream.get(0);
    }

    @Override
    public MergeResult mergeStatement(BaseStatement mainStatement, BaseStatement other) {
        CreateTable createTable = (CreateTable)mainStatement;
        return mergeStatement(createTable, other);
    }

    /**
     * 将一个操作语句合并到createTable中
     *
     * @param createTable
     * @param baseOperatorStatement
     * @return
     */
    public MergeResult mergeStatement(CreateTable createTable, BaseStatement baseOperatorStatement) {
        if (baseOperatorStatement instanceof SetTableComment) {
            return getMergeResult(createTable, (SetTableComment)baseOperatorStatement);
        } else if (baseOperatorStatement instanceof SetTableAliasedName) {
            return getMergeResult(createTable, (SetTableAliasedName)baseOperatorStatement);
        } else if (baseOperatorStatement instanceof SetTableProperties) {
            return getMergeResult(createTable, (SetTableProperties)baseOperatorStatement);
        } else if (baseOperatorStatement instanceof UnSetTableProperties) {
            return getMergeResult(createTable, (UnSetTableProperties)baseOperatorStatement);
        } else if (baseOperatorStatement instanceof SetColComment) {
            return getMergeResult(createTable, (SetColComment)baseOperatorStatement);
        }
        return new MergeResult(createTable, false);
    }

    private MergeResult getMergeResult(CreateTable create, SetColComment baseOperatorStatement) {
        Identifier changeColumn = baseOperatorStatement.getChangeColumn();
        Comment comment = baseOperatorStatement.getComment();
        if (create.isColumnEmpty()) {
            return new MergeResult(create, false);
        }
        List<ColumnDefinition> sourceColumnDefinition = create.getColumnDefines();
        List<ColumnDefinition> merge = mergeColumn(sourceColumnDefinition, changeColumn, comment);
        CreateTable build = CreateTable.builder()
            .tableName(create.getQualifiedName())
            .aliasedName(create.getAliasedName())
            .detailType(create.getTableDetailType())
            .columns(merge)
            .partition(create.getPartitionedBy())
            .constraints(create.getConstraintStatements())
            .comment(create.getComment())
            .properties(create.getProperties())
            .createOrReplace(create.getCreateOrReplace())
            .tableIndex(create.getTableIndexList())
            .ifNotExist(create.isNotExists())
            .build();
        return new MergeResult(build, true);
    }

    private MergeResult getMergeResult(CreateTable create, UnSetTableProperties baseOperatorStatement) {
        List<String> propertyKeys = baseOperatorStatement.getPropertyKeys();
        List<Property> sourceProperty = create.getProperties();
        if (create.isPropertyEmpty()) {
            return new MergeResult(create, false);
        }
        Map<String, String> propertyMap = PropertyUtil.toMap(sourceProperty);
        for (String key : propertyKeys) {
            propertyMap.remove(key);
        }
        List<Property> result = PropertyUtil.toProperty(propertyMap);
        CreateTable build = CreateTable.builder()
            .tableName(create.getQualifiedName())
            .aliasedName(create.getAliasedName())
            .detailType(create.getTableDetailType())
            .columns(create.getColumnDefines())
            .partition(create.getPartitionedBy())
            .constraints(create.getConstraintStatements())
            .comment(create.getComment())
            .properties(result)
            .createOrReplace(create.getCreateOrReplace())
            .tableIndex(create.getTableIndexList())
            .ifNotExist(create.isNotExists())
            .build();
        return new MergeResult(build, true);
    }

    private MergeResult getMergeResult(CreateTable create, SetTableProperties baseOperatorStatement) {
        List<Property> properties = baseOperatorStatement.getProperties();
        List<Property> sourceProperties = create.getProperties();
        if (sourceProperties == null) {
            sourceProperties = properties;
        } else {
            Map<String, String> source = PropertyUtil.toMap(sourceProperties);
            for (Property p : properties) {
                source.put(p.getName(), p.getValue());
            }
            sourceProperties = PropertyUtil.toProperty(source);
        }
        CreateTable createTable = CreateTable.builder()
            .tableName(create.getQualifiedName())
            .aliasedName(create.getAliasedName())
            .detailType(create.getTableDetailType())
            .columns(create.getColumnDefines())
            .partition(create.getPartitionedBy())
            .constraints(create.getConstraintStatements())
            .comment(create.getComment())
            .properties(sourceProperties)
            .createOrReplace(create.getCreateOrReplace())
            .tableIndex(create.getTableIndexList())
            .ifNotExist(create.isNotExists())
            .build();
        return new MergeResult(createTable, true);
    }

    private MergeResult getMergeResult(CreateTable create, SetTableAliasedName baseOperatorStatement) {
        SetTableAliasedName setTableAliasedName = baseOperatorStatement;
        CreateTable build = CreateTable.builder()
            .tableName(create.getQualifiedName())
            .aliasedName(setTableAliasedName.getAliasedName())
            .detailType(create.getTableDetailType())
            .columns(create.getColumnDefines())
            .partition(create.getPartitionedBy())
            .constraints(create.getConstraintStatements())
            .comment(create.getComment())
            .properties(create.getProperties())
            .createOrReplace(create.getCreateOrReplace())
            .ifNotExist(create.isNotExists())
            .tableIndex(create.getTableIndexList())
            .build();
        return new MergeResult(build, true);
    }

    private MergeResult getMergeResult(CreateTable create, SetTableComment baseOperatorStatement) {
        SetTableComment setTableComment = baseOperatorStatement;
        Comment comment = setTableComment.getComment();
        CreateTable build = CreateTable.builder()
            .tableName(create.getQualifiedName())
            .aliasedName(create.getAliasedName())
            .detailType(create.getTableDetailType())
            .columns(create.getColumnDefines())
            .partition(create.getPartitionedBy())
            .constraints(create.getConstraintStatements())
            .comment(comment)
            .properties(create.getProperties())
            .tableIndex(create.getTableIndexList())
            .createOrReplace(create.getCreateOrReplace())
            .ifNotExist(create.isNotExists())
            .build();
        return new MergeResult(build, true);
    }

    private List<ColumnDefinition> mergeColumn(List<ColumnDefinition> sourceColumnDefinition, Identifier changeColumn,
        Comment comment) {
        List<ColumnDefinition> list = Lists.newArrayListWithCapacity(sourceColumnDefinition.size());
        for (ColumnDefinition columnDefinition : sourceColumnDefinition) {
            if (!columnDefinition.getColName().equals(changeColumn)) {
                list.add(columnDefinition);
                continue;
            }
            ColumnDefinition newColumn = ColumnDefinition.builder()
                .colName(columnDefinition.getColName())
                .aliasedName(columnDefinition.getAliasedName())
                .category(columnDefinition.getCategory())
                .dataType(columnDefinition.getDataType())
                .comment(comment)
                .properties(columnDefinition.getColumnProperties())
                .notNull(columnDefinition.getNotNull())
                .primary(columnDefinition.getPrimary())
                .defaultValue(columnDefinition.getDefaultValue())
                .refDimension(columnDefinition.getRefDimension())
                .refIndicators(columnDefinition.getRefIndicators())
                .build();
            list.add(newColumn);
        }
        return list;
    }
}
