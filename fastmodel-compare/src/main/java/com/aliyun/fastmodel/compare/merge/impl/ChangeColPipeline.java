/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.compare.merge.impl;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.aliyun.fastmodel.compare.util.CompareUtil;
import com.aliyun.fastmodel.compare.merge.Pipeline;
import com.aliyun.fastmodel.compare.merge.PipelineComponent;
import com.aliyun.fastmodel.compare.util.MergeTableUtil;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.table.ChangeCol;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable.TableBuilder;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.core.tree.util.PropertyUtil;
import com.google.auto.service.AutoService;
import com.google.common.collect.Lists;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2022/10/9
 */
@PipelineComponent(ChangeCol.class)
@AutoService(Pipeline.class)
public class ChangeColPipeline implements Pipeline<ChangeCol, CreateTable> {
    @Override
    public CreateTable process(CreateTable input, ChangeCol baseStatement) {
        Identifier oldColName = baseStatement.getOldColName();
        List<ColumnDefinition> columnDefines = null;
        if (!input.isColumnEmpty()) {
            columnDefines = Lists.newArrayList(input.getColumnDefines());
            int position = CompareUtil.getPosition(columnDefines, baseStatement.getOldColName());
            if (position >= 0) {
                ColumnDefinition columnDefinition = columnDefines.get(position);
                ColumnDefinition newColumnDefinition = getNewColumnDefinition(baseStatement, columnDefinition);
                columnDefines.set(position, newColumnDefinition);
            }
        }
        List<ColumnDefinition> partitionColumns;
        PartitionedBy partitionedBy = input.getPartitionedBy();
        if (!input.isPartitionEmpty()) {
            partitionColumns = Lists.newArrayList(input.getPartitionedBy().getColumnDefinitions());
            int position = CompareUtil.getPosition(partitionColumns, baseStatement.getOldColName());
            if (position >= 0) {
                ColumnDefinition columnDefinition = partitionColumns.get(position);
                ColumnDefinition newColumnDefinition = getNewColumnDefinition(baseStatement, columnDefinition);
                partitionColumns.set(position, newColumnDefinition);
            }
            partitionedBy = new PartitionedBy(partitionColumns);
        }
        TableBuilder builder = MergeTableUtil.getSuppier(input.getTableDetailType().getParent());
        return builder
            .comment(input.getComment())
            .tableName(input.getQualifiedName())
            .columns(columnDefines)
            .detailType(input.getTableDetailType())
            .partition(partitionedBy)
            .constraints(input.getConstraintStatements())
            .tableIndex(input.getTableIndexList())
            .aliasedName(input.getAliasedName())
            .createOrReplace(input.getCreateOrReplace())
            .properties(input.getProperties())
            .build();
    }

    private ColumnDefinition getNewColumnDefinition(ChangeCol baseStatement, ColumnDefinition columnDefinition) {
        ColumnDefinition newColumn = ColumnDefinition
            .builder()
            .colName(Optional.ofNullable(baseStatement.getNewColName()).orElse(columnDefinition.getColName()))
            .aliasedName(Optional.ofNullable(baseStatement.getColumnDefinition().getAliasedName()).orElse(columnDefinition.getAliasedName()))
            .comment(Optional.ofNullable(baseStatement.getComment()).orElse(columnDefinition.getComment()))
            .category(Optional.ofNullable(baseStatement.getCategory()).orElse(columnDefinition.getCategory()))
            .dataType(Optional.ofNullable(baseStatement.getDataType()).orElse(columnDefinition.getDataType()))
            .defaultValue(Optional.ofNullable(baseStatement.getDefaultValue()).orElse(columnDefinition.getDefaultValue()))
            .notNull(Optional.ofNullable(baseStatement.getNotNull()).orElse(columnDefinition.getNotNull()))
            .primary(Optional.ofNullable(baseStatement.getPrimary()).orElse(columnDefinition.getPrimary()))
            .properties(CompareUtil.merge(baseStatement.getColumnProperties(), columnDefinition.getColumnProperties()))
            .refDimension(Optional.ofNullable(baseStatement.getColumnDefinition().getRefDimension()).orElse(columnDefinition.getRefDimension()))
            .refIndicators(Optional.ofNullable(baseStatement.getColumnDefinition().getRefIndicators()).orElse(columnDefinition.getRefIndicators()))
            .build();
        return newColumn;
    }




}
