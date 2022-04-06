/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.compare.merge.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.compare.merge.Pipeline;
import com.aliyun.fastmodel.compare.merge.PipelineComponent;
import com.aliyun.fastmodel.compare.util.MergeTableUtil;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.table.AddPartitionCol;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable.TableBuilder;
import com.aliyun.fastmodel.core.tree.statement.table.DropPartitionCol;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.google.auto.service.AutoService;
import com.google.common.collect.Lists;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2022/10/9
 */
@PipelineComponent(DropPartitionCol.class)
@AutoService(Pipeline.class)
public class DropPartitionColPipeline implements Pipeline<DropPartitionCol, CreateTable> {

    @Override
    public CreateTable process(CreateTable input, DropPartitionCol baseStatement) {
        Identifier columnDefines = baseStatement.getColumnName();
        PartitionedBy partitionedBy = input.getPartitionedBy();
        if (partitionedBy != null) {
            List<ColumnDefinition> columnDefinitions = partitionedBy.getColumnDefinitions();
            List<ColumnDefinition> collect = columnDefinitions.stream().filter(c -> {
                return !Objects.equals(c.getColName(), columnDefines);
            }).collect(Collectors.toList());
            partitionedBy = new PartitionedBy(collect);
        } else {
            return input;
        }
        TableBuilder builder = MergeTableUtil.getSuppier(input.getTableDetailType().getParent());
        return builder
            .comment(input.getComment())
            .tableName(input.getQualifiedName())
            .columns(input.getColumnDefines())
            .detailType(input.getTableDetailType())
            .partition(partitionedBy)
            .constraints(input.getConstraintStatements())
            .tableIndex(input.getTableIndexList())
            .aliasedName(input.getAliasedName())
            .createOrReplace(input.getCreateOrReplace())
            .properties(input.getProperties())
            .build();
    }
}
