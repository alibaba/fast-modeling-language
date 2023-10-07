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

import com.aliyun.fastmodel.compare.merge.Pipeline;
import com.aliyun.fastmodel.compare.merge.PipelineComponent;
import com.aliyun.fastmodel.compare.util.MergeTableUtil;
import com.aliyun.fastmodel.core.tree.statement.table.AddPartitionCol;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable.TableBuilder;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.google.auto.service.AutoService;
import com.google.common.collect.Lists;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2022/10/9
 */
@PipelineComponent(AddPartitionCol.class)
@AutoService(Pipeline.class)
public class AddPartitionColPipeline implements Pipeline<AddPartitionCol, CreateTable> {

    @Override
    public CreateTable process(CreateTable input, AddPartitionCol baseStatement) {
        ColumnDefinition columnDefines = baseStatement.getColumnDefinition();
        PartitionedBy partitionedBy = input.getPartitionedBy();
        if (partitionedBy == null) {
            partitionedBy = new PartitionedBy(Lists.newArrayList(columnDefines));
        }else{
            List<ColumnDefinition> all = new ArrayList<>();
            List<ColumnDefinition> columnDefinitions = partitionedBy.getColumnDefinitions();
            all.addAll(columnDefinitions);
            all.add(columnDefines);
            partitionedBy = new PartitionedBy(all);
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
