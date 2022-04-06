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
import com.aliyun.fastmodel.core.tree.statement.table.AddCols;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable.TableBuilder;
import com.google.auto.service.AutoService;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2022/10/9
 */
@PipelineComponent(AddCols.class)
@AutoService(Pipeline.class)
public class AddColsPipeline implements Pipeline<AddCols, CreateTable> {
    @Override
    public CreateTable process(CreateTable input, AddCols baseStatement) {
        List<ColumnDefinition> columnDefineList = baseStatement.getColumnDefineList();
        List<ColumnDefinition> columnDefines = input.getColumnDefines();
        List<ColumnDefinition> all = new ArrayList<>();
        if (columnDefines != null) {
            all.addAll(columnDefines);
        }
        all.addAll(columnDefineList);
        TableBuilder suppier = MergeTableUtil.getSuppier(input.getTableDetailType().getParent());
        return suppier
            .comment(input.getComment())
            .tableName(input.getQualifiedName())
            .columns(all)
            .detailType(input.getTableDetailType())
            .partition(input.getPartitionedBy())
            .constraints(input.getConstraintStatements())
            .tableIndex(input.getTableIndexList())
            .aliasedName(input.getAliasedName())
            .createOrReplace(input.getCreateOrReplace())
            .properties(input.getProperties())
            .build();
    }
}
