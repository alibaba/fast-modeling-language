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
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable.TableBuilder;
import com.aliyun.fastmodel.core.tree.statement.table.DropCol;
import com.google.auto.service.AutoService;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2022/10/9
 */
@PipelineComponent(DropCol.class)
@AutoService(Pipeline.class)
public class DropColPipeline implements Pipeline<DropCol, CreateTable> {

    @Override
    public CreateTable process(CreateTable input, DropCol baseStatement) {
        Identifier columnName = baseStatement.getColumnName();
        List<ColumnDefinition> columnDefines = input.getColumnDefines().stream().filter(
            c -> {
                return !Objects.equals(c.getColName(), columnName);
            }
        ).collect(Collectors.toList());
        TableBuilder builder = MergeTableUtil.getSuppier(input.getTableDetailType().getParent());
        return builder
            .comment(input.getComment())
            .tableName(input.getQualifiedName())
            .columns(columnDefines)
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
