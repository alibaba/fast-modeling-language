/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.compare.merge.impl;

import com.aliyun.fastmodel.compare.merge.Pipeline;
import com.aliyun.fastmodel.compare.merge.PipelineComponent;
import com.aliyun.fastmodel.compare.util.MergeTableUtil;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable.TableBuilder;
import com.aliyun.fastmodel.core.tree.statement.table.RenameTable;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableAliasedName;
import com.google.auto.service.AutoService;

/**
 * rename table pipeline
 *
 * @author panguanjing
 * @date 2022/10/9
 */
@PipelineComponent(SetTableAliasedName.class)
@AutoService(Pipeline.class)
public class SetTableAliasedNamePipeLine implements Pipeline<SetTableAliasedName, CreateTable> {

    @Override
    public CreateTable process(CreateTable input, SetTableAliasedName baseStatement) {
        TableBuilder builder = MergeTableUtil.getSuppier(input.getTableDetailType().getParent());
        return builder
            .comment(input.getComment())
            .tableName(input.getQualifiedName())
            .columns(input.getColumnDefines())
            .detailType(input.getTableDetailType())
            .partition(input.getPartitionedBy())
            .constraints(input.getConstraintStatements())
            .tableIndex(input.getTableIndexList())
            .aliasedName(baseStatement.getAliasedName())
            .createOrReplace(input.getCreateOrReplace())
            .properties(input.getProperties())
            .build();
    }
}
