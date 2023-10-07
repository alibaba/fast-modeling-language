/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.compare.merge.impl;

import java.util.List;

import com.aliyun.fastmodel.compare.merge.Pipeline;
import com.aliyun.fastmodel.compare.merge.PipelineComponent;
import com.aliyun.fastmodel.compare.util.MergeTableUtil;
import com.aliyun.fastmodel.core.tree.statement.table.AddConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable.TableBuilder;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.google.auto.service.AutoService;
import com.google.common.collect.Lists;

/**
 * AddConstraintPipeline
 *
 * @author panguanjing
 * @date 2022/10/9
 */
@PipelineComponent(AddConstraint.class)
@AutoService(Pipeline.class)
public class AddConstraintPipeline implements Pipeline<AddConstraint, CreateTable> {

    @Override
    public CreateTable process(CreateTable input, AddConstraint baseStatement) {
        List<BaseConstraint> constraintStatements = input.getConstraintStatements();
        List<BaseConstraint> all = Lists.newArrayList();
        if (constraintStatements != null) {
            all.addAll(constraintStatements);
        }
        all.add(baseStatement.getConstraintStatement());
        TableBuilder builder = MergeTableUtil.getSuppier(input.getTableDetailType().getParent());
        return builder
            .comment(input.getComment())
            .tableName(input.getQualifiedName())
            .columns(input.getColumnDefines())
            .detailType(input.getTableDetailType())
            .partition(input.getPartitionedBy())
            .constraints(all)
            .tableIndex(input.getTableIndexList())
            .aliasedName(input.getAliasedName())
            .createOrReplace(input.getCreateOrReplace())
            .properties(input.getProperties())
            .build();
    }
}
