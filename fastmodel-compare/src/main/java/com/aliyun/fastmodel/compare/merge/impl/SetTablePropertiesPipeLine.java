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

import com.aliyun.fastmodel.compare.CompareStrategy;
import com.aliyun.fastmodel.compare.merge.Pipeline;
import com.aliyun.fastmodel.compare.merge.PipelineComponent;
import com.aliyun.fastmodel.compare.util.CompareUtil;
import com.aliyun.fastmodel.compare.util.MergeTableUtil;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable.TableBuilder;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableComment;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableProperties;
import com.aliyun.fastmodel.core.tree.util.PropertyUtil;
import com.google.auto.service.AutoService;
import com.google.common.collect.Lists;

/**
 * set table comment pipeline
 *
 * @author panguanjing
 * @date 2022/10/9
 */
@PipelineComponent(SetTableProperties.class)
@AutoService(Pipeline.class)
public class SetTablePropertiesPipeLine implements Pipeline<SetTableProperties, CreateTable> {

    @Override
    public CreateTable process(CreateTable input, SetTableProperties baseStatement) {
        List<Property> properties = input.getProperties();
        List<Property> setProperties = baseStatement.getProperties();
        List<Property> all = Lists.newArrayList();
        if (properties == null) {
            all.addAll(setProperties);
        } else {
            all = CompareUtil.merge(properties, setProperties);
        }
        TableBuilder builder = MergeTableUtil.getSuppier(input.getTableDetailType().getParent());
        return builder
            .comment(input.getComment())
            .tableName(input.getQualifiedName())
            .columns(input.getColumnDefines())
            .detailType(input.getTableDetailType())
            .partition(input.getPartitionedBy())
            .constraints(input.getConstraintStatements())
            .tableIndex(input.getTableIndexList())
            .aliasedName(input.getAliasedName())
            .createOrReplace(input.getCreateOrReplace())
            .properties(all)
            .build();
    }
}
