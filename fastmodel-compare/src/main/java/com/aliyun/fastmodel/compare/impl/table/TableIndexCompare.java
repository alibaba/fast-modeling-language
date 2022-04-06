/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.compare.impl.table;

import java.util.List;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.statement.table.CreateIndex;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.DropIndex;
import com.aliyun.fastmodel.core.tree.statement.table.index.TableIndex;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Table Index Compare
 *
 * @author panguanjing
 * @date 2021/8/30
 */
public class TableIndexCompare implements TableElementCompare {

    @Override
    public List<BaseStatement> compareTableElement(CreateTable before, CreateTable after) {
        List<TableIndex> tableIndexList = before.getTableIndexList();
        List<TableIndex> afterTableIndex = after.getTableIndexList();
        ImmutableList.Builder<BaseStatement> builder = ImmutableList.builder();
        boolean beforeEmpty = tableIndexList == null || tableIndexList.isEmpty();
        boolean afterNotEmpty = afterTableIndex != null && !afterTableIndex.isEmpty();
        if (beforeEmpty && !afterNotEmpty) {
            return ImmutableList.of();
        }
        if (beforeEmpty && afterNotEmpty) {
            List<CreateIndex> collect = afterTableIndex.stream().map(
                x -> new CreateIndex(
                    QualifiedName.of(x.getIndexName().getValue()),
                    after.getQualifiedName(), x.getIndexColumnNames(), x.getProperties())).collect(Collectors.toList());
            builder.addAll(collect);
            return builder.build();
        }
        if (!afterNotEmpty) {
            List<DropIndex> dropConstraints = tableIndexList.stream().map(
                x -> new DropIndex(QualifiedName.of(x.getIndexName().getValue()), before.getQualifiedName())
            ).collect(Collectors.toList());
            builder.addAll(dropConstraints);
            return builder.build();
        }
        List<TableIndex> tableIndices = Lists.newArrayList(afterTableIndex);
        for (TableIndex beforeConstraint : tableIndexList) {
            if (isNotContain(tableIndices, beforeConstraint)) {
                builder.add(new DropIndex(
                    QualifiedName.of(beforeConstraint.getIndexName().getValue()),
                    after.getQualifiedName()));
            } else {
                tableIndices.remove(beforeConstraint);
            }
        }
        List<CreateIndex> list = tableIndices.stream().map(x -> new CreateIndex(
            QualifiedName.of(x.getIndexName().getValue()), after.getQualifiedName(), x.getIndexColumnNames(),
            x.getProperties()))
            .collect(Collectors.toList());
        builder.addAll(list);
        return builder.build();
    }

    private boolean isNotContain(List<TableIndex> tableIndexList, TableIndex c) {
        return !tableIndexList.contains(c);
    }
}
