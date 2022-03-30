/*
 * Copyright 2021-2022 Alibaba Group Holding Ltd.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.aliyun.fastmodel.compare.table;

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
