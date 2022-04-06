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

package com.aliyun.fastmodel.compare.impl.table;

import java.util.List;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.statement.table.AddPartitionCol;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.DropPartitionCol;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.google.common.collect.ImmutableList;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/8/30
 */
public class PartitionByCompare extends ColumnCompare implements TableElementCompare {
    @Override
    public List<BaseStatement> compareTableElement(CreateTable before, CreateTable after) {
        PartitionedBy beforePartitionedBy = before.getPartitionedBy();
        PartitionedBy afterPartitionedBy = after.getPartitionedBy();
        boolean beforeEmpty = beforePartitionedBy == null || !beforePartitionedBy.isNotEmpty();
        boolean afterNotEmpty = afterPartitionedBy != null && afterPartitionedBy.isNotEmpty();
        if (beforeEmpty && !afterNotEmpty) {
            return ImmutableList.of();
        }
        /**
         * 1. if before is null, after not null, then statement return add partitioned by
         * 2. if before is not null , after is null, then statement return drop partitioned by
         * 3. if after not contains before elements, then statement return drop partitioned,
         * 4. if after contains before elements then statement not return .
         */
        ImmutableList.Builder<BaseStatement> builder = ImmutableList.builder();
        if (beforeEmpty && afterNotEmpty) {
            List<AddPartitionCol> collect = afterPartitionedBy.getColumnDefinitions().stream().map(
                    x -> new AddPartitionCol(after.getQualifiedName(), x))
                .collect(Collectors.toList());
            builder.addAll(collect);
            return builder.build();
        }
        if (!afterNotEmpty) {
            List<ColumnDefinition> columnDefinitions = beforePartitionedBy.getColumnDefinitions();
            List<DropPartitionCol> collect = columnDefinitions.stream().map(
                x -> new DropPartitionCol(after.getQualifiedName(), x.getColName())).collect(Collectors.toList());
            builder.addAll(collect);
            return builder.build();
        }
        List<ColumnDefinition> beforePartitionedByColumnDefinitions = beforePartitionedBy.getColumnDefinitions();
        List<ColumnDefinition> afterPartitionedByColumnDefinitions = afterPartitionedBy.getColumnDefinitions();
        return incrementCompare(after.getQualifiedName(), beforePartitionedByColumnDefinitions,
            afterPartitionedByColumnDefinitions,
            (q, c) -> {
                return new DropPartitionCol(q, c.getColName());
            },
            (q, c) -> {
                return c.stream().map(
                    x -> {
                        return new AddPartitionCol(q, x);
                    }
                ).collect(Collectors.toList());
            }
        ).build();
    }
}
