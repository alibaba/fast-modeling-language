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

package com.aliyun.aliyun.transform.zen.compare;

import java.util.List;

import com.aliyun.aliyun.transform.zen.ZenTransformer;
import com.aliyun.fastmodel.compare.CompareNodeExecute;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.ListNode;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.transform.api.compare.CompareContext;
import com.aliyun.fastmodel.transform.api.compare.CompareResult;
import com.aliyun.fastmodel.transform.api.compare.NodeCompare;
import com.aliyun.fastmodel.transform.api.dialect.Dialect;
import com.aliyun.fastmodel.transform.api.dialect.DialectName;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * zenNodeCompare
 *
 * @author panguanjing
 * @date 2021/9/3
 */
@AutoService(NodeCompare.class)
@Dialect(DialectName.Constants.ZEN)
public class ZenNodeCompare implements NodeCompare {

    private ZenTransformer zenTransformer = new ZenTransformer();

    @Override
    public CompareResult compareResult(DialectNode before, DialectNode after, CompareContext context) {
        Preconditions.checkNotNull(context, "context can't be null");
        Preconditions.checkNotNull(context.getQualifiedName(), "context.qualifiedName can't be nul");
        ListNode beforeNode = (before != null && before.isExecutable()) ? (ListNode)zenTransformer.reverse(before) : null;
        ListNode afterNode = (after != null && after.isExecutable()) ? (ListNode)zenTransformer.reverse(after) : null;

        List<ColumnDefinition> beforeColumn = beforeNode != null ? (List<ColumnDefinition>)beforeNode.getChildren()
            : ImmutableList.of();
        List<ColumnDefinition> afterColumn = afterNode != null ? (List<ColumnDefinition>)afterNode.getChildren()
            : ImmutableList.of();

        CreateTable beforeTable = CreateTable.builder()
            .tableName(context.getQualifiedName())
            .columns(beforeColumn)
            .build();

        CreateTable afterTable = CreateTable.builder()
            .tableName(context.getQualifiedName())
            .columns(afterColumn)
            .build();

        List<BaseStatement> baseStatements = CompareNodeExecute.getInstance().compareNode(beforeTable, afterTable,
            context.getCompareStrategy());
        return new CompareResult(beforeTable, afterTable, baseStatements);
    }

}
