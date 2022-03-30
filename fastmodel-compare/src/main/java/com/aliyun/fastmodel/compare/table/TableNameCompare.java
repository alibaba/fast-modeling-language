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

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.RenameTable;
import com.google.common.collect.ImmutableList;

/**
 * TableNameCompare
 *
 * @author panguanjing
 * @date 2021/8/30
 */
public class TableNameCompare implements TableElementCompare {
    @Override
    public List<BaseStatement> compareTableElement(CreateTable before, CreateTable after) {
        QualifiedName qualifiedName = before.getQualifiedName();
        if (!qualifiedName.getSuffix().equalsIgnoreCase(after.getQualifiedName().getSuffix())) {
            return ImmutableList.of(new RenameTable(before.getQualifiedName(), after.getQualifiedName()));
        }
        return ImmutableList.of();
    }
}
