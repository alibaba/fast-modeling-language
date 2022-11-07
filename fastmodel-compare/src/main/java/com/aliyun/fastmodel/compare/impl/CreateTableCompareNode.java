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

package com.aliyun.fastmodel.compare.impl;

import java.util.List;

import com.aliyun.fastmodel.compare.CompareStrategy;
import com.aliyun.fastmodel.compare.impl.table.AliasCompare;
import com.aliyun.fastmodel.compare.impl.table.ColumnCompare;
import com.aliyun.fastmodel.compare.impl.table.CommentCompare;
import com.aliyun.fastmodel.compare.impl.table.ConstraintCompare;
import com.aliyun.fastmodel.compare.impl.table.PartitionByCompare;
import com.aliyun.fastmodel.compare.impl.table.PropertiesCompare;
import com.aliyun.fastmodel.compare.impl.table.TableElementCompare;
import com.aliyun.fastmodel.compare.impl.table.TableIndexCompare;
import com.aliyun.fastmodel.compare.impl.table.TableNameCompare;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.DropTable;
import com.google.common.collect.ImmutableList;

/**
 * 创建表的比对处理
 *
 * @author panguanjing
 * @date 2021/2/2
 */
public class CreateTableCompareNode extends BaseCompareNode<CreateTable> {

    private List<TableElementCompare> tableElementCompareList;

    public CreateTableCompareNode() {
        tableElementCompareList = ImmutableList.of(
            new TableNameCompare(),
            new AliasCompare(),
            new ColumnCompare(),
            new TableIndexCompare(),
            new CommentCompare(),
            new PartitionByCompare(),
            new ConstraintCompare(),
            new PropertiesCompare()
        );
    }

    /**
     * 1. 如果before是空，after不是为空，那么直接返回after的语句
     * 2. 如果before不为空，after是空，那么返回drop语句
     * 3. 如果before和after不为空，按照对象比较的情况进行生成计算，这里可以进行并行计算内容处理
     *
     * @return
     */
    @Override
    public List<BaseStatement> compareResult(CreateTable before, CreateTable after, CompareStrategy strategy) {
        ImmutableList.Builder<BaseStatement> builder = ImmutableList.builder();
        if (after == null) {
            DropTable dropTable = new DropTable(before.getQualifiedName(), true);
            builder.add(dropTable);
            return builder.build();
        }
        if (before == null) {
            //之前是空的，还要增加判断比对策略
            if (strategy == CompareStrategy.INCREMENTAL) {
                builder.add(after);
                return builder.build();
            } else if (strategy == CompareStrategy.FULL) {
                //全量的话，先生成删除内容
                DropTable dropTable = new DropTable(after.getQualifiedName(), true);
                builder.add(dropTable);
                builder.add(after);
                return builder.build();
            }
        }
        if (strategy == CompareStrategy.FULL) {
            DropTable dropTable = new DropTable(before.getQualifiedName(), true);
            builder.add(dropTable);
            builder.add(after);
            return builder.build();
        }
        for (TableElementCompare tableElementCompare : tableElementCompareList) {
            List<BaseStatement> baseStatementList = tableElementCompare.compareTableElement(before, after);
            builder.addAll(baseStatementList);
        }
        return builder.build();
    }

}
