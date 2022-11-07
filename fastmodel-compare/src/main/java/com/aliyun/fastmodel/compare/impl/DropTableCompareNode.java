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
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.statement.table.DropTable;
import com.google.common.collect.ImmutableList;

/**
 * DropTableCompareNode
 *
 * @author panguanjing
 * @date 2021/2/24
 */
public class DropTableCompareNode extends BaseCompareNode<DropTable> {

    @Override
    public List<BaseStatement> compareResult(DropTable before, DropTable after, CompareStrategy strategy) {
        if (after == null) {
            throw new IllegalArgumentException("after statement can't be null");
        }
        return ImmutableList.of(after);
    }
}
