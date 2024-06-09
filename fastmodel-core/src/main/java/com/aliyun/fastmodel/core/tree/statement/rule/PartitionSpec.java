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

package com.aliyun.fastmodel.core.tree.statement.rule;

import java.util.List;

import com.aliyun.fastmodel.core.tree.AbstractNode;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.literal.BaseLiteral;
import com.google.common.collect.ImmutableList;
import lombok.Getter;

/**
 * 分区表达式描述
 *
 * @author panguanjing
 * @date 2021/5/29
 */
@Getter
public class PartitionSpec extends AbstractNode {

    /**
     * 分区列名
     */
    private final Identifier partitionColName;

    /**
     * 分区的表达式信息
     */
    private final BaseLiteral baseLiteral;

    public PartitionSpec(Identifier partitionColName,
        BaseLiteral baseLiteral) {
        this.partitionColName = partitionColName;
        this.baseLiteral = baseLiteral;
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of();
    }
}
