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

package com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.element;

import java.util.List;

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.expr.literal.LongLiteral;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.desc.SingleRangePartition;
import com.aliyun.fastmodel.transform.oceanbase.parser.visitor.OceanBaseMysqlAstVisitor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import lombok.Getter;

/**
 * RangePartitionElement
 *
 * @author panguanjing
 * @date 2024/2/6
 */
@Getter
public class RangePartitionElement extends PartitionElement {

    private final LongLiteral idCount;

    private final SingleRangePartition singleRangePartition;

    private final SubPartitionList subPartitionList;

    public RangePartitionElement(LongLiteral idCount, SingleRangePartition singleRangePartition, SubPartitionList subPartitionList) {
        this.idCount = idCount;
        this.singleRangePartition = singleRangePartition;
        this.subPartitionList = subPartitionList;
    }

    @Override
    public List<? extends Node> getChildren() {
        ImmutableList.Builder<Node> immutableList = new Builder();
        if (singleRangePartition != null) {
            immutableList.add(singleRangePartition);
        }
        if (subPartitionList != null) {
            immutableList.add(subPartitionList);
        }
        if (idCount != null) {
            immutableList.add(idCount);
        }
        return immutableList.build();
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        OceanBaseMysqlAstVisitor<R, C> astVisitor = (OceanBaseMysqlAstVisitor<R, C>)visitor;
        return astVisitor.visitRangePartitionElement(this, context);
    }
}
