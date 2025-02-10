/*
 * Copyright [2024] [name of copyright owner]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.fastmodel.transform.api.extension.tree.partition.sub;

import java.util.List;

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.sub.element.BasePartitionElement;
import com.aliyun.fastmodel.transform.api.extension.visitor.ExtensionAstVisitor;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import lombok.Getter;

/**
 * sub range partition
 *
 * @author panguanjing
 * @date 2024/2/6
 */
@Getter
public class SubRangePartition extends BaseSubPartition {

    private final BaseExpression expression;

    private final List<Identifier> columnList;

    private final List<BasePartitionElement> singleRangePartitionList;

    public SubRangePartition(BaseExpression expression, List<Identifier> columnList, List<BasePartitionElement> singleRangePartitionList) {
        this.singleRangePartitionList = singleRangePartitionList;
        Preconditions.checkArgument(expression != null || columnList != null, "expression or columnList need not null");
        this.expression = expression;
        this.columnList = columnList;
    }

    @Override
    public List<? extends Node> getChildren() {
        Builder<Node> nodes = ImmutableList.<Node>builder();
        if (expression != null) {
            nodes.add(expression);
        }
        if (columnList != null) {
            nodes.addAll(columnList);
        }
        if (singleRangePartitionList != null) {
            nodes.addAll(singleRangePartitionList);
        }
        return nodes.build();
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        ExtensionAstVisitor<R, C> extensionAstVisitor = (ExtensionAstVisitor<R, C>)visitor;
        return extensionAstVisitor.visitSubRangePartition(this, context);
    }
}
