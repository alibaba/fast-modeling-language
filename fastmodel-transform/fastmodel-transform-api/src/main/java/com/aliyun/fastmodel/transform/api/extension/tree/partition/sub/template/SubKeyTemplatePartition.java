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

package com.aliyun.fastmodel.transform.api.extension.tree.partition.sub.template;

import java.util.List;

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.sub.BaseSubPartition;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.sub.element.SubPartitionList;
import com.aliyun.fastmodel.transform.api.extension.visitor.ExtensionAstVisitor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import lombok.Getter;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2024/2/18
 */
@Getter
public class SubKeyTemplatePartition extends BaseSubPartition {

    private final List<Identifier> columnList;

    private final SubPartitionList subPartitionList;

    public SubKeyTemplatePartition(List<Identifier> columnList, SubPartitionList subPartitionList) {
        this.columnList = columnList;
        this.subPartitionList = subPartitionList;
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        ExtensionAstVisitor<R, C> astVisitor = (ExtensionAstVisitor<R, C>)visitor;
        return astVisitor.visitSubKeyTemplatePartition(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        Builder<Node> builder = ImmutableList.builder();
        if (columnList != null) {
            builder.addAll(columnList);
        }
        if (subPartitionList != null) {
            builder.add(subPartitionList);
        }

        return builder.build();
    }
}
