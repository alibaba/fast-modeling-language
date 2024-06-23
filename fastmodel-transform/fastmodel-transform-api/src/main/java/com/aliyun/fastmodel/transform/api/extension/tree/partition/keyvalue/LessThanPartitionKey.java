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

package com.aliyun.fastmodel.transform.api.extension.tree.partition.keyvalue;

import java.util.List;

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.transform.api.extension.visitor.ExtensionAstVisitor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import lombok.Getter;

/**
 * less than
 *
 * @author panguanjing
 * @date 2023/9/14
 */
@Getter
public class LessThanPartitionKey extends PartitionKey {

    private final boolean maxValue;

    private final ListPartitionValue partitionValues;

    public LessThanPartitionKey(boolean maxValue, ListPartitionValue partitionValues) {
        this.maxValue = maxValue;
        this.partitionValues = partitionValues;
    }

    public LessThanPartitionKey(ListPartitionValue partitionValues) {
        this.partitionValues = partitionValues;
        this.maxValue = false;
    }

    @Override
    public List<? extends Node> getChildren() {
        if (maxValue) {
            return ImmutableList.of();
        }
        return Lists.newArrayList(partitionValues);
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        ExtensionAstVisitor<R, C> extensionVisitor = (ExtensionAstVisitor<R, C>)visitor;
        return extensionVisitor.visitLessThanPartitionKey(this, context);
    }
}
