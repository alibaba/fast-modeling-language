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

package com.aliyun.fastmodel.core.tree.statement.element;

import java.util.List;

import com.aliyun.fastmodel.core.tree.AbstractFmlNode;
import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import lombok.Getter;

/**
 * FML中注释的信息，多行注释信息
 *
 * @author panguanjing
 * @date 2021/4/21
 */
@Getter
public class MultiComment extends AbstractFmlNode {

    private final Node node;

    public MultiComment(Node node) {
        Preconditions.checkNotNull(node, "comment node can't be null");
        if (node instanceof MultiComment) {
            throw new IllegalArgumentException("argument can't be the nest");
        }
        this.node = node;
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of(node);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitMultiComment(this, context);
    }
}
