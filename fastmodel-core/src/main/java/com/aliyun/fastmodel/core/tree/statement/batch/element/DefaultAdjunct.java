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

package com.aliyun.fastmodel.core.tree.statement.batch.element;

import java.util.List;

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.batch.AbstractBatchElement;
import lombok.Getter;

/**
 * 默认的修饰词
 *
 * @author panguanjing
 * @date 2021/2/7
 */
@Getter
public class DefaultAdjunct extends AbstractBatchElement {

    private final List<Identifier> adjuncts;

    public DefaultAdjunct(List<Identifier> adjuncts) {this.adjuncts = adjuncts;}

    @Override
    public List<? extends Node> getChildren() {
        return adjuncts;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDefaultAdjunct(this, context);
    }
}
