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

package com.aliyun.fastmodel.core.tree.statement.dqc.check;

import java.util.List;

import com.aliyun.fastmodel.core.tree.AbstractNode;
import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.google.common.collect.ImmutableList;
import lombok.Getter;

/**
 * 检查元素内容
 *
 * @author panguanjing
 * @date 2021/6/7
 */
@Getter
public abstract class BaseCheckElement extends AbstractNode {

    protected final Identifier checkName;

    protected final Boolean enforced;

    protected final boolean enable;

    public BaseCheckElement(Identifier checkName, Boolean enforced, boolean enable) {
        this.checkName = checkName;
        this.enforced = enforced;
        this.enable = enable;
    }

    /**
     * get bool expression for every check element
     *
     * @return {@link BaseExpression}
     */
    public abstract BaseExpression getBoolExpression();

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitBaseCheckElement(this, context);
    }
}
