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

package com.aliyun.fastmodel.core.tree.statement.dqc;

import java.util.List;

import com.aliyun.fastmodel.core.tree.AbstractNode;
import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.dqc.check.BaseCheckElement;
import com.google.common.collect.ImmutableList;
import lombok.Getter;

/**
 * 更新dqc 规则元素
 *
 * @author panguanjing
 * @date 2021/6/27
 */
@Getter
public class ChangeDqcRuleElement extends AbstractNode {

    private final Identifier oldRuleName;

    private final BaseCheckElement baseCheckElement;

    public ChangeDqcRuleElement(Identifier oldRuleName,
                                BaseCheckElement baseCheckElement) {
        this.oldRuleName = oldRuleName;
        this.baseCheckElement = baseCheckElement;
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of(oldRuleName, baseCheckElement);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitChangeDqcRuleElement(this, context);
    }
}
