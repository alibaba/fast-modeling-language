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

import com.aliyun.fastmodel.core.tree.AbstractFmlNode;
import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.google.common.collect.ImmutableList;
import lombok.Getter;
import lombok.ToString;

/**
 * 更改规则元素
 *
 * @author panguanjing
 * @date 2021/6/3
 */
@Getter
@ToString
public class ChangeRuleElement extends AbstractFmlNode {

    private Identifier oldRuleName;

    private RuleDefinition ruleDefinition;

    public ChangeRuleElement(Identifier oldRuleName,
                             RuleDefinition ruleDefinition) {
        this.oldRuleName = oldRuleName;
        this.ruleDefinition = ruleDefinition;
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of(oldRuleName, ruleDefinition);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitChangeRuleElement(this, context);
    }
}
