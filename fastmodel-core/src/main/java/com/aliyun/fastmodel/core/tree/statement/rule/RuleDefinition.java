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
import com.aliyun.fastmodel.core.tree.AliasedName;
import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.google.common.collect.ImmutableList;
import lombok.Getter;
import lombok.ToString;

/**
 * 规则定义
 *
 * @author panguanjing
 * @date 2021/5/29
 */
@Getter
@ToString
public class RuleDefinition extends AbstractNode {
    private final RuleGrade ruleGrade;

    private final Identifier ruleName;

    private final RuleStrategy ruleStrategy;

    private final Comment comment;

    private final AliasedName aliasedName;

    private final boolean enable;

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of(ruleStrategy);
    }

    protected RuleDefinition(RuleDefinitionBuilder builder) {
        ruleGrade = builder.ruleGrade;
        ruleName = builder.ruleName;
        ruleStrategy = builder.ruleStrategy;
        comment = builder.comment;
        aliasedName = builder.aliasedName;
        enable = builder.enable;
    }

    public static RuleDefinitionBuilder builder() {
        return new RuleDefinitionBuilder();
    }

    public static class RuleDefinitionBuilder {
        private RuleGrade ruleGrade = RuleGrade.WEAK;
        private Identifier ruleName;
        private RuleStrategy ruleStrategy;
        private Comment comment;
        private AliasedName aliasedName;
        private boolean enable = true;

        public RuleDefinitionBuilder aliasedName(AliasedName aliasedName) {
            this.aliasedName = aliasedName;
            return this;
        }

        public RuleDefinitionBuilder ruleGrade(RuleGrade ruleGrade) {
            this.ruleGrade = ruleGrade;
            return this;
        }

        public RuleDefinitionBuilder ruleName(Identifier ruleName) {
            this.ruleName = ruleName;
            return this;
        }

        public RuleDefinitionBuilder comment(Comment comment) {
            this.comment = comment;
            return this;
        }

        public RuleDefinitionBuilder ruleStrategy(RuleStrategy ruleStrategy) {
            this.ruleStrategy = ruleStrategy;
            return this;
        }

        public RuleDefinitionBuilder enable(boolean enable) {
            this.enable = enable;
            return this;
        }

        public RuleDefinition build() {
            return new RuleDefinition(this);
        }
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitRuleDefinition(this, context);
    }
}
