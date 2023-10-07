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

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.statement.BaseCreate;
import com.aliyun.fastmodel.core.tree.statement.element.CreateElement;
import lombok.Getter;

/**
 * 创建规则集合
 *
 * @author panguanjing
 * @date 2021/5/29
 */
@Getter
public class CreateRules extends BaseCreate {

    private final RulesLevel rulesLevel;

    private final QualifiedName tableName;

    private final List<PartitionSpec> partitionSpecList;

    private final List<RuleDefinition> ruleDefinitions;

    private CreateRules(RulesBuilder builder) {
        super(builder.createElement);
        rulesLevel = builder.rulesLevel;
        tableName = builder.tableName;
        partitionSpecList = builder.partitionSpecList;
        ruleDefinitions = builder.ruleDefinitions;
    }

    public static RulesBuilder builder() {
        return new RulesBuilder();
    }

    public static class RulesBuilder {
        private RulesLevel rulesLevel;
        private CreateElement createElement;
        private QualifiedName tableName;
        private List<PartitionSpec> partitionSpecList;
        private List<RuleDefinition> ruleDefinitions;

        public RulesBuilder ruleLevel(RulesLevel rulesLevel) {
            this.rulesLevel = rulesLevel;
            return this;
        }

        public RulesBuilder createElement(CreateElement createElement) {
            this.createElement = createElement;
            return this;
        }

        public RulesBuilder tableName(QualifiedName tableName) {
            this.tableName = tableName;
            return this;
        }

        public RulesBuilder partitionSpecList(List<PartitionSpec> list) {
            partitionSpecList = list;
            return this;
        }

        public RulesBuilder ruleDefinitions(List<RuleDefinition> ruleDefinitions) {
            this.ruleDefinitions = ruleDefinitions;
            return this;
        }

        public CreateRules build() {
            return new CreateRules(this);
        }
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateRules(this, context);
    }
}
