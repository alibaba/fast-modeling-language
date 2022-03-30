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

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.statement.BaseCreate;
import com.aliyun.fastmodel.core.tree.statement.dqc.check.BaseCheckElement;
import com.aliyun.fastmodel.core.tree.statement.element.CreateElement;
import com.aliyun.fastmodel.core.tree.statement.rule.PartitionSpec;
import lombok.Getter;

/**
 * 创建DQC规则内容
 *
 * @author panguanjing
 * @date 2021/6/7
 */
@Getter
public class CreateDqcRule extends BaseCreate {

    private final QualifiedName tableName;

    private final List<PartitionSpec> partitionSpecList;

    private final List<BaseCheckElement> baseCheckElements;

    private CreateDqcRule(DqcRuleBuilder rulesBuilder) {
        super(rulesBuilder.createElement);
        tableName = rulesBuilder.tableName;
        partitionSpecList = rulesBuilder.partitionSpecList;
        baseCheckElements = rulesBuilder.baseCheckElements;
    }

    public static DqcRuleBuilder builder() {
        return new DqcRuleBuilder();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateDqcRule(this, context);
    }

    public static class DqcRuleBuilder {
        private CreateElement createElement;
        private QualifiedName tableName;
        private List<PartitionSpec> partitionSpecList;
        private List<BaseCheckElement> baseCheckElements;

        public DqcRuleBuilder createElement(CreateElement createElement) {
            this.createElement = createElement;
            return this;
        }

        public DqcRuleBuilder tableName(QualifiedName tableName) {
            this.tableName = tableName;
            return this;
        }

        public DqcRuleBuilder partitionSpecList(List<PartitionSpec> list) {
            partitionSpecList = list;
            return this;
        }

        public DqcRuleBuilder ruleDefinitions(List<BaseCheckElement> baseCheckElements) {
            this.baseCheckElements = baseCheckElements;
            return this;
        }

        public CreateDqcRule build() {
            return new CreateDqcRule(this);
        }
    }
}
