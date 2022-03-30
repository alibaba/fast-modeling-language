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
import com.aliyun.fastmodel.core.tree.statement.BaseOperatorStatement;
import lombok.Getter;

/**
 * 增加Rules的语句的
 *
 * @author panguanjing
 * @date 2021/6/1
 */
@Getter
public class ChangeRules extends BaseOperatorStatement {

    /**
     * 引用的表
     */
    private final QualifiedName tableName;

    /**
     * 列的内容
     */
    private final List<PartitionSpec> partitionSpecList;

    /**
     * 规则定义
     */
    private final List<ChangeRuleElement> ruleDefinitions;

    public ChangeRules(QualifiedName qualifiedName, List<ChangeRuleElement> ruleDefinitions) {
        super(qualifiedName);
        this.ruleDefinitions = ruleDefinitions;
        tableName = null;
        partitionSpecList = null;
    }

    public ChangeRules(QualifiedName tableName,
                       List<PartitionSpec> partitionSpecList, List<ChangeRuleElement> ruleDefinitions) {
        super(null);
        this.tableName = tableName;
        this.partitionSpecList = partitionSpecList;
        this.ruleDefinitions = ruleDefinitions;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitChangeRules(this, context);
    }
}
