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
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.BaseOperatorStatement;
import lombok.Getter;

/**
 * 删除Rules的处理
 *
 * @author panguanjing
 * @date 2021/5/30
 */
@Getter
public class DropRule extends BaseOperatorStatement {

    private final RulesLevel rulesLevel;
    /**
     * 依赖的表名
     */
    private final QualifiedName tableName;

    /**
     * 分区表达式
     */
    private final List<PartitionSpec> partitionSpecList;

    /**
     * 当tableName不为空时候，这里就是列名
     */
    private final Identifier ruleOrColumn;

    public DropRule(RulesLevel rulesLevel, QualifiedName tableName,
                    List<PartitionSpec> partitionSpecList, Identifier ruleOrColumn) {
        super(null);
        this.rulesLevel = rulesLevel;
        this.tableName = tableName;
        this.partitionSpecList = partitionSpecList;
        this.ruleOrColumn = ruleOrColumn;
    }

    public DropRule(RulesLevel rulesLevel, QualifiedName qualifiedName,
                    Identifier ruleOrColumn) {
        super(qualifiedName);
        this.rulesLevel = rulesLevel;
        tableName = null;
        partitionSpecList = null;
        this.ruleOrColumn = ruleOrColumn;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDropRule(this, context);
    }
}
