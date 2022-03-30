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
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.BaseOperatorStatement;
import com.aliyun.fastmodel.core.tree.statement.rule.PartitionSpec;
import com.aliyun.fastmodel.core.tree.util.RuleUtil;
import lombok.Getter;

/**
 * 删除dqc的规则
 *
 * @author panguanjing
 * @date 2021/6/27
 */
@Getter
public class DropDqcRule extends BaseOperatorStatement {

    private final QualifiedName tableName;

    /**
     * 分区表达式
     */
    private final List<PartitionSpec> partitionSpecList;

    /**
     * 当tableName不为空时候，这里就是列名
     */
    private final Identifier ruleOrColumn;

    public DropDqcRule(QualifiedName qualifiedName, QualifiedName tableName,
                       List<PartitionSpec> partitionSpecList,
                       Identifier ruleOrColumn) {
        super(qualifiedName);
        this.tableName = tableName;
        this.partitionSpecList = partitionSpecList;
        this.ruleOrColumn = ruleOrColumn;
    }

    public DropDqcRule(QualifiedName tableName,
                       List<PartitionSpec> partitionSpecList,
                       Identifier ruleOrColumn) {
        this(RuleUtil.generateRulesName(tableName), tableName, partitionSpecList, ruleOrColumn);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDropDqcRule(this, context);
    }
}
