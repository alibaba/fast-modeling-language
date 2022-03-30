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
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.statement.BaseOperatorStatement;
import com.aliyun.fastmodel.core.tree.statement.dqc.check.BaseCheckElement;
import com.aliyun.fastmodel.core.tree.statement.rule.PartitionSpec;
import com.aliyun.fastmodel.core.tree.util.RuleUtil;
import com.google.common.collect.ImmutableList;
import lombok.Getter;

/**
 * 增加dqcRule
 * <pre>
 *     ALTER DQC_RULE
 * </pre>
 *
 * @author panguanjing
 * @date 2021/6/27
 */
@Getter
public class AddDqcRule extends BaseOperatorStatement {

    private final QualifiedName tableName;

    private final List<PartitionSpec> partitionSpecList;

    private final List<BaseCheckElement> baseCheckElements;

    public AddDqcRule(QualifiedName tableName,
                      List<PartitionSpec> partitionSpecList,
                      List<BaseCheckElement> baseCheckElements) {
        this(RuleUtil.generateRulesName(tableName), tableName, partitionSpecList, baseCheckElements);
    }

    public AddDqcRule(QualifiedName qualifiedName, QualifiedName tableName,
                      List<PartitionSpec> partitionSpecList,
                      List<BaseCheckElement> baseCheckElements) {
        super(qualifiedName);
        this.tableName = tableName;
        this.partitionSpecList = partitionSpecList;
        this.baseCheckElements = baseCheckElements;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAddDqcRule(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        ImmutableList.Builder<Node> builder = ImmutableList.builder();
        if (partitionSpecList != null) {
            builder.addAll(partitionSpecList);
        }
        if (baseCheckElements != null) {
            builder.addAll(baseCheckElements);
        }
        return builder.build();
    }
}
