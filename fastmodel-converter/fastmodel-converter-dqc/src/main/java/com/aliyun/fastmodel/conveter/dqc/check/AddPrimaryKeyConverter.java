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

package com.aliyun.fastmodel.conveter.dqc.check;

import java.util.List;

import com.aliyun.fastmodel.converter.spi.ConvertContext;
import com.aliyun.fastmodel.converter.spi.StatementConverter;
import com.aliyun.fastmodel.conveter.dqc.BaseDqcStatementConverter;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.dqc.AddDqcRule;
import com.aliyun.fastmodel.core.tree.statement.dqc.check.BaseCheckElement;
import com.aliyun.fastmodel.core.tree.statement.rule.PartitionSpec;
import com.aliyun.fastmodel.core.tree.statement.rule.RuleDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.AddConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.PrimaryConstraint;
import com.aliyun.fastmodel.core.tree.util.RuleUtil;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;

/**
 * 增加主键的converter操作
 *
 * @author panguanjing
 * @date 2021/6/6
 */
@AutoService(StatementConverter.class)
public class AddPrimaryKeyConverter extends BaseDqcStatementConverter<AddConstraint> {

    @Override
    public BaseStatement convert(AddConstraint source, ConvertContext context) {
        BaseConstraint constraintStatement = source.getConstraintStatement();
        if (!(constraintStatement instanceof PrimaryConstraint)) {
            return null;
        }
        PrimaryConstraint primaryConstraint = (PrimaryConstraint)constraintStatement;
        List<Identifier> colNames = primaryConstraint.getColNames();
        QualifiedName qualifiedName = source.getQualifiedName();
        List<PartitionSpec> partitionSpecList = getPartitionSpec(context);
        if (colNames.size() == 1) {
            List<RuleDefinition> ruleDefinitions = toPrimaryRule(ColumnDefinition.builder().primary(true)
                .colName(colNames.get(0)).build(), true);
            List<BaseCheckElement> baseCheckElements = toCheckElement(ruleDefinitions);
            return new AddDqcRule(
                RuleUtil.generateRulesName(qualifiedName),
                qualifiedName,
                partitionSpecList,
                baseCheckElements
            );
        } else {
            /**
             * 生成一个唯一值
             * 生成一个不为空规则
             */
            RuleDefinition uniqueRule = toUniqueRule(source.getQualifiedName().getSuffix(), colNames, true);
            ImmutableList<RuleDefinition> of = ImmutableList.of(uniqueRule);
            List<BaseCheckElement> list = toCheckElement(of);
            return new AddDqcRule(
                RuleUtil.generateRulesName(qualifiedName),
                qualifiedName,
                partitionSpecList,
                list
            );
        }
    }

}
