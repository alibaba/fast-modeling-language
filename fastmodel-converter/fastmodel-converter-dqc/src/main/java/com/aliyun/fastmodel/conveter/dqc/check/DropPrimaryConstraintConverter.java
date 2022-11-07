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
import java.util.Optional;

import com.aliyun.fastmodel.converter.spi.ConvertContext;
import com.aliyun.fastmodel.converter.spi.StatementConverter;
import com.aliyun.fastmodel.conveter.dqc.BaseDqcStatementConverter;
import com.aliyun.fastmodel.conveter.dqc.util.FmlTableUtil;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.dqc.AddDqcRule;
import com.aliyun.fastmodel.core.tree.statement.rule.PartitionSpec;
import com.aliyun.fastmodel.core.tree.statement.rule.RuleDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.DropConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.PrimaryConstraint;
import com.aliyun.fastmodel.core.tree.util.RuleUtil;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;

/**
 * 删除constraint内容
 *
 * @author panguanjing
 * @date 2021/6/15
 */
@AutoService(StatementConverter.class)
public class DropPrimaryConstraintConverter extends BaseDqcStatementConverter<DropConstraint, AddDqcRule> {

    @Override
    public AddDqcRule convert(DropConstraint dropConstraint, ConvertContext context) {
        if (context == null || context.getBeforeStatement() == null) {
            return null;
        }
        BaseStatement beforeStatement = context.getBeforeStatement();
        assert beforeStatement instanceof CreateTable;
        CreateTable createTable = (CreateTable)beforeStatement;
        PrimaryConstraint primaryConstraint = getPrimaryConstraint(createTable);
        if (primaryConstraint == null) {
            //如果删除不是主键的constraint
            return null;
        }
        Identifier name = primaryConstraint.getName();
        Identifier constraintName = dropConstraint.getConstraintName();
        if (!StringUtils.equalsIgnoreCase(constraintName.getValue(),
            name.getValue())) {
            return null;
        }
        List<PartitionSpec> partitionSpecList = FmlTableUtil.getPartitionSpec(createTable);
        List<Identifier> colNames = primaryConstraint.getColNames();
        QualifiedName qualifiedName = dropConstraint.getQualifiedName();
        if (colNames.size() == 1) {
            List<RuleDefinition> list = toPrimaryRule(ColumnDefinition.builder().primary(true).colName(colNames.get(0))
                .build(), false);
            return new AddDqcRule(
                RuleUtil.generateRulesName(qualifiedName),
                qualifiedName,
                partitionSpecList,
                toCheckElement(list)
            );
        } else {
            /**
             * 生成一个唯一值
             * 生成一个不为空规则
             */
            RuleDefinition uniqueRule = toUniqueRule(qualifiedName.getSuffix(), colNames, false);
            ImmutableList<RuleDefinition> of = ImmutableList.of(uniqueRule);
            return new AddDqcRule(
                RuleUtil.generateRulesName(qualifiedName),
                qualifiedName,
                partitionSpecList,
                toCheckElement(of)
            );
        }
    }

    public PrimaryConstraint getPrimaryConstraint(CreateTable columns) {
        if (columns.isConstraintEmpty()) {
            return null;
        }
        Optional<BaseConstraint> first = columns.getConstraintStatements().stream().filter(
            x -> x instanceof PrimaryConstraint).findFirst();

        return (PrimaryConstraint)first.orElse(null);
    }
}
