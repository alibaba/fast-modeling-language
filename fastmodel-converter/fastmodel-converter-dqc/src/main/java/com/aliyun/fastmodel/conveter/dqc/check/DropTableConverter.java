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
import com.aliyun.fastmodel.core.tree.statement.dqc.AddDqcRule;
import com.aliyun.fastmodel.core.tree.statement.rule.PartitionSpec;
import com.aliyun.fastmodel.core.tree.statement.rule.RuleDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.DropTable;
import com.aliyun.fastmodel.core.tree.util.RuleUtil;
import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;

/**
 * DropTableConverter
 *
 * @author panguanjing
 * @date 2021/6/23
 */
@AutoService(StatementConverter.class)
public class DropTableConverter extends BaseDqcStatementConverter<DropTable, AddDqcRule> {

    @Override
    public AddDqcRule convert(DropTable dropTable, ConvertContext context) {
        if (context == null || context.getBeforeStatement() == null) {
            return null;
        }
        BaseStatement beforeStatement = context.getBeforeStatement();
        CreateTable createTable = (CreateTable)beforeStatement;
        List<PartitionSpec> partitionSpecList = getPartitionSpec(createTable);
        List<RuleDefinition> list = toRuleDefinition(createTable, false, context);
        if (list.isEmpty()) {
            return null;
        }
        return new AddDqcRule(RuleUtil.generateRulesName(dropTable.getQualifiedName()),
            dropTable.getQualifiedName(), partitionSpecList, toCheckElement(list));
    }
}
