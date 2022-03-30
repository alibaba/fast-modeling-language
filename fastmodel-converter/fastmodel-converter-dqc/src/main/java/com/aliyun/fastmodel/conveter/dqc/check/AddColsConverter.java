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
import java.util.stream.Collectors;

import com.aliyun.fastmodel.converter.spi.ConvertContext;
import com.aliyun.fastmodel.converter.spi.StatementConverter;
import com.aliyun.fastmodel.conveter.dqc.BaseDqcStatementConverter;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.statement.dqc.AddDqcRule;
import com.aliyun.fastmodel.core.tree.statement.dqc.check.BaseCheckElement;
import com.aliyun.fastmodel.core.tree.statement.rule.PartitionSpec;
import com.aliyun.fastmodel.core.tree.statement.rule.RuleDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.AddCols;
import com.aliyun.fastmodel.core.tree.util.RuleUtil;
import com.google.auto.service.AutoService;

/**
 * AddRulesConverter
 *
 * @author panguanjing
 * @date 2021/6/2
 */
@AutoService(StatementConverter.class)
public class AddColsConverter extends BaseDqcStatementConverter<AddCols> {
    @Override
    public BaseStatement convert(AddCols source, ConvertContext context) {
        QualifiedName tableName = source.getQualifiedName();
        List<PartitionSpec> partitionSpecList = getPartitionSpec(context);
        List<RuleDefinition> list = toRuleDefinition(source.getColumnDefineList(), true)
            .stream()
            .collect(Collectors.toList());
        List<BaseCheckElement> checkElements = toCheckElement(list);
        if (checkElements.isEmpty()) {
            return null;
        }
        return new AddDqcRule(
            RuleUtil.generateRulesName(tableName),
            tableName,
            partitionSpecList,
            checkElements
        );
    }
}
