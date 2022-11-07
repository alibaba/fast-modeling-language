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
import com.aliyun.fastmodel.conveter.dqc.util.FmlTableUtil;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.statement.dqc.CreateDqcRule;
import com.aliyun.fastmodel.core.tree.statement.dqc.check.BaseCheckElement;
import com.aliyun.fastmodel.core.tree.statement.element.CreateElement;
import com.aliyun.fastmodel.core.tree.statement.rule.RuleDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.util.RuleUtil;
import com.google.auto.service.AutoService;

/**
 * 创建规则的转换器
 *
 * @author panguanjing
 * @date 2021/5/31
 */
@AutoService(StatementConverter.class)
public class CreateTableConverter extends BaseDqcStatementConverter<CreateTable,CreateDqcRule> {

    @Override
    public CreateDqcRule convert(CreateTable source, ConvertContext convertContext) {
        List<RuleDefinition> list = toRuleDefinition(source, true, convertContext).stream().filter(x -> x.isEnable()).filter(
                distinctByKey(ruleDefinition -> {
                    return getDistinctKey(ruleDefinition);
                })).
            collect(Collectors.toList());
        if (list.isEmpty()) {
            //没有规则不需要生成
            return null;
        }
        List<BaseCheckElement> baseCheckElements = toCheckElement(list);
        QualifiedName rulesName = RuleUtil.generateRulesName(source.getQualifiedName());
        CreateDqcRule createRules = CreateDqcRule.builder()
            .tableName(source.getQualifiedName())
            .createElement(
                CreateElement.builder()
                    .qualifiedName(rulesName)
                    .build())
            .partitionSpecList(FmlTableUtil.getPartitionSpec(source))
            .ruleDefinitions(baseCheckElements)
            .build();
        return createRules;
    }


}
