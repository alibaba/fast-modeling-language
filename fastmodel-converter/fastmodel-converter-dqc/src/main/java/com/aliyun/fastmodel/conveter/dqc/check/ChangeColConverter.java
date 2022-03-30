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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.converter.spi.ConvertContext;
import com.aliyun.fastmodel.converter.spi.StatementConverter;
import com.aliyun.fastmodel.conveter.dqc.BaseDqcStatementConverter;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.dqc.ChangeDqcRule;
import com.aliyun.fastmodel.core.tree.statement.dqc.ChangeDqcRuleElement;
import com.aliyun.fastmodel.core.tree.statement.dqc.check.BaseCheckElement;
import com.aliyun.fastmodel.core.tree.statement.rule.PartitionSpec;
import com.aliyun.fastmodel.core.tree.statement.rule.RuleDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.ChangeCol;
import com.aliyun.fastmodel.core.tree.util.RuleUtil;
import com.google.auto.service.AutoService;

/**
 * 更改列切换
 *
 * @author panguanjing
 * @date 2021/6/2
 */
@AutoService(StatementConverter.class)
public class ChangeColConverter extends BaseDqcStatementConverter<ChangeCol> {
    @Override
    public BaseStatement convert(ChangeCol source, ConvertContext context) {
        Identifier oldColName = source.getOldColName();
        Identifier newColName = source.getNewColName();
        List<PartitionSpec> partitionSpecList = getPartitionSpec(context);
        //如果相等，返回添加规则操作
        List<RuleDefinition> list = toRuleDefinition(Arrays.asList(source.getColumnDefinition()), true);
        List<ChangeDqcRuleElement> changeRuleElements = new ArrayList<>();
        if (list.isEmpty()) {
            //如果不相等的话，才进行添加。
            if (!Objects.equals(oldColName, newColName)) {
                RuleDefinition build = RuleDefinition.builder().ruleName(newColName).build();
                BaseCheckElement baseCheckElement = toSingleCheckElement(build);
                ChangeDqcRuleElement ruleElement = new ChangeDqcRuleElement(oldColName,
                    baseCheckElement);
                changeRuleElements.add(ruleElement);
            }
        } else {
            changeRuleElements = list.stream().map(x -> {
                return new ChangeDqcRuleElement(oldColName, toSingleCheckElement(x));
            }).collect(Collectors.toList());
        }
        if (changeRuleElements.isEmpty()) {
            return null;
        }
        ChangeDqcRule changeRules = new ChangeDqcRule(
            RuleUtil.generateRulesName(source.getQualifiedName()),
            source.getQualifiedName(),
            partitionSpecList,
            changeRuleElements
        );
        return changeRules;
    }

}
