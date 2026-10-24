/*
 * Copyright 2021 Alibaba Group Holding Ltd.
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

package com.aliyun.fastmodel.transform.mc.builder.dqc;

import java.util.List;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.statement.dqc.AddDqcRule;
import com.aliyun.fastmodel.core.tree.statement.dqc.check.BaseCheckElement;
import com.aliyun.fastmodel.core.tree.statement.rule.PartitionSpec;
import com.aliyun.fastmodel.core.tree.statement.rule.RuleDefinition;
import com.aliyun.fastmodel.transform.api.builder.BuilderAnnotation;
import com.aliyun.fastmodel.transform.api.builder.StatementBuilder;
import com.aliyun.fastmodel.transform.api.dialect.DialectName;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.api.domain.dqc.EntityRules;
import com.aliyun.fastmodel.transform.api.domain.dqc.constant.EntityLevel;
import com.aliyun.fastmodel.transform.mc.context.MaxComputeContext;
import com.google.auto.service.AutoService;

/**
 * QueryRowToColBuilder:
 *
 * @author panguanjing
 * @date 2020/10/20
 */
@BuilderAnnotation(values = AddDqcRule.class, dialect = DialectName.Constants.MAXCOMPUTE)
@AutoService(StatementBuilder.class)
public class AddDqcRuleBuilder extends BaseDqcRuleBuilder {

    @Override
    public DialectNode build(BaseStatement source, MaxComputeContext context) {
        AddDqcRule createRules = (AddDqcRule)source;
        return new DialectNode(toNode(createRules, context));
    }

    /**
     * 将创建规则转换为可执行的参数
     *
     * @param createRules
     * @param context
     * @return 转换对象
     */
    private String toNode(AddDqcRule createRules, MaxComputeContext context) {
        EntityRules entityRules = new EntityRules();
        entityRules.setEntityLevel(EntityLevel.SQL);
        List<PartitionSpec> partitionSpecList = createRules.getPartitionSpecList();
        String matchExpression = toMatchExpression(partitionSpecList);
        entityRules.setMatchExpression(matchExpression);
        QualifiedName tableName = createRules.getTableName();
        entityRules.setTableName(tableName.getSuffix());
        entityRules.setDialectName(DialectName.MAXCOMPUTE);
        List<BaseCheckElement> baseCheckElements = createRules.getBaseCheckElements();
        List<RuleDefinition> ruleDefinitions = toRuleDefinition(createRules.getBaseCheckElements());
        entityRules.setRuleModelList(toRuleModelList(ruleDefinitions, tableName, matchExpression, context));
        return JSON.toJSONString(entityRules, SerializerFeature.PrettyFormat);
    }

}
