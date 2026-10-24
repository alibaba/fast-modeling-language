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

package com.aliyun.fastmodel.transform.mc.builder.rules;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.statement.rule.ChangeRuleElement;
import com.aliyun.fastmodel.core.tree.statement.rule.ChangeRules;
import com.aliyun.fastmodel.core.tree.statement.rule.PartitionSpec;
import com.aliyun.fastmodel.core.tree.util.RuleUtil;
import com.aliyun.fastmodel.transform.api.builder.BuilderAnnotation;
import com.aliyun.fastmodel.transform.api.builder.StatementBuilder;
import com.aliyun.fastmodel.transform.api.dialect.DialectName;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.api.domain.dqc.BaseRuleModel;
import com.aliyun.fastmodel.transform.api.domain.dqc.BaseRuleModel.Action;
import com.aliyun.fastmodel.transform.api.domain.dqc.EntityRules;
import com.aliyun.fastmodel.transform.api.domain.dqc.constant.EntityLevel;
import com.aliyun.fastmodel.transform.mc.context.MaxComputeContext;
import com.google.auto.service.AutoService;

/**
 * 修改规则处理
 *
 * @author panguanjing
 * @date 2021/6/2
 */
@BuilderAnnotation(values = ChangeRules.class, dialect = DialectName.Constants.MAXCOMPUTE)
@AutoService(StatementBuilder.class)
public class ChangeRulesBuilder extends BaseRulesBuilder {
    @Override
    public DialectNode build(BaseStatement source, MaxComputeContext context) {
        ChangeRules changeRules = (ChangeRules)source;
        EntityRules entityRules = new EntityRules();
        entityRules.setEntityLevel(EntityLevel.SQL);
        List<PartitionSpec> partitionSpecList = changeRules.getPartitionSpecList();
        String matchExpression = toMatchExpression(partitionSpecList);
        entityRules.setMatchExpression(matchExpression);
        QualifiedName tableName = changeRules.getTableName();
        entityRules.setTableName(tableName.getSuffix());
        entityRules.setDialectName(DialectName.MAXCOMPUTE);
        List<ChangeRuleElement> ruleDefinitions = changeRules.getRuleDefinitions();
        List<BaseRuleModel> baseRuleModels = new ArrayList<>();
        for (ChangeRuleElement r : ruleDefinitions) {
            BaseRuleModel e = toBaseRuleModel(r.getRuleDefinition(), tableName, matchExpression, context);
            e.setOldProperty(r.getOldRuleName().getValue());
            if (e.getAction() == Action.DELETE) {
                e.setRuleName(RuleUtil.replaceRuleName(e.getRuleName(), e.getProperty(), e.getOldProperty()));
            }
            baseRuleModels.add(e);
        }
        entityRules.setRuleModelList(baseRuleModels);
        String result = JSON.toJSONString(entityRules, SerializerFeature.PrettyFormat);
        return new DialectNode(result, true);
    }
}
