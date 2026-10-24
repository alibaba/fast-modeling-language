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

import java.util.Arrays;
import java.util.List;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.statement.rule.DropRule;
import com.aliyun.fastmodel.core.tree.statement.rule.PartitionSpec;
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
 * 删除规则处理
 *
 * @author panguanjing
 * @date 2021/6/2
 */
@BuilderAnnotation(values = DropRule.class, dialect = DialectName.Constants.MAXCOMPUTE)
@AutoService(StatementBuilder.class)
public class DropRulesBuilder extends BaseRulesBuilder {
    @Override
    public DialectNode build(BaseStatement source, MaxComputeContext context) {
        DropRule dropRule = (DropRule)source;
        if (dropRule.getTableName() == null) {
            throw new UnsupportedOperationException("unsupported build dropRule without tableName");
        }
        EntityRules entityRules = new EntityRules();
        entityRules.setEntityLevel(EntityLevel.getByRulesLevel(dropRule.getRulesLevel()));
        List<PartitionSpec> partitionSpecList = dropRule.getPartitionSpecList();
        entityRules.setMatchExpression(toMatchExpression(partitionSpecList));
        entityRules.setTableName(dropRule.getTableName().getSuffix());
        BaseRuleModel baseRuleModel = new BaseRuleModel();
        baseRuleModel.setAction(Action.DELETE);
        baseRuleModel.setProperty(dropRule.getRuleOrColumn().getValue());
        entityRules.setRuleModelList(Arrays.asList(baseRuleModel));
        return new DialectNode(JSON.toJSONString(entityRules, SerializerFeature.PrettyFormat));

    }
}
