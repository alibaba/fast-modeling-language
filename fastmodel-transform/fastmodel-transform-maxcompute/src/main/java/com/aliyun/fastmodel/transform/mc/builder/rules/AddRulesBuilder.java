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

import java.util.List;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.statement.rule.AddRules;
import com.aliyun.fastmodel.core.tree.statement.rule.PartitionSpec;
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
@BuilderAnnotation(values = AddRules.class, dialect = DialectName.Constants.MAXCOMPUTE)
@AutoService(StatementBuilder.class)
public class AddRulesBuilder extends BaseRulesBuilder {

    @Override
    public DialectNode build(BaseStatement source, MaxComputeContext context) {
        AddRules createRules = (AddRules)source;
        return new DialectNode(toNode(createRules, context));
    }

    /**
     * 将创建规则转换为可执行的参数
     *
     * @param createRules
     * @param context
     * @return 转换对象
     */
    private String toNode(AddRules createRules, MaxComputeContext context) {
        EntityRules entityRules = new EntityRules();
        entityRules.setEntityLevel(EntityLevel.SQL);
        List<PartitionSpec> partitionSpecList = createRules.getPartitionSpecList();
        String matchExpression = toMatchExpression(partitionSpecList);
        entityRules.setMatchExpression(matchExpression);
        QualifiedName tableName = createRules.getTableName();
        entityRules.setTableName(tableName.getSuffix());
        entityRules.setDialectName(DialectName.MAXCOMPUTE);
        entityRules.setRuleModelList(toRuleModelList(createRules.getRuleDefinitions(), tableName, matchExpression, context));
        return JSON.toJSONString(entityRules, SerializerFeature.PrettyFormat);
    }

}
