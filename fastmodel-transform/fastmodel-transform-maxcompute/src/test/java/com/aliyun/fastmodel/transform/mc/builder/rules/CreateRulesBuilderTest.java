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

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.atom.TableOrColumn;
import com.aliyun.fastmodel.core.tree.expr.enums.ComparisonOperator;
import com.aliyun.fastmodel.core.tree.expr.literal.LongLiteral;
import com.aliyun.fastmodel.core.tree.statement.element.CreateElement;
import com.aliyun.fastmodel.core.tree.statement.rule.CreateRules;
import com.aliyun.fastmodel.core.tree.statement.rule.PartitionSpec;
import com.aliyun.fastmodel.core.tree.statement.rule.RuleDefinition;
import com.aliyun.fastmodel.core.tree.statement.rule.RuleStrategy;
import com.aliyun.fastmodel.core.tree.statement.rule.RulesLevel;
import com.aliyun.fastmodel.core.tree.statement.rule.function.BaseFunctionName;
import com.aliyun.fastmodel.core.tree.statement.rule.function.VolFunction;
import com.aliyun.fastmodel.core.tree.statement.rule.function.column.ColumnFunction;
import com.aliyun.fastmodel.core.tree.statement.rule.function.table.TableFunction;
import com.aliyun.fastmodel.core.tree.statement.rule.strategy.FixedStrategy;
import com.aliyun.fastmodel.core.tree.statement.rule.strategy.VolInterval;
import com.aliyun.fastmodel.core.tree.statement.rule.strategy.VolOperator;
import com.aliyun.fastmodel.core.tree.statement.rule.strategy.VolStrategy;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.api.domain.dqc.BaseRuleModel;
import com.aliyun.fastmodel.transform.api.domain.dqc.EntityRules;
import com.aliyun.fastmodel.transform.api.domain.dqc.constant.EntityLevel;
import com.aliyun.fastmodel.transform.mc.context.MaxComputeContext;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * 创建规则的builder处理
 *
 * @author panguanjing
 * @date 2021/5/31
 */
public class CreateRulesBuilderTest {

    CreateRulesBuilder createRulesBuilder = new CreateRulesBuilder();

    @Test
    public void testToNode() {
        List<RuleDefinition> defines = getRuleDefines();
        CreateRules ab = CreateRules.builder().tableName(QualifiedName.of("dim_shop")).ruleLevel(RulesLevel.TASK)
            .createElement(
                CreateElement.builder().qualifiedName(QualifiedName.of("ab")).build()).ruleDefinitions(defines).build();
        DialectNode build = createRulesBuilder.build(ab,
            MaxComputeContext.builder().build());
        String node = build.getNode();
        System.out.println(node);
        EntityRules entityRules = JSON.parseObject(node, EntityRules.class);
        assertEquals(entityRules.getEntityLevel(), EntityLevel.TASK);
    }

    @Test
    public void testToNodeColumn() {
        List<RuleDefinition> defines = getRuleDefinesColumn();
        CreateRules ab = CreateRules.builder().tableName(QualifiedName.of("dim_shop")).ruleLevel(RulesLevel.TASK)
            .createElement(
                CreateElement.builder().qualifiedName(QualifiedName.of("ab")).build()).ruleDefinitions(defines).build();
        DialectNode build = createRulesBuilder.build(ab,
            MaxComputeContext.builder().build());
        String node = build.getNode();
        System.out.println(node);
        EntityRules entityRules = JSON.parseObject(node, EntityRules.class);
        assertEquals(entityRules.getEntityLevel(), EntityLevel.TASK);
    }

    @Test
    public void testToNodeWithExpression() {
        List<RuleDefinition> defines = getRuleDefinesColumn();
        List<PartitionSpec> list = ImmutableList.of(
            new PartitionSpec(new Identifier("a"), null),
            new PartitionSpec(new Identifier("b"), null)
        );
        CreateRules ab = CreateRules.builder().tableName(QualifiedName.of("dim_shop")).partitionSpecList(list)
            .ruleLevel(RulesLevel.TASK)
            .createElement(
                CreateElement.builder().qualifiedName(QualifiedName.of("ab")).build()).ruleDefinitions(defines).build();
        DialectNode build = createRulesBuilder.build(ab,
            MaxComputeContext.builder().build());
        String node = build.getNode();
        System.out.println(node);
        EntityRules entityRules = JSON.parseObject(node, EntityRules.class);
        assertEquals(entityRules.getEntityLevel(), EntityLevel.TASK);
    }

    private List<RuleDefinition> getRuleDefinesColumn() {
        RuleStrategy strategy = new FixedStrategy(
            new ColumnFunction(
                BaseFunctionName.UNIQUE_COUNT,
                new TableOrColumn(QualifiedName.of("id")),
                DataTypeUtil.simpleType(DataTypeEnums.DATE)
            ),
            ComparisonOperator.EQUAL,
            new LongLiteral("0")
        );
        return ImmutableList.of(RuleDefinition.builder().ruleName(new Identifier("rule1")).ruleStrategy(strategy)
            .build());
    }

    private List<RuleDefinition> getRuleDefines() {
        RuleStrategy strategy = new VolStrategy(
            new VolFunction(
                new TableFunction(BaseFunctionName.TABLE_SIZE, ImmutableList.of()),
                ImmutableList.of(new LongLiteral("1"))
            ),
            VolOperator.ABS,
            new VolInterval(new Integer(15), new Integer(30))
        );
        return ImmutableList.of(RuleDefinition.builder().ruleName(new Identifier("rule1")).ruleStrategy(strategy)
            .build());
    }

    @Test
    public void testToNodeSelfCustom() {
        List<RuleDefinition> defines = getRuleDefinesSelf();
        CreateRules ab = CreateRules.builder().tableName(QualifiedName.of("dim_shop")).ruleLevel(RulesLevel.TASK)
            .createElement(
                CreateElement.builder().qualifiedName(QualifiedName.of("ab")).build()).ruleDefinitions(defines).build();
        DialectNode build = createRulesBuilder.build(ab,
            MaxComputeContext.builder().build());
        String node = build.getNode();
        System.out.println(node);
        EntityRules entityRules = JSON.parseObject(node, EntityRules.class);
        assertEquals(entityRules.getEntityLevel(), EntityLevel.TASK);
    }

    @Test
    public void testUnique() {
        BaseStatement source = CreateRules.builder().ruleLevel(RulesLevel.SQL).createElement(
            CreateElement.builder().qualifiedName(QualifiedName.of("abc")).build()
        ).tableName(QualifiedName.of("dim_shop")).ruleDefinitions(
            ImmutableList.of(
                RuleDefinition.builder().ruleName(new Identifier("abc")).ruleStrategy(
                    new FixedStrategy(new TableFunction(BaseFunctionName.UNIQUE,
                        ImmutableList.of(new Identifier("c1"), new Identifier(
                            "c2"
                        ))),
                        ComparisonOperator.EQUAL,
                        new LongLiteral("1"))
                ).build()
            )
        ).build();
        DialectNode build = createRulesBuilder.build(source, MaxComputeContext.builder().build());
        EntityRules entityRules = JSON.parseObject(build.getNode(), EntityRules.class);
        List<BaseRuleModel> ruleModelList = entityRules.getRuleModelList();
        BaseRuleModel baseRuleModel = ruleModelList.get(0);
        String ruleValue = baseRuleModel.getRuleValue();
        assertEquals("SYSTEM:fields:duplicated_count:fixed", ruleValue);
        assertEquals("c1,c2", baseRuleModel.getProperty());
    }

    private List<RuleDefinition> getRuleDefinesSelf() {
        RuleStrategy strategy = new VolStrategy(
            new VolFunction(
                new TableFunction(BaseFunctionName.TABLE_SIZE, ImmutableList.of()),
                ImmutableList.of(new LongLiteral("1"))
            ),
            VolOperator.ABS,
            new VolInterval(new Integer(15), new Integer(30))
        );
        return ImmutableList.of(RuleDefinition.builder().ruleName(new Identifier("rule1")).ruleStrategy(strategy)
            .build());

    }
}