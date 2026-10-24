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

import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.atom.TableOrColumn;
import com.aliyun.fastmodel.core.tree.expr.enums.ComparisonOperator;
import com.aliyun.fastmodel.core.tree.expr.literal.LongLiteral;
import com.aliyun.fastmodel.core.tree.statement.dqc.CreateDqcRule;
import com.aliyun.fastmodel.core.tree.statement.dqc.check.BaseCheckElement;
import com.aliyun.fastmodel.core.tree.statement.dqc.check.TableCheckElement;
import com.aliyun.fastmodel.core.tree.statement.element.CreateElement;
import com.aliyun.fastmodel.core.tree.statement.rule.PartitionSpec;
import com.aliyun.fastmodel.core.tree.statement.rule.RuleStrategy;
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
public class CreateDqcRuleBuilderTest {

    CreateDqcRuleBuilder createRulesBuilder = new CreateDqcRuleBuilder();

    @Test
    public void testToNode() {
        List<BaseCheckElement> defines = getRuleDefines();
        CreateDqcRule ab = CreateDqcRule.builder().tableName(QualifiedName.of("dim_shop"))
            .createElement(
                CreateElement.builder().qualifiedName(QualifiedName.of("ab")).build()).ruleDefinitions(defines).build();
        DialectNode build = createRulesBuilder.build(ab,
            MaxComputeContext.builder().build());
        String node = build.getNode();
        System.out.println(node);
        EntityRules entityRules = JSON.parseObject(node, EntityRules.class);
        assertEquals(entityRules.getEntityLevel(), EntityLevel.SQL);
    }

    @Test
    public void testToNodeColumn() {
        List<BaseCheckElement> defines = getRuleDefines_column();
        CreateDqcRule ab = CreateDqcRule.builder().tableName(QualifiedName.of("dim_shop"))
            .createElement(
                CreateElement.builder().qualifiedName(QualifiedName.of("ab")).build()).ruleDefinitions(defines).build();
        DialectNode build = createRulesBuilder.build(ab,
            MaxComputeContext.builder().build());
        String node = build.getNode();
        System.out.println(node);
        EntityRules entityRules = JSON.parseObject(node, EntityRules.class);
        assertEquals(entityRules.getEntityLevel(), EntityLevel.SQL);
    }

    @Test
    public void testToNodeWithExpression() {
        List<BaseCheckElement> defines = getRuleDefines_column();
        List<PartitionSpec> list = ImmutableList.of(
            new PartitionSpec(new Identifier("a"), null),
            new PartitionSpec(new Identifier("b"), null)
        );
        CreateDqcRule ab = CreateDqcRule.builder().tableName(QualifiedName.of("dim_shop")).partitionSpecList(list)
            .createElement(
                CreateElement.builder().qualifiedName(QualifiedName.of("ab")).build()).ruleDefinitions(defines).build();
        DialectNode build = createRulesBuilder.build(ab,
            MaxComputeContext.builder().build());
        String node = build.getNode();
        System.out.println(node);
        EntityRules entityRules = JSON.parseObject(node, EntityRules.class);
        assertEquals(entityRules.getEntityLevel(), EntityLevel.SQL);
    }

    private List<BaseCheckElement> getRuleDefines_column() {
        RuleStrategy strategy = new FixedStrategy(
            new ColumnFunction(
                BaseFunctionName.UNIQUE_COUNT,
                new TableOrColumn(QualifiedName.of("id")),
                DataTypeUtil.simpleType(DataTypeEnums.DATE)
            ),
            ComparisonOperator.EQUAL,
            new LongLiteral("0")
        );
        return ImmutableList.of(TableCheckElement.builder().checkerName(new Identifier("rule1"))
            .expression(strategy.toExpression())
            .build());
    }

    private List<BaseCheckElement> getRuleDefines() {
        RuleStrategy strategy = new VolStrategy(
            new VolFunction(
                new TableFunction(BaseFunctionName.TABLE_SIZE, ImmutableList.of()),
                ImmutableList.of(new LongLiteral("1"))
            ),
            VolOperator.ABS,
            new VolInterval(new Integer(15), new Integer(30))
        );
        return ImmutableList.of(TableCheckElement.builder().checkerName(new Identifier("rule1"))
            .expression(strategy.toExpression())
            .build());
    }

    @Test
    public void testToNodeSelfCustom() {
        List<BaseCheckElement> defines = getRuleDefines_self();
        CreateDqcRule ab = CreateDqcRule.builder().tableName(QualifiedName.of("dim_shop"))
            .createElement(
                CreateElement.builder().qualifiedName(QualifiedName.of("ab")).build()).ruleDefinitions(defines).build();
        DialectNode build = createRulesBuilder.build(ab,
            MaxComputeContext.builder().build());
        String node = build.getNode();
        System.out.println(node);
        EntityRules entityRules = JSON.parseObject(node, EntityRules.class);
        assertEquals(entityRules.getEntityLevel(), EntityLevel.SQL);
    }

    @Test
    public void testUnique() {
        FixedStrategy fixedStrategy = new FixedStrategy(new TableFunction(BaseFunctionName.UNIQUE,
            ImmutableList.of(new Identifier("c1"), new Identifier(
                "c2"
            ))),
            ComparisonOperator.EQUAL,
            new LongLiteral("1"));
        CreateDqcRule source = CreateDqcRule.builder().createElement(
            CreateElement.builder().qualifiedName(QualifiedName.of("abc")).build()
        ).tableName(QualifiedName.of("dim_shop")).ruleDefinitions(
            ImmutableList.of(
                TableCheckElement.builder().checkerName(new Identifier("abc")).expression(
                    fixedStrategy.toExpression()
                ).build()
            )
        ).build();
        DialectNode build = createRulesBuilder.build(source, MaxComputeContext.builder().build());
        EntityRules entityRules = JSON.parseObject(build.getNode(), EntityRules.class);
        List<BaseRuleModel> ruleModelList = entityRules.getRuleModelList();
        BaseRuleModel baseRuleModel = ruleModelList.get(0);
        String ruleValue = baseRuleModel.getRuleValue();
        assertEquals("SYSTEM:fields:duplicated_count:fixed:1", ruleValue);
        assertEquals("c1,c2", baseRuleModel.getProperty());
    }

    private List<BaseCheckElement> getRuleDefines_self() {
        RuleStrategy strategy = new VolStrategy(
            new VolFunction(
                new TableFunction(BaseFunctionName.TABLE_SIZE, ImmutableList.of()),
                ImmutableList.of(new LongLiteral("1"))
            ),
            VolOperator.ABS,
            new VolInterval(new Integer(15), new Integer(30))
        );
        return ImmutableList.of(TableCheckElement.builder().checkerName(new Identifier("rule1"))
            .expression(strategy.toExpression())
            .build());

    }
}