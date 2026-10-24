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

import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.enums.ComparisonOperator;
import com.aliyun.fastmodel.core.tree.expr.literal.LongLiteral;
import com.aliyun.fastmodel.core.tree.statement.rule.AddRules;
import com.aliyun.fastmodel.core.tree.statement.rule.RuleDefinition;
import com.aliyun.fastmodel.core.tree.statement.rule.function.column.InTableFunction;
import com.aliyun.fastmodel.core.tree.statement.rule.strategy.FixedStrategy;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.mc.context.MaxComputeContext;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/6/28
 */
public class AddRulesBuilderTest {

    @Test
    public void build() {
        AddRulesBuilder addRulesBuilder = new AddRulesBuilder();
        RuleDefinition build1 = RuleDefinition.builder().ruleName(new Identifier("name")).build();
        AddRules addRules = new AddRules(
            QualifiedName.of("dim_shop"),
            ImmutableList.of(),
            ImmutableList.of(build1)
        );
        DialectNode build = addRulesBuilder.build(addRules, MaxComputeContext.builder().build());
        assertNotNull(build);
    }

    @Test
    public void addRuleWithCode() {
        AddRulesBuilder addRulesBuilder = new AddRulesBuilder();
        RuleDefinition build1 = RuleDefinition.builder()
            .ruleName(new Identifier("name"))
            .ruleStrategy(new FixedStrategy(
                new InTableFunction(QualifiedName.of("c1"), QualifiedName.of("t1"),
                    new Identifier("c2")),
                ComparisonOperator.EQUAL,
                new LongLiteral("0")
            ))
            .build();
        AddRules addRules = new AddRules(
            QualifiedName.of("dim_shop"),
            ImmutableList.of(),
            ImmutableList.of(build1)
        );
        DialectNode build = addRulesBuilder.build(addRules, MaxComputeContext.builder().build());
        assertEquals(build.getNode(), "{\n"
            + "\t\"dialectName\":\"MAXCOMPUTE\",\n"
            + "\t\"entityLevel\":\"SQL\",\n"
            + "\t\"matchExpression\":\"NOTAPARTITIONTABLE\",\n"
            + "\t\"ruleModelList\":[\n"
            + "\t\t{\n"
            + "\t\t\t\"action\":\"ADD\",\n"
            + "\t\t\t\"blockType\":\"BLOCK_WEAK\",\n"
            + "\t\t\t\"checkerType\":\"FIX_STRATEGY_CHECK\",\n"
            + "\t\t\t\"expectValue\":\"0\",\n"
            + "\t\t\t\"operator\":\"=\",\n"
            + "\t\t\t\"predictType\":\"NOT_DYNAMIC\",\n"
            + "\t\t\t\"property\":\"USER_DEFINED\",\n"
            + "\t\t\t\"propertyType\":\"user_defined\",\n"
            + "\t\t\t\"ruleName\":\"name\",\n"
            + "\t\t\t\"ruleType\":\"SLEF_CUSTOM\",\n"
            + "\t\t\t\"ruleValue\":\"SELECT count(*)\\nFROM\\n  dim_shop\\nWHERE NOT (c1 IN (SELECT c2\\nFROM\\n  "
            + "t1\\n))\\n\"\n"
            + "\t\t}\n"
            + "\t],\n"
            + "\t\"tableName\":\"dim_shop\"\n"
            + "}");

    }
}