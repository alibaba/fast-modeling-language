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

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.atom.TableOrColumn;
import com.aliyun.fastmodel.core.tree.expr.enums.ComparisonOperator;
import com.aliyun.fastmodel.core.tree.expr.literal.LongLiteral;
import com.aliyun.fastmodel.core.tree.statement.dqc.ChangeDqcRule;
import com.aliyun.fastmodel.core.tree.statement.dqc.ChangeDqcRuleElement;
import com.aliyun.fastmodel.core.tree.statement.dqc.check.BaseCheckElement;
import com.aliyun.fastmodel.core.tree.statement.dqc.check.TableCheckElement;
import com.aliyun.fastmodel.core.tree.statement.rule.RuleStrategy;
import com.aliyun.fastmodel.core.tree.statement.rule.function.BaseFunctionName;
import com.aliyun.fastmodel.core.tree.statement.rule.function.column.ColumnFunction;
import com.aliyun.fastmodel.core.tree.statement.rule.strategy.FixedStrategy;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.mc.context.MaxComputeContext;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

/**
 * changeRules unit test
 *
 * @author panguanjing
 * @date 2021/6/3
 */
public class ChangeDqcRuleBuilderTest {

    @Test
    public void testBuild() {
        ChangeDqcRuleBuilder changeRulesBuilder = new ChangeDqcRuleBuilder();
        RuleStrategy strategy = new FixedStrategy(
            new ColumnFunction(BaseFunctionName.UNIQUE_COUNT, new TableOrColumn(QualifiedName.of("id")),
                DataTypeUtil.simpleType(
                    DataTypeEnums.BIGINT)),
            ComparisonOperator.EQUAL, new LongLiteral("0")
        );
        BaseCheckElement baseCheckElement = TableCheckElement.builder().checkerName(new Identifier("ruleName"))
            .expression(strategy.toExpression()).build();
        List<ChangeDqcRuleElement> ruleDefine = ImmutableList.of(
            new ChangeDqcRuleElement(new Identifier("old_column"), baseCheckElement)
        );
        BaseStatement statment = new ChangeDqcRule(QualifiedName.of("dim_show"), null, ruleDefine);
        DialectNode build = changeRulesBuilder.build(statment, MaxComputeContext.builder().build());
        assertNotNull(build.getNode());
    }
}