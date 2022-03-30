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

package com.aliyun.fastmodel.parser.impl;

import java.util.List;

import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.atom.TableOrColumn;
import com.aliyun.fastmodel.core.tree.expr.enums.ComparisonOperator;
import com.aliyun.fastmodel.core.tree.expr.literal.LongLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.core.tree.statement.rule.AddRules;
import com.aliyun.fastmodel.core.tree.statement.rule.PartitionSpec;
import com.aliyun.fastmodel.core.tree.statement.rule.RuleDefinition;
import com.aliyun.fastmodel.core.tree.statement.rule.function.BaseFunctionName;
import com.aliyun.fastmodel.core.tree.statement.rule.function.column.ColumnFunction;
import com.aliyun.fastmodel.core.tree.statement.rule.strategy.FixedStrategy;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.aliyun.fastmodel.parser.BaseTest;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * 增加rules的优化
 *
 * @author panguanjing
 * @date 2021/6/3
 */
public class AddRulesTest extends BaseTest {

    @Test
    public void testAddRules() {
        List<RuleDefinition> ruleList = ImmutableList.of(
            RuleDefinition.builder().ruleName(new Identifier("r1")).ruleStrategy(
                new FixedStrategy(
                    new ColumnFunction(BaseFunctionName.NULL_COUNT, new TableOrColumn(QualifiedName.of("c1")),
                        DataTypeUtil.simpleType(DataTypeEnums.BIGINT)), ComparisonOperator.EQUAL, new LongLiteral("0"))
            ).build()
        );

        AddRules addRules = new AddRules(QualifiedName.of("dim_shop"),
            ImmutableList.of(
                new PartitionSpec(new Identifier("c1"), new StringLiteral("yyyyMMdd")),
                new PartitionSpec(new Identifier("c2"), new StringLiteral("yyyyMMdd"))
            ), ruleList);

        String s = addRules.toString();
        AddRules parse = nodeParser.parseStatement(s);
        assertEquals(parse.getTableName(), addRules.getTableName());
    }
}
