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

package com.aliyun.fastmodel.core.tree.util;

import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.rule.function.BaseFunctionName;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * 规则工具类，用于标识符生成的能力
 *
 * @author panguanjing
 * @date 2021/5/31
 */
public class RuleUtilTest {
    @Test
    public void testGenerateRuleIdentifier() {
        QualifiedName dimShop = RuleUtil.generateRulesName(QualifiedName.of("dim_shop"));
        assertEquals(dimShop.getSuffix(), "sys_rules_dim_shop");
    }

    @Test
    public void testGenerateRuleName() {
        QualifiedName tableName = RuleUtil.generateRulesName(QualifiedName.of("tableName"));
        assertEquals(tableName.getSuffix(), "sys_rules_tablename");
    }

    @Test
    public void testReplace() {
        String s = RuleUtil.replaceRuleName("NOT_NULL_col1", "col1", "newCol2");
        assertEquals(s, "NOT_NULL_newCol2");
        String s1 = RuleUtil.replaceRuleName("NOT_NULL_col1", "col1", null);
        assertEquals(s1, "NOT_NULL_col1");
    }

    @Test
    public void testGenerateRule() {
        Identifier identifier = RuleUtil.generateRuleNameByFunction(BaseFunctionName.UNIQUE_COUNT,
            new Identifier("col1"));
        assertEquals(identifier.getValue(), "字段规则-唯一值数-(col1)");
    }

    @Test
    public void testGenerateRuleNullCount() {
        Identifier identifier = RuleUtil.generateRuleNameByFunction(BaseFunctionName.NULL_COUNT,
            new Identifier("col1"));
        assertEquals(identifier.getValue(), "字段规则-非空-(col1)");
    }

    @Test
    public void testGenerateRuleUnique() {
        Identifier identifier = RuleUtil.generateRuleNameByFunction(BaseFunctionName.UNIQUE,
            new Identifier("col1"));
        assertEquals(identifier.getValue(), "表级规则-唯一-(col1)");
    }

    @Test
    public void testGenerateRuleDuplicateCount() {
        Identifier identifier = RuleUtil.generateRuleNameByFunction(BaseFunctionName.DUPLICATE_COUNT,
            new Identifier("col1"));
        assertEquals(identifier.getValue(), "字段规则-唯一-(col1)");
    }
}