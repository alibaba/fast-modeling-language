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

import com.aliyun.fastmodel.core.formatter.FastModelFormatter;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.atom.TableOrColumn;
import com.aliyun.fastmodel.core.tree.expr.literal.BaseLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.LongLiteral;
import com.aliyun.fastmodel.core.tree.statement.rule.CreateRules;
import com.aliyun.fastmodel.core.tree.statement.rule.RuleDefinition;
import com.aliyun.fastmodel.core.tree.statement.rule.RuleStrategy;
import com.aliyun.fastmodel.core.tree.statement.rule.RulesLevel;
import com.aliyun.fastmodel.core.tree.statement.rule.function.BaseFunction;
import com.aliyun.fastmodel.core.tree.statement.rule.function.BaseFunctionName;
import com.aliyun.fastmodel.core.tree.statement.rule.function.VolFunction;
import com.aliyun.fastmodel.core.tree.statement.rule.function.column.ColumnFunction;
import com.aliyun.fastmodel.core.tree.statement.rule.function.column.InTableFunction;
import com.aliyun.fastmodel.core.tree.statement.rule.strategy.DynamicStrategy;
import com.aliyun.fastmodel.core.tree.statement.rule.strategy.FixedStrategy;
import com.aliyun.fastmodel.core.tree.statement.rule.strategy.VolInterval;
import com.aliyun.fastmodel.core.tree.statement.rule.strategy.VolOperator;
import com.aliyun.fastmodel.core.tree.statement.rule.strategy.VolStrategy;
import com.aliyun.fastmodel.parser.BaseTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * 规则的单元测试
 *
 * @author panguanjing
 * @date 2021/5/30
 */
public class RulesTest extends BaseTest {
    @Test
    public void testCreateRules() {
        String fml = "Create SQL  rules IF NOT EXISTS abc alias '分区表达式' REFERENCES dim_shop ("
            + "STRONG 生成规则内容 min(id) > 10 comment 'comment') COMMENT 'abc'";
        CreateRules createRules = parse(fml, CreateRules.class);
        assertEquals(createRules.getRulesLevel(), RulesLevel.SQL);
    }

    @Test
    public void testCreateRulesSys() {
        String fml = "Create Task  rules IF NOT EXISTS abc alias '分区表达式' REFERENCES dim_shop ("
            + "STRONG SYS_RULES_ceshi1syp3 min(id) > 10 comment 'comment') COMMENT 'abc'";
        CreateRules createRules = parse(fml, CreateRules.class);
        String s = FastModelFormatter.formatNode(createRules);
        assertEquals("CREATE TASK RULES abc REFERENCES dim_shop\n"
            + "(\n"
            + "   STRONG sys_rules_ceshi1syp3 MIN(id) > 10  COMMENT 'comment'\n"
            + ") COMMENT 'abc'", s);
    }

    @Test
    public void testCreateRulesMin() {
        String fml = "Create SQL  rules IF NOT EXISTS abc alias '分区表达式' REFERENCES dim_shop ("
            + "STRONG 生成规则内容 min(id) as bigint > 10 comment 'comment') COMMENT 'abc'";
        CreateRules createRules = parse(fml, CreateRules.class);
        assertEquals(createRules.getRulesLevel(), RulesLevel.SQL);
        List<RuleDefinition> ruleDefinitions = createRules.getRuleDefinitions();
        RuleDefinition ruleDefinition = ruleDefinitions.get(0);
        RuleStrategy ruleStrategy = ruleDefinition.getRuleStrategy();
        BaseFunction function = ruleStrategy.getFunction();
        ColumnFunction columnFunction = (ColumnFunction)function;
        BaseDataType baseDataType = columnFunction.getBaseDataType();
        assertEquals(baseDataType.getTypeName(), DataTypeEnums.BIGINT);
    }

    @Test
    public void testVol() {

        String fml = "Create TASK rules abc REFERENCES dim_shop PARTITION(ds='yyyymmdd') ("
            + " WEAK w1 vol(min(id),1,7,30) INCREASE [15,30] COMMENT ''"
            + ") WITH ('abc'='bc')";
        CreateRules createRules = parse(fml, CreateRules.class);
        List<RuleDefinition> ruleDefinitions = createRules.getRuleDefinitions();
        RuleDefinition ruleDefinition = ruleDefinitions.get(0);
        RuleStrategy ruleStrategy = ruleDefinition.getRuleStrategy();
        assertEquals(ruleStrategy.getClass(), VolStrategy.class);
        VolStrategy volStrategy = (VolStrategy)ruleStrategy;
        VolInterval volInterval = volStrategy.getVolInterval();
        assertEquals(volInterval.getStart().intValue(), 15);
        assertEquals(volInterval.getEnd().intValue(), 30);
        assertEquals(volStrategy.getVolOperator(), VolOperator.INCREASE);
        VolFunction volFunction = volStrategy.getVolFunction();
        BaseFunction baseFunction = volFunction.getBaseFunction();
        assertEquals(baseFunction.funcName(), BaseFunctionName.MIN);
    }

    @Test
    public void testDynamic() {
        String fml = "CREATE TASK RULES abc REFERENCES dim_shop (WEAK w1 dynamic(min(id),15))";
        CreateRules createRules = parse(fml, CreateRules.class);
        RuleDefinition ruleDefinitions = createRules.getRuleDefinitions().get(0);
        RuleStrategy ruleStrategy = ruleDefinitions.getRuleStrategy();
        assertEquals(ruleStrategy.getClass(), DynamicStrategy.class);
        DynamicStrategy dynamicStrategy = (DynamicStrategy)ruleStrategy;
        BaseFunction baseFunction = dynamicStrategy.getBaseFunction();
        assertEquals(baseFunction.funcName(), BaseFunctionName.MIN);
        List<BaseExpression> arguments = baseFunction.arguments();
        BaseExpression baseExpression = arguments.get(0);
        assertEquals(baseExpression, new TableOrColumn(QualifiedName.of("id")));
        BaseLiteral number = dynamicStrategy.getNumber();
        LongLiteral longLiteral = (LongLiteral)number;
        assertEquals(longLiteral.getValue(), new Long(15));
    }

    @Test
    public void testInTable() {
        String fml = "CREATE TASK RULES abc REFERENCES dim_shop ("
            + "WEAK w1 in_table(id, code_table) = 1)"
            + "WITH ('abc'='bc')";
        CreateRules createRules = parse(fml, CreateRules.class);
        List<RuleDefinition> ruleDefinitions = createRules.getRuleDefinitions();
        RuleDefinition ruleDefinition = ruleDefinitions.get(0);
        RuleStrategy ruleStrategy = ruleDefinition.getRuleStrategy();
        FixedStrategy fixedStrategy = (FixedStrategy)ruleStrategy;
        BaseFunction baseFunction = fixedStrategy.getBaseFunction();
        InTableFunction inTableFunction = (InTableFunction)baseFunction;
        QualifiedName tableName = inTableFunction.getTableName();
        assertEquals(tableName, QualifiedName.of("code_table"));
        List<BaseExpression> arguments = inTableFunction.arguments();
        BaseExpression baseExpression = arguments.get(2);
        assertEquals(baseExpression, new TableOrColumn(QualifiedName.of("code")));
    }

    @Test
    public void testInTableWithColumn() {
        String fml = "CREATE TASK RULES abc REFERENCES dim_shop ("
            + "WEAK w1 in_table(id, code_table, column2) = 1)"
            + "WITH ('abc'='bc')";
        CreateRules createRules = parse(fml, CreateRules.class);
        List<RuleDefinition> ruleDefinitions = createRules.getRuleDefinitions();
        RuleDefinition ruleDefinition = ruleDefinitions.get(0);
        RuleStrategy ruleStrategy = ruleDefinition.getRuleStrategy();
        FixedStrategy fixedStrategy = (FixedStrategy)ruleStrategy;
        BaseFunction baseFunction = fixedStrategy.getBaseFunction();
        InTableFunction inTableFunction = (InTableFunction)baseFunction;
        QualifiedName tableName = inTableFunction.getTableName();
        assertEquals(tableName, QualifiedName.of("code_table"));
        List<BaseExpression> arguments = inTableFunction.arguments();
        BaseExpression baseExpression = arguments.get(2);
        assertEquals(baseExpression, new TableOrColumn(QualifiedName.of("column2")));
    }
}
