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

package com.aliyun.fastmodel.dqc.check;

import java.util.List;

import com.aliyun.fastmodel.conveter.dqc.DefaultConvertContext;
import com.aliyun.fastmodel.conveter.dqc.check.ChangeColConverter;
import com.aliyun.fastmodel.core.formatter.FastModelFormatter;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.dqc.ChangeDqcRule;
import com.aliyun.fastmodel.core.tree.statement.dqc.ChangeDqcRuleElement;
import com.aliyun.fastmodel.core.tree.statement.dqc.check.BaseCheckElement;
import com.aliyun.fastmodel.core.tree.statement.table.ChangeCol;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * ChangeColConverterTest
 *
 * @author panguanjing
 * @date 2021/6/7
 */
public class ChangeColConverterTest {

    ChangeColConverter changeColConverter = new ChangeColConverter();
    DefaultConvertContext context = new DefaultConvertContext();

    @Before
    public void setUp() throws Exception {
        CreateTable dimShop = CreateTable.builder().tableName(QualifiedName.of("dim_shop")).build();
        context.setBeforeStatement(dimShop);
        context.setAfterStatement(dimShop);
    }

    @Test
    public void testConverter() {
        ChangeCol source = new ChangeCol(
            QualifiedName.of("dim_shop"),
            new Identifier("c1"),
            ColumnDefinition.builder().colName(new Identifier("c2")).primary(
                true
            ).notNull(true).dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT)).build()
        );
        BaseStatement convert = changeColConverter.convert(source, context);
        ChangeDqcRule changeRules = (ChangeDqcRule)convert;
        List<ChangeDqcRuleElement> ruleDefinitions = changeRules.getChangeDqcRuleElement();
        assertEquals(2, ruleDefinitions.size());
    }

    @Test
    public void testConverterChangename() {
        ChangeCol source = new ChangeCol(
            QualifiedName.of("dim_shop"),
            new Identifier("c1"),
            ColumnDefinition.builder().colName(new Identifier("c2"))
                .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT)).build()
        );
        BaseStatement convert = changeColConverter.convert(source, context);
        ChangeDqcRule changeRules = (ChangeDqcRule)convert;
        String s = FastModelFormatter.formatNode(changeRules);
        assertEquals(s, "ALTER DQC_RULE ON TABLE dim_shop\n"
            + "CHANGE (\n"
            + "   c1   CONSTRAINT c2 NOT ENFORCED\n"
            + ")");
    }

    @Test
    public void testConverterNotNull() {
        ChangeCol source = new ChangeCol(
            QualifiedName.of("dim_shop"),
            new Identifier("c1"),
            ColumnDefinition.builder().colName(new Identifier("c2")).notNull(true
            ).dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT)).build()
        );
        ChangeDqcRule changeRules =
            (ChangeDqcRule)changeColConverter.convert(source, context);
        List<ChangeDqcRuleElement> ruleDefinitions = changeRules.getChangeDqcRuleElement();
        assertEquals(1, ruleDefinitions.size());
        ChangeDqcRuleElement changeRuleElement = ruleDefinitions.get(0);
        BaseCheckElement ruleDefinition = changeRuleElement.getBaseCheckElement();
        assertEquals(ruleDefinition.getCheckName().getValue(), "字段规则-非空-(c2)");
    }

    @Test
    public void testConverterNull() {
        ChangeCol changeCol = new ChangeCol(
            QualifiedName.of("dim_shop"),
            new Identifier("c1"),
            ColumnDefinition.builder().colName(new Identifier("c1")).notNull(false)
                .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT)).build()
        );
        ChangeDqcRule rules = (ChangeDqcRule)changeColConverter.convert(changeCol, context);
        List<ChangeDqcRuleElement> ruleDefinitions = rules.getChangeDqcRuleElement();
        assertEquals(1, ruleDefinitions.size());
        ChangeDqcRuleElement changeRuleElement = ruleDefinitions.get(0);
        BaseCheckElement ruleDefinition = changeRuleElement.getBaseCheckElement();
        assertEquals(ruleDefinition.isEnable(), false);
    }

    @Test
    public void testConverterPrimaryKey() {
        ChangeCol changeCol = new ChangeCol(
            QualifiedName.of("dim_shop"),
            new Identifier("c1"),
            ColumnDefinition.builder().colName(new Identifier("c1")).primary(false).dataType(
                DataTypeUtil.simpleType(DataTypeEnums.BIGINT)).build()
        );
        ChangeDqcRule changeRules = (ChangeDqcRule)changeColConverter.convert(changeCol, context);
        List<ChangeDqcRuleElement> ruleElements = changeRules.getChangeDqcRuleElement();
        assertEquals(ruleElements.size(), 1);
        ChangeDqcRuleElement ruleElement = ruleElements.get(0);
        assertEquals(ruleElement.getBaseCheckElement().isEnable(), false);
    }

    @Test
    public void testConverterCodeTable() {
        ChangeCol changeCol = new ChangeCol(
            QualifiedName.of("dim_shop"),
            new Identifier("c1"),
            ColumnDefinition.builder().colName(new Identifier("c1")).properties(
                ImmutableList.of(new Property("code_table", ""))
            ).build()
        );
        ChangeDqcRule changeRules = (ChangeDqcRule)changeColConverter.convert(changeCol, context);
        List<ChangeDqcRuleElement> changeDqcRuleElement = changeRules.getChangeDqcRuleElement();
        assertEquals(changeRules.toString(), "ALTER DQC_RULE ON TABLE dim_shop\n"
            + "CHANGE (\n"
            + "   c1   CONSTRAINT `字段规则-标准代码-(c1)` CHECK(IN_TABLE(c1, ``, code) = 0) NOT ENFORCED DISABLE\n"
            + ")");
    }

    @Test
    public void testConverterCodeTableWithOld() {
        QualifiedName dimShop = QualifiedName.of("dim_shop");
        Identifier c1 = new Identifier("c1");
        ImmutableList<Property> properties1 = ImmutableList.of(new Property("code_table", "abc"));
        ChangeCol changeCol = new ChangeCol(
            dimShop,
            c1,
            ColumnDefinition.builder().colName(c1).properties(
                properties1
            ).build()
        );
        ImmutableList<Property> properties = ImmutableList.of(new Property("code_table", "dce"));
        List<ColumnDefinition> columnDefinitions = ImmutableList.of(
            ColumnDefinition.builder()
                .colName(c1)
                .properties(properties)
                .build()
        );
        context.setBeforeStatement(CreateTable.builder().tableName(dimShop).columns(columnDefinitions).build());
        ChangeDqcRule changeRules = (ChangeDqcRule)changeColConverter.convert(changeCol, context);
        List<ChangeDqcRuleElement> changeDqcRuleElement = changeRules.getChangeDqcRuleElement();
        assertEquals(changeRules.toString(), "ALTER DQC_RULE ON TABLE dim_shop\n"
            + "CHANGE (\n"
            + "   c1   CONSTRAINT `字段规则-标准代码-(c1)` CHECK(IN_TABLE(c1, dce, code) = 0) NOT ENFORCED DISABLE,\n"
            + "   c1   CONSTRAINT `字段规则-标准代码-(c1)` CHECK(IN_TABLE(c1, abc, code) = 0) NOT ENFORCED\n"
            + ")");
    }

    @Test
    public void testConverterCodeTableExist() {
        ChangeCol changeCol = new ChangeCol(
            QualifiedName.of("dim_shop"),
            new Identifier("c1"),
            ColumnDefinition.builder().colName(new Identifier("c1")).properties(
                ImmutableList.of(new Property("code_table", "t1"))
            ).build()
        );
        ChangeDqcRule changeRules = (ChangeDqcRule)changeColConverter.convert(changeCol, context);
        List<ChangeDqcRuleElement> changeDqcRuleElement = changeRules.getChangeDqcRuleElement();
        assertEquals(changeRules.toString(), "ALTER DQC_RULE ON TABLE dim_shop\n"
            + "CHANGE (\n"
            + "   c1   CONSTRAINT `字段规则-标准代码-(c1)` CHECK(IN_TABLE(c1, t1, code) = 0) NOT ENFORCED\n"
            + ")");
    }
}