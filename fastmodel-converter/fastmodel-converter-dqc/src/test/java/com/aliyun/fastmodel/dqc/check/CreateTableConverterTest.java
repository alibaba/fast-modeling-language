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

import com.aliyun.fastmodel.conveter.dqc.check.CreateTableConverter;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.ComparisonExpression;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.constants.ColumnPropertyDefaultKey;
import com.aliyun.fastmodel.core.tree.statement.dqc.CreateDqcRule;
import com.aliyun.fastmodel.core.tree.statement.dqc.check.BaseCheckElement;
import com.aliyun.fastmodel.core.tree.statement.rule.function.BaseFunction;
import com.aliyun.fastmodel.core.tree.statement.rule.function.column.ColumnFunction;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateDimTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.PrimaryConstraint;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * 转换处理内容
 *
 * @author panguanjing
 * @date 2021/5/31
 */
public class CreateTableConverterTest {

    CreateTableConverter createTableConverter = new CreateTableConverter();

    @Test
    public void testConvert() {
        List<ColumnDefinition> columns = ImmutableList.of(
            ColumnDefinition.builder().colName(new Identifier("abc")).dataType(
                DataTypeUtil.simpleType(DataTypeEnums.BIGINT)
            ).build()
        );
        CreateTable c = CreateDimTable.builder().tableName(QualifiedName.of("dim_shop")).columns(
            columns
        ).build();
        BaseStatement convert = createTableConverter.convert(c, null);
        assertNull(convert);
    }

    @Test
    public void testConvertPrimaryKey() {
        List<ColumnDefinition> columns = ImmutableList.of(
            ColumnDefinition.builder().colName(new Identifier("abc")).dataType(
                DataTypeUtil.simpleType(DataTypeEnums.BIGINT)
            ).primary(true).build()
        );
        CreateTable c = CreateDimTable.builder().tableName(QualifiedName.of("dim_shop")).columns(
            columns
        ).build();
        BaseStatement convert = createTableConverter.convert(c, null);
        assertNotNull(convert);
        CreateDqcRule createRules = (CreateDqcRule)convert;
        List<BaseCheckElement> ruleDefinitions = createRules.getBaseCheckElements();
        assertEquals(1, ruleDefinitions.size());
    }

    @Test
    public void testConvertNotNull() {
        List<ColumnDefinition> columns = ImmutableList.of(
            ColumnDefinition.builder().colName(new Identifier("abc")).dataType(
                DataTypeUtil.simpleType(DataTypeEnums.BIGINT)
            ).notNull(true).build()
        );
        CreateTable c = CreateDimTable.builder().tableName(QualifiedName.of("dim_shop")).columns(
            columns
        ).build();
        BaseStatement convert = createTableConverter.convert(c, null);
        assertNotNull(convert);
        CreateDqcRule createRules = (CreateDqcRule)convert;
        List<BaseCheckElement> ruleDefinitions = createRules.getBaseCheckElements();
        assertEquals(1, ruleDefinitions.size());
    }

    @Test
    public void testConvertConstraint() {
        List<ColumnDefinition> columns = ImmutableList.of(
            ColumnDefinition.builder().colName(new Identifier("abc")).dataType(
                DataTypeUtil.simpleType(DataTypeEnums.BIGINT)
            ).notNull(true).build(),

            ColumnDefinition.builder().colName(new Identifier("abd")).dataType(
                DataTypeUtil.simpleType(DataTypeEnums.BIGINT)
            ).build()
        );
        List<BaseConstraint> constraints = ImmutableList.of(
            new PrimaryConstraint(new Identifier("aaa"), ImmutableList.of(new Identifier("abc"), new Identifier("abd")))
        );
        CreateTable c = CreateDimTable.builder().tableName(QualifiedName.of("dim_shop")).columns(
            columns
        ).constraints(constraints).build();
        BaseStatement convert = createTableConverter.convert(c, null);
        assertNotNull(convert);
        CreateDqcRule createRules = (CreateDqcRule)convert;
        List<BaseCheckElement> ruleDefinitions = createRules.getBaseCheckElements();
        assertEquals(3, ruleDefinitions.size());
        BaseCheckElement ruleDefinition = ruleDefinitions.get(0);
        BaseExpression boolExpression = ruleDefinition.getBoolExpression();
        ComparisonExpression comparisonExpression = (ComparisonExpression)boolExpression;
        BaseFunction baseFunction = (BaseFunction)comparisonExpression.getLeft();
        assertEquals(baseFunction.getClass(), ColumnFunction.class);
    }

    @Test
    public void testConvertCodeTable() {
        List<Property> properties = ImmutableList.of(
            new Property(ColumnPropertyDefaultKey.code_table.name(), "code_table_1")
        );
        List<ColumnDefinition> columns = ImmutableList.of(
            ColumnDefinition.builder().colName(new Identifier("abc")).dataType(
                DataTypeUtil.simpleType(DataTypeEnums.BIGINT)
            ).properties(properties).build()
        );
        CreateTable c = CreateDimTable.builder().tableName(QualifiedName.of("dim_shop")).columns(
            columns
        ).build();
        BaseStatement convert = createTableConverter.convert(c, null);
        assertNotNull(convert);
        CreateDqcRule createRules = (CreateDqcRule)convert;
        List<BaseCheckElement> ruleDefinitions = createRules.getBaseCheckElements();
        assertEquals(1, ruleDefinitions.size());
    }
}