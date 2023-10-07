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
import com.aliyun.fastmodel.conveter.dqc.check.AddColsConverter;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.constants.ColumnPropertyDefaultKey;
import com.aliyun.fastmodel.core.tree.statement.dqc.AddDqcRule;
import com.aliyun.fastmodel.core.tree.statement.dqc.check.BaseCheckElement;
import com.aliyun.fastmodel.core.tree.statement.table.AddCols;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/6/8
 */
public class AddColsConverterTest {

    AddColsConverter addColsConverter = new AddColsConverter();
    DefaultConvertContext context = new DefaultConvertContext();

    @Before
    public void setUp() throws Exception {
        CreateTable dim_shop = CreateTable.builder().tableName(QualifiedName.of("dim_shop")).build();
        context.setBeforeStatement(dim_shop);
        context.setAfterStatement(dim_shop);
    }

    @Test
    public void testAddCols() {
        AddCols source = new AddCols(
            QualifiedName.of("a.b"),
            ImmutableList.of(
                ColumnDefinition.builder().colName(new Identifier("a")).dataType(
                    DataTypeUtil.simpleType(DataTypeEnums.BIGINT)
                ).notNull(true).build()
            )
        );
        BaseStatement convert = addColsConverter.convert(source, context);
        assertEquals(AddDqcRule.class, convert.getClass());
    }

    @Test
    public void testAddColsWithCode() {
        AddCols source = new AddCols(
            QualifiedName.of("a.b"),
            ImmutableList.of(
                ColumnDefinition
                    .builder()
                    .colName(new Identifier("a"))
                    .dataType(
                        DataTypeUtil.simpleType(DataTypeEnums.BIGINT)
                    ).notNull(true)
                    .properties(ImmutableList.of(new Property(ColumnPropertyDefaultKey.code_table.name(), "c1")))
                    .build()
            )
        );
        BaseStatement convert = addColsConverter.convert(source, context);
        assertEquals(AddDqcRule.class, convert.getClass());
    }

    @Test
    public void testRemoveDuplicate() {
        AddCols addCols = new AddCols(
            QualifiedName.of("a.b"),
            ImmutableList.of(
                ColumnDefinition.builder().colName(new Identifier("a")).dataType(
                    DataTypeUtil.simpleType(DataTypeEnums.BIGINT)
                ).primary(true).notNull(true).build()
            ));
        BaseStatement convert = addColsConverter.convert(addCols, context);
        AddDqcRule addRules = (AddDqcRule)convert;
        List<BaseCheckElement> ruleDefinitions = addRules.getBaseCheckElements();
        assertEquals(2, ruleDefinitions.size());
    }
}