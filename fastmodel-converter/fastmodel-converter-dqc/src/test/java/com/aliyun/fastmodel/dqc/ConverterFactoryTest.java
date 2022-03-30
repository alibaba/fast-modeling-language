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

package com.aliyun.fastmodel.dqc;

import java.util.List;

import com.aliyun.fastmodel.converter.spi.BaseConverterFactory;
import com.aliyun.fastmodel.conveter.dqc.DefaultConvertContext;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.dqc.ChangeDqcRule;
import com.aliyun.fastmodel.core.tree.statement.dqc.ChangeDqcRuleElement;
import com.aliyun.fastmodel.core.tree.statement.table.AddCols;
import com.aliyun.fastmodel.core.tree.statement.table.ChangeCol;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * ConverterExecuteTest
 *
 * @author panguanjing
 * @date 2021/5/31
 */
public class ConverterFactoryTest {

    DefaultConvertContext context = new DefaultConvertContext();

    @Before
    public void setUp() throws Exception {
        CreateTable dimShop = CreateTable.builder().tableName(QualifiedName.of("dim_shop")).build();
        context.setBeforeStatement(dimShop);
        context.setAfterStatement(dimShop);
    }

    @Test
    public void testConveter() {
        List<ColumnDefinition> columns = ImmutableList.of(
            ColumnDefinition.builder().colName(new Identifier("co1"))
                .dataType(DataTypeUtil.simpleType(DataTypeEnums.DATE)).build()
        );
        List<BaseStatement> source = ImmutableList.of(
            CreateTable.builder().columns(columns).tableName(QualifiedName.of("dim_shop")).build());
        List<BaseStatement> convert = BaseConverterFactory.getInstance().convert(source, context);
        assertEquals(0, convert.size());
    }

    @Test
    public void testConverterAddCols() {
        List<BaseStatement> source = ImmutableList.of(
            new AddCols(
                QualifiedName.of("dim_shop"),
                ImmutableList.of(
                    ColumnDefinition.builder().colName(new Identifier("col1")).dataType(
                        DataTypeUtil.simpleType(DataTypeEnums.BIGINT)
                    ).notNull(true).build()
                )
            )
        );
        List<BaseStatement> convert = BaseConverterFactory.getInstance().convert(source, context);
        assertEquals(1, convert.size());
    }

    @Test
    public void testConverterChangeCol() {
        List<BaseStatement> source = ImmutableList.of(
            new ChangeCol(
                QualifiedName.of("dim_shop"),
                new Identifier("c1"),
                ColumnDefinition.builder().colName(new Identifier("col1")).dataType(
                    DataTypeUtil.simpleType(DataTypeEnums.BIGINT)
                ).notNull(true).build()
            )
        );
        List<BaseStatement> convert = BaseConverterFactory.getInstance().convert(source, context);
        assertEquals(1, convert.size());
        ChangeDqcRule changeRules = (ChangeDqcRule)convert.get(0);
        List<ChangeDqcRuleElement> ruleDefinitions = changeRules.getChangeDqcRuleElement();
        assertEquals(ruleDefinitions.size(), 1);
    }
}