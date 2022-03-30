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

package com.aliyun.fastmodel.converter.examples;

import java.util.List;

import com.aliyun.fastmodel.converter.spi.BaseConverterFactory;
import com.aliyun.fastmodel.conveter.dqc.DefaultConvertContext;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.DropConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.PrimaryConstraint;
import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/6/14
 */
public class DemoConverterTest {

    BaseConverterFactory converterFactory;

    DefaultConvertContext context = new DefaultConvertContext();

    @Before
    public void setUp() throws Exception {
        converterFactory = BaseConverterFactory.getInstance();
        CreateTable createTable = CreateTable.builder().tableName(QualifiedName.of("dim_shop")).
            constraints(
                ImmutableList.of(new PrimaryConstraint(new Identifier("c1"), ImmutableList.of(
                    new Identifier("pk")
                )))
            ).partition(null).columns(
                ImmutableList.of(ColumnDefinition.builder().colName(new Identifier("pk")).build())).build();
        context.setBeforeStatement(createTable);
        context.setAfterStatement(createTable);
    }

    @Test
    public void testConverter() {
        List<BaseStatement> convert = converterFactory.convert(
            new DropConstraint(QualifiedName.of("dim_shop"), new Identifier("c1")), context);
        assertEquals(1, convert.size());
        BaseStatement baseStatement = convert.get(0);
        assertEquals("ALTER DQC_RULE ON TABLE dim_shop\n"
            + "ADD (\n"
            + "   CONSTRAINT `字段规则-唯一-(pk)` CHECK(DUPLICATE_COUNT(pk) = 0) NOT ENFORCED DISABLE\n"
            + ")", baseStatement.toString());
    }

}