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
import com.aliyun.fastmodel.conveter.dqc.check.DropTableConverter;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.dqc.AddDqcRule;
import com.aliyun.fastmodel.core.tree.statement.dqc.check.BaseCheckElement;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition.ColumnBuilder;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.DropTable;
import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * drop table converter
 *
 * @author panguanjing
 * @date 2021/6/23
 */
public class DropTableConverterTest {

    DropTableConverter dropTableConverter = new DropTableConverter();

    DefaultConvertContext context = new DefaultConvertContext();

    @Before
    public void setUp() throws Exception {
        ColumnBuilder col1 = ColumnDefinition.builder().colName(new Identifier("col1")).primary(true);
        CreateTable build = CreateTable.builder().tableName(QualifiedName.of("dim_shop")).
            columns(
                ImmutableList.of(col1.build())
            ).build();
        CreateTable createTable = build;
        context.setBeforeStatement(createTable);
        context.setAfterStatement(createTable);
    }

    @Test
    public void testConverter() {
        DropTable source = new DropTable(
            QualifiedName.of("dim_shop")
        );
        BaseStatement convert = dropTableConverter.convert(source, context);
        AddDqcRule addRules = (AddDqcRule)convert;
        List<BaseCheckElement> ruleDefinitions = addRules.getBaseCheckElements();
        assertEquals(1, ruleDefinitions.size());
        BaseCheckElement ruleDefinition = ruleDefinitions.get(0);
        assertEquals("ALTER DQC_RULE ON TABLE dim_shop\n"
            + "ADD (\n"
            + "   CONSTRAINT `字段规则-唯一-(col1)` CHECK(DUPLICATE_COUNT(col1) = 0) NOT ENFORCED DISABLE\n"
            + ")", addRules.toString());
    }
}
