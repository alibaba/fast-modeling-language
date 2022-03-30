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

import com.aliyun.fastmodel.conveter.dqc.DefaultConvertContext;
import com.aliyun.fastmodel.conveter.dqc.check.DropColConverter;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.dqc.DropDqcRule;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.DropCol;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * drop col converter test
 *
 * @author panguanjing
 * @date 2021/6/8
 */
public class DropColConverterTest {

    DefaultConvertContext context = new DefaultConvertContext();

    @Before
    public void setUp() throws Exception {
        CreateTable dimShop = CreateTable.builder().tableName(QualifiedName.of("dim_shop")).build();
        context.setBeforeStatement(dimShop);
        context.setAfterStatement(dimShop);
    }

    @Test
    public void testConvert() {
        DropColConverter dropColConverter = new DropColConverter();
        DropCol source = new DropCol(
            QualifiedName.of("dim_shop"),
            new Identifier("col1")
        );
        BaseStatement convert = dropColConverter.convert(source, context);
        DropDqcRule dropRule = (DropDqcRule)convert;
        QualifiedName tableName = dropRule.getTableName();
        assertEquals(tableName, QualifiedName.of("dim_shop"));
        Identifier ruleOrColumn = dropRule.getRuleOrColumn();
        assertEquals(ruleOrColumn, new Identifier("col1"));
    }
}