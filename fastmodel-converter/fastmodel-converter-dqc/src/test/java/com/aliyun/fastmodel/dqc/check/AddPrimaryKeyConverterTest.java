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
import com.aliyun.fastmodel.conveter.dqc.check.AddPrimaryKeyConverter;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.dqc.AddDqcRule;
import com.aliyun.fastmodel.core.tree.statement.dqc.check.BaseCheckElement;
import com.aliyun.fastmodel.core.tree.statement.table.AddConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.PrimaryConstraint;
import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * add primary key
 *
 * @author panguanjing
 * @date 2021/6/7
 */
public class AddPrimaryKeyConverterTest {
    DefaultConvertContext context = new DefaultConvertContext();

    @Before
    public void setUp() throws Exception {
        CreateTable dimShop = CreateTable.builder().tableName(QualifiedName.of("dim_shop")).build();
        context.setBeforeStatement(dimShop);
        context.setAfterStatement(dimShop);
    }

    @Test
    public void testAddPrimaryKey() {
        AddPrimaryKeyConverter addPrimaryKeyConverter = new AddPrimaryKeyConverter();
        AddConstraint source = new AddConstraint(QualifiedName.of("dim_shop"), new PrimaryConstraint(
            new Identifier("abc"),
            ImmutableList.of(new Identifier("c1"), new Identifier("c2"))
        ));
        BaseStatement convert = addPrimaryKeyConverter.convert(source, context);
        AddDqcRule addRules = (AddDqcRule)convert;
        List<BaseCheckElement> ruleDefinitions = addRules.getBaseCheckElements();
        assertEquals(1, ruleDefinitions.size());
    }
}