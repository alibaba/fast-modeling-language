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

package com.aliyun.aliyun.transform.zen.format;

import java.util.List;

import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/9/15
 */
public class ZenVisitorTest {
    ZenVisitor zenVisitor;

    @Before
    public void setUp() throws Exception {
        zenVisitor = new ZenVisitor(TransformContext.builder().build());
    }

    @Test
    public void testVisitor() {
        List<ColumnDefinition> columns = ImmutableList
            .of(
                ColumnDefinition.builder()
                    .colName(new Identifier("c1"))
                    .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
                    .build()
            );
        CreateTable node = CreateTable.builder()
            .tableName(QualifiedName.of("dim_shop"))
            .columns(columns)
            .build();
        Boolean result = zenVisitor.visitCreateTable(node, 0);
        assertTrue(result);
    }
}