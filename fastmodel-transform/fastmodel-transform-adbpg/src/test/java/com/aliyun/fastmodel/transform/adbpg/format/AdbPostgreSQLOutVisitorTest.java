/*
 * Copyright [2024] [name of copyright owner]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.fastmodel.transform.adbpg.format;

import java.util.List;

import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.aliyun.fastmodel.transform.adbpg.context.AdbPostgreSQLTransformContext;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.desc.DistributeNonKeyConstraint;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class AdbPostgreSQLOutVisitorTest {
    @Mock
    AdbPostgreSQLTransformContext context;
    // Field builder of type StringBuilder - was not mocked since Mockito doesn't mock a Final class when 'mock-maker-inline' option is not set
    @InjectMocks
    AdbPostgreSQLOutVisitor adbPostgreSQLOutVisitor;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testVisitCreateTable() throws Exception {
        AdbPostgreSQLTransformContext context = AdbPostgreSQLTransformContext.builder().build();
        AdbPostgreSQLOutVisitor adbPostgreSQLOutVisitor = new AdbPostgreSQLOutVisitor(context);
        List<ColumnDefinition> columns = Lists.newArrayList();
        columns.add(ColumnDefinition.builder().colName(new Identifier("c1")).dataType(DataTypeUtil.simpleType("INT", null)).build());

        List<BaseConstraint> constraints = Lists.newArrayList();
        constraints.add(new DistributeNonKeyConstraint(Lists.newArrayList(new Identifier("c1")), null, null, null));
        CreateTable node = CreateTable.builder()
            .tableName(QualifiedName.of("abc"))
            .columns(columns)
            .constraints(constraints)
            .build();
        adbPostgreSQLOutVisitor.visitCreateTable(node, 0);

        String s = adbPostgreSQLOutVisitor.getBuilder().toString();
        Assert.assertEquals("CREATE TABLE abc (\n"
            + "   c1 INT\n"
            + ")\n"
            + "DISTRIBUTED BY (c1)\n"
            + ";", s);
    }
}

// Generated with love by TestMe :) Please report issues and submit feature requests at: http://weirddev.com/forum#!/testme