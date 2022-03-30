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

package com.aliyun.fastmodel.transform.fml;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.datatype.GenericDataType;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.show.ShowObjects;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateDimTable;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.fml.context.FmlTransformContext;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * FmlTransformerTest
 *
 * @author panguanjing
 * @date 2021/1/28
 */
public class FmlTransformerTest {

    @Test
    public void testTransform() {
        FmlTransformer fmlTransformer = new FmlTransformer();
        DialectNode transform = fmlTransformer.transform(CreateDimTable.builder().tableName(
            QualifiedName.of("a.b")
        ).build(), FmlTransformContext.builder().appendSemicolon(true).build());
        assertEquals("CREATE DIM TABLE b;", transform.getNode());
    }

    @Test
    public void testFrom() {
        FmlTransformer fmlTransformer = new FmlTransformer();
        BaseStatement from = fmlTransformer.reverse(new DialectNode("show tables;"));
        assertEquals(from.getClass(), ShowObjects.class);
    }

    @Test
    public void testFilter() {
        ImmutableList<ColumnDefinition> col1 = ImmutableList.of(ColumnDefinition.builder().colName(
            new Identifier("col1")).dataType(
            new GenericDataType(new Identifier(
                DataTypeEnums.BIGINT.name()
            ))).properties(ImmutableList.of(new Property("uuid", "key"))).build()
        );
        CreateDimTable createDimTable = CreateDimTable.builder().tableName(
            QualifiedName.of("a.b")).columns(
            col1
        ).comment(
            new Comment("comment")
        ).build();
        FmlTransformer fmlTransformer = new FmlTransformer();
        DialectNode uuid = fmlTransformer.transform(createDimTable,
            FmlTransformContext.builder().exclude("uuid").build());
        assertEquals(uuid.getNode(),
            "CREATE DIM TABLE b \n"
                + "(\n"
                + "   col1 BIGINT\n"
                + ")\n"
                + "COMMENT 'comment'");

        uuid = fmlTransformer.transform(createDimTable, FmlTransformContext.builder().include("uuid").build());
        assertEquals(uuid.getNode(), "CREATE DIM TABLE b \n"
            + "(\n"
            + "   col1 BIGINT WITH ('uuid'='key')\n"
            + ")\n"
            + "COMMENT 'comment'");
    }
}