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

package com.aliyun.aliyun.transform.zen;

import java.util.List;

import com.aliyun.fastmodel.core.formatter.FastModelVisitor;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * ZenTransformerTest
 *
 * @author panguanjing
 * @date 2021/8/16
 */
public class ZenTransformerTest {

    ZenTransformer zenTransformer;

    @Before
    public void setUp() throws Exception {
        zenTransformer = new ZenTransformer();
    }

    @Test
    public void reverse() {
        Node reverse = zenTransformer.reverse(new DialectNode("user_id\nuser_name"));
        assertNotNull(reverse);
        FastModelVisitor fastModelVisitor = new FastModelVisitor();
        fastModelVisitor.process(reverse);
        assertEquals(fastModelVisitor.getBuilder().toString(), "user_id STRING COMMENT 'user_id';\n"
            + "user_name STRING COMMENT 'user_name';");
    }

    @Test
    public void test_transform() {
        List<ColumnDefinition> columns = ImmutableList.of(
            ColumnDefinition.builder().colName(new Identifier("c1")).dataType(
                DataTypeUtil.simpleType(DataTypeEnums.STRING)).comment(new Comment("comment")).build(),
            ColumnDefinition.builder().colName(new Identifier("c2")).dataType(
                DataTypeUtil.simpleType(DataTypeEnums.BIGINT)).comment(new Comment("comment")).build()
        );
        CreateTable createTable = CreateTable.builder().tableName(QualifiedName.of("dim_shop")).columns(
            columns
        ).build();
        DialectNode transform = zenTransformer.transform(createTable);
        String node = transform.getNode();
        assertEquals(node, "c1 STRING 'comment'\n"
            + "c2 BIGINT 'comment'");
    }

    @Test
    public void test_transform_withoutComment() {
        List<ColumnDefinition> columns = ImmutableList.of(
            ColumnDefinition.builder().colName(new Identifier("c1")).dataType(
                DataTypeUtil.simpleType(DataTypeEnums.STRING)).build(),
            ColumnDefinition.builder().colName(new Identifier("c2")).dataType(
                DataTypeUtil.simpleType(DataTypeEnums.BIGINT)).build()
        );
        CreateTable createTable = CreateTable.builder().tableName(QualifiedName.of("dim_shop")).columns(
            columns
        ).build();
        DialectNode transform = zenTransformer.transform(createTable);
        String node = transform.getNode();
        assertEquals(node, "c1 STRING\n"
            + "c2 BIGINT");
    }
}