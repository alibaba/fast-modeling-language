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

package com.aliyun.fastmodel.transform.postgresql;

import java.util.List;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.NumericParameter;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.postgresql.context.PostgreSQLTransformContext;
import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * PostgreSQL Transformer 测试
 *
 * @author panguanjing
 * @date 2026/3/31
 */
public class PostgreSQLTransformerTest {

    PostgreSQLTransformer transformer = new PostgreSQLTransformer();

    @Test
    public void testTransformSimple() {
        List<ColumnDefinition> columns = Lists.newArrayList();
        columns.add(ColumnDefinition.builder()
            .colName(new Identifier("id"))
            .dataType(DataTypeUtil.simpleType("bigint", null))
            .build());
        columns.add(ColumnDefinition.builder()
            .colName(new Identifier("name"))
            .dataType(DataTypeUtil.simpleType("varchar", Lists.newArrayList(new NumericParameter("100"))))
            .build());
        BaseStatement source = CreateTable.builder()
            .tableName(QualifiedName.of("users"))
            .columns(columns)
            .build();
        TransformContext context = PostgreSQLTransformContext.builder().build();
        DialectNode result = transformer.transform(source, context);
        assertNotNull(result);
        assertTrue(result.getNode().contains("CREATE TABLE"));
        assertTrue(result.getNode().contains("users"));
    }

    @Test
    public void testTransformWithComment() {
        List<ColumnDefinition> columns = Lists.newArrayList();
        columns.add(ColumnDefinition.builder()
            .colName(new Identifier("id"))
            .dataType(DataTypeUtil.simpleType("integer", null))
            .comment(new Comment("主键ID"))
            .build());
        columns.add(ColumnDefinition.builder()
            .colName(new Identifier("name"))
            .dataType(DataTypeUtil.simpleType("varchar", Lists.newArrayList(new NumericParameter("200"))))
            .comment(new Comment("名称"))
            .build());
        BaseStatement source = CreateTable.builder()
            .tableName(QualifiedName.of("products"))
            .columns(columns)
            .comment(new Comment("商品表"))
            .build();
        TransformContext context = PostgreSQLTransformContext.builder().build();
        DialectNode result = transformer.transform(source, context);
        assertNotNull(result);
        assertTrue(result.getNode().contains("COMMENT ON TABLE products IS"));
        assertTrue(result.getNode().contains("COMMENT ON COLUMN products.id IS"));
        assertTrue(result.getNode().contains("COMMENT ON COLUMN products.name IS"));
    }

    @Test
    public void testNoBeginCommit() {
        List<ColumnDefinition> columns = Lists.newArrayList();
        columns.add(ColumnDefinition.builder()
            .colName(new Identifier("id"))
            .dataType(DataTypeUtil.simpleType("integer", null))
            .build());
        BaseStatement source = CreateTable.builder()
            .tableName(QualifiedName.of("test"))
            .columns(columns)
            .build();
        DialectNode result = transformer.transform(source, PostgreSQLTransformContext.builder().build());
        String ddl = result.getNode();
        assertTrue(!ddl.contains("BEGIN;"));
        assertTrue(!ddl.contains("COMMIT;"));
    }
}
