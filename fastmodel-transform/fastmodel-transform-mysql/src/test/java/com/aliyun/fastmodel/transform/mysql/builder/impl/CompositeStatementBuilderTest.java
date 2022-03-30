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

package com.aliyun.fastmodel.transform.mysql.builder.impl;

import java.util.List;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.CompositeStatement;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.SetColComment;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableComment;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * CompositeStatementBuilderTest
 *
 * @author panguanjing
 * @date 2021/7/27
 */
public class CompositeStatementBuilderTest {

    @Test
    public void build() {
        List<BaseStatement> statements = ImmutableList.of(
            CreateTable.builder()
                .tableName(QualifiedName.of("dim_shop"))
                .comment(new Comment("comment"))
                .build(),
            new SetTableComment(QualifiedName.of("dim_shop"), new Comment("comment"))
        );
        CompositeStatement compositeStatement = new CompositeStatement(
            statements
        );
        CompositeStatementBuilder compositeStatementBuilder = new CompositeStatementBuilder();
        DialectNode build = compositeStatementBuilder.build(compositeStatement, null);
        assertEquals(build.getNode(), "CREATE TABLE dim_shop COMMENT 'comment'");
    }

    @Test
    public void buildWithColumnComment() {
        List<BaseStatement> statements = ImmutableList.of(
            CreateTable.builder()
                .tableName(QualifiedName.of("dim_shop"))
                .comment(new Comment("comment"))
                .columns(
                    ImmutableList.of(
                        ColumnDefinition.builder().colName(new Identifier("c1"))
                            .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
                            .build()
                    )
                )
                .build(),
            new SetTableComment(QualifiedName.of("dim_shop"), new Comment("comment")),
            new SetColComment(QualifiedName.of("dim_shop"), new Identifier("c1"), new Comment("c1 Comment"))
        );
        CompositeStatement compositeStatement = new CompositeStatement(
            statements
        );
        CompositeStatementBuilder compositeStatementBuilder = new CompositeStatementBuilder();
        DialectNode build = compositeStatementBuilder.build(compositeStatement, null);
        assertEquals(build.getNode(), "CREATE TABLE dim_shop\n"
            + "(\n"
            + "   c1 BIGINT COMMENT 'c1 Comment'\n"
            + ") COMMENT 'comment'");
    }

    @Test
    public void buildWithNoMerge() {
        List<BaseStatement> statements = ImmutableList.of(
            CreateTable.builder()
                .tableName(QualifiedName.of("dim_shop"))
                .comment(new Comment("comment"))
                .columns(
                    ImmutableList.of(
                        ColumnDefinition.builder()
                            .colName(new Identifier("c1"))
                            .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
                            .build()
                    )
                )
                .build(),
            new SetTableComment(QualifiedName.of("dim_shop_2"), new Comment("comment"))
        );
        CompositeStatement compositeStatement = new CompositeStatement(
            statements
        );
        CompositeStatementBuilder compositeStatementBuilder = new CompositeStatementBuilder();
        DialectNode build = compositeStatementBuilder.build(compositeStatement, null);
        assertEquals(build.getNode(), "CREATE TABLE dim_shop\n"
            + "(\n"
            + "   c1 BIGINT\n"
            + ") COMMENT 'comment';\n"
            + "ALTER TABLE dim_shop_2 SET COMMENT 'comment';");
    }

    @Test
    public void buildWithNoCreate() {
        List<BaseStatement> statements = ImmutableList.of(
            new SetTableComment(QualifiedName.of("dim_shop_2"), new Comment("comment"))
        );
        CompositeStatement compositeStatement = new CompositeStatement(
            statements
        );
        CompositeStatementBuilder compositeStatementBuilder = new CompositeStatementBuilder();
        DialectNode build = compositeStatementBuilder.build(compositeStatement, null);
        assertEquals(build.getNode(), "ALTER TABLE dim_shop_2 SET COMMENT 'comment'");
    }

    @Test
    public void buildWithTwoCreate() {
        List<BaseStatement> statements = ImmutableList.of(
            CreateTable.builder()
                .tableName(QualifiedName.of("dim_shop"))
                .comment(new Comment("comment"))
                .columns(
                    ImmutableList.of(
                        ColumnDefinition.builder()
                            .colName(new Identifier("c1"))
                            .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
                            .build()
                    )
                )
                .build(),
            CreateTable.builder()
                .tableName(QualifiedName.of("dim_shop2"))
                .comment(new Comment("comment"))
                .columns(
                    ImmutableList.of(
                        ColumnDefinition.builder()
                            .colName(new Identifier("c1"))
                            .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
                            .build()
                    )
                )
                .build()
        );
        CompositeStatement compositeStatement = new CompositeStatement(
            statements
        );
        CompositeStatementBuilder compositeStatementBuilder = new CompositeStatementBuilder();
        DialectNode build = compositeStatementBuilder.build(compositeStatement, null);
        assertEquals(build.getNode(), "CREATE TABLE dim_shop\n"
            + "(\n"
            + "   c1 BIGINT\n"
            + ") COMMENT 'comment';\n"
            + "CREATE TABLE dim_shop2\n"
            + "(\n"
            + "   c1 BIGINT\n"
            + ") COMMENT 'comment';");
    }
}