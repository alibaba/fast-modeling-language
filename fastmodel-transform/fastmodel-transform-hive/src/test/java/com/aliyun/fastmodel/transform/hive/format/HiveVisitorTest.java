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

package com.aliyun.fastmodel.transform.hive.format;

import java.util.Arrays;

import com.aliyun.fastmodel.core.tree.AliasedName;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.Row;
import com.aliyun.fastmodel.core.tree.expr.literal.LongLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.core.tree.statement.element.MultiComment;
import com.aliyun.fastmodel.core.tree.statement.insert.Insert;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.aliyun.fastmodel.core.tree.util.QueryUtil;
import com.aliyun.fastmodel.transform.hive.context.HiveTransformContext;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/7/26
 */
public class HiveVisitorTest {

    HiveVisitor hiveVisitor = new HiveVisitor(HiveTransformContext.builder().build());

    @Test
    public void testInsertOverwrite() {
        Row row = new Row(ImmutableList.of(new StringLiteral("abc"), new LongLiteral("1")));
        Insert insert = new Insert(true, QualifiedName.of("a.b"), null, QueryUtil
            .query(QueryUtil.values(row, row)),
            ImmutableList.of(new Identifier("col1"), new Identifier("col2")));
        hiveVisitor.visitInsert(insert, 0);
        String visitor = hiveVisitor.getBuilder().toString();
        assertEquals(visitor, "INSERT OVERWRITE TABLE b VALUES \n"
            + "  ('abc', 1)\n"
            + ", ('abc', 1)\n");
    }

    @Test
    public void testInsert() {
        Row row = new Row(ImmutableList.of(new StringLiteral("abc"), new LongLiteral("1")));
        Insert insert = new Insert(false, QualifiedName.of("a.b"), null, QueryUtil
            .query(QueryUtil.values(row, row)),
            ImmutableList.of(new Identifier("col1"), new Identifier("col2")));
        hiveVisitor.visitInsert(insert, 0);
        String visitor = hiveVisitor.getBuilder().toString();
        assertEquals(visitor, "INSERT INTO TABLE b (col1,col2) \n"
            + "  VALUES \n"
            + "  ('abc', 1)\n"
            + ", ('abc', 1)\n");
    }

    @Test
    public void testVisitCreateTableWithoutColumns() {
        CreateTable createTable = CreateTable.builder()
            .tableName(QualifiedName.of("abc"))
            .columnComments(Arrays.asList(
                new MultiComment(
                    ColumnDefinition.builder()
                        .colName(new Identifier("c1"))
                        .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
                        .aliasedName(new AliasedName("c"))
                        .comment(new Comment("comment"))
                        .build()
                )
            ))
            .build();
        hiveVisitor.visitCreateTable(createTable, 0);
        assertEquals(hiveVisitor.getBuilder().toString(), "CREATE TABLE abc\n"
            + "--(\n"
            + "--   c1 BIGINT COMMENT 'comment'\n"
            + "--)");
    }

    @Test
    public void testVisitCreateTableWithColumns() {
        ColumnDefinition build = ColumnDefinition.builder()
            .colName(new Identifier("c1"))
            .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
            .comment(new Comment("comment"))
            .build();
        CreateTable createTable = CreateTable.builder()
            .tableName(QualifiedName.of("abc"))
            .columns(Arrays.asList(
                build
            ))
            .columnComments(Arrays.asList(
                new MultiComment(
                    build
                )
            ))
            .build();
        hiveVisitor.visitCreateTable(createTable, 0);
        assertEquals(hiveVisitor.getBuilder().toString(), "CREATE TABLE abc\n"
            + "(\n"
            + "   c1 BIGINT COMMENT 'comment'\n"
            + ")");
    }

    @Test
    public void testWithDatabase() {
        HiveTransformContext context = HiveTransformContext.builder()
            .database("d1")
            .schema("s1")
            .build();
        HiveVisitor hiveVisitor = new HiveVisitor(context);
        String test = hiveVisitor.getCode(QualifiedName.of("test"));
        assertEquals(test, "d1.s1.test");
        context = HiveTransformContext.builder()
            .schema("s1")
            .build();
        hiveVisitor = new HiveVisitor(context);
        test = hiveVisitor.getCode(QualifiedName.of("test"));
        assertEquals(test, "s1.test");

    }
}