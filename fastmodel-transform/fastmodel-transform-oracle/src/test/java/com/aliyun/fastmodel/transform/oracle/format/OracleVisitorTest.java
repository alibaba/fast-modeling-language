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

package com.aliyun.fastmodel.transform.oracle.format;

import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.SetColComment;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableComment;
import com.aliyun.fastmodel.transform.oracle.context.OracleContext;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * OracleVisitorTest
 *
 * @author panguanjing
 * @date 2021/7/28
 */
public class OracleVisitorTest {

    OracleVisitor oracleVisitor = new OracleVisitor(OracleContext.builder().build());

    @Test
    public void testCreateTable() {
        CreateTable node = CreateTable.builder().tableName(QualifiedName.of("dim_shop")).build();
        oracleVisitor.visitCreateTable(node, 0);
        String s = oracleVisitor.getBuilder().toString();
        assertEquals(s, "CREATE TABLE dim_shop;");
    }

    @Test
    public void testComment() {
        SetTableComment setTableComment = new SetTableComment(QualifiedName.of("dim_shop"), new Comment("comment"));
        oracleVisitor.visitSetTableComment(setTableComment, 0);
        String s = oracleVisitor.getBuilder().toString();
        assertEquals(s, "COMMENT ON TABLE dim_shop IS 'comment';");
    }

    @Test
    public void testCommentCol() {
        SetColComment setColComment = new SetColComment(QualifiedName.of("dim_shop"), new Identifier("c1"),
            new Comment("comment"));
        oracleVisitor.visitSetColComment(setColComment, 0);
        String s = oracleVisitor.getBuilder().toString();
        assertEquals("COMMENT ON COLUMN dim_shop.c1 IS 'comment';", s);
    }
}