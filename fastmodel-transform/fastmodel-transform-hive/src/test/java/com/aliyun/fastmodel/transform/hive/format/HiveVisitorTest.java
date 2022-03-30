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

import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.Row;
import com.aliyun.fastmodel.core.tree.expr.literal.LongLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.core.tree.statement.insert.Insert;
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
}