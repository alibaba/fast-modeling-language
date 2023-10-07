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

package com.aliyun.fastmodel.core.tree.statement;

import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.TestAstVisitor;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.core.tree.statement.adjunct.DropAdjunct;
import com.aliyun.fastmodel.core.tree.statement.adjunct.SetAdjunctComment;
import com.aliyun.fastmodel.core.tree.statement.adjunct.SetAdjunctProperties;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import static org.junit.Assert.assertNull;

/**
 * @author panguanjing
 * @date 2020/11/25
 */
public class StatementAcceptTest {

    TestAstVisitor testAstVisitor = new TestAstVisitor();

    @Test
    public void testAccept() {
        String accept = new DropAdjunct(QualifiedName.of("a.b")).accept(testAstVisitor, null);
        assertNull(accept);
        String accept1 = new SetAdjunctComment(QualifiedName.of("a"), new Comment("comment")).accept(testAstVisitor,
            null);
        assertNull(accept1);
        String accept2 = new SetAdjunctProperties(QualifiedName.of("a"),
            ImmutableList.of(new Property("key", "value")),
            new StringLiteral("abc")
        ).accept(testAstVisitor, null);
        assertNull(accept2);
    }
}
