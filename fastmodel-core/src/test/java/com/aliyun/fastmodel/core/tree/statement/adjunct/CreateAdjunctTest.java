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

package com.aliyun.fastmodel.core.tree.statement.adjunct;

import java.util.List;

import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.TestAstVisitor;
import com.aliyun.fastmodel.core.tree.expr.CustomExpression;
import com.aliyun.fastmodel.core.tree.statement.element.CreateElement;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2020/11/25
 */
public class CreateAdjunctTest {
    CreateAdjunct createAdjunct =
        new CreateAdjunct(CreateElement.builder().qualifiedName(QualifiedName.of("a.b")).comment(
            new Comment("comment")
        ).build(),
            new CustomExpression("a=1"));

    @Test
    public void accept() {
        String accept = createAdjunct.accept(new TestAstVisitor(), null);
        assertNull(accept);
    }

    @Test
    public void testToString() {
        assertNotNull(createAdjunct.toString());
    }

    @Test
    public void getChildren() {

        List<? extends Node> children = createAdjunct.getChildren();
        assertEquals(1, children.size());
    }

    @Test
    public void testEquals() {
        CreateAdjunct createAdjunct2 =
            new CreateAdjunct(CreateElement.builder().qualifiedName(QualifiedName.of("a.b")).comment(
                new Comment("comment")).build(),
                new CustomExpression("a=1"));
        assertEquals(createAdjunct2, createAdjunct);

    }

}