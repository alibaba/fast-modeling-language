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

package com.aliyun.fastmodel.transform.graph.builder;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.statement.script.RefDirection;
import com.aliyun.fastmodel.core.tree.statement.script.RefObject;
import com.aliyun.fastmodel.core.tree.statement.script.RefRelation;
import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.aliyun.fastmodel.transform.api.dialect.GenericDialectNode;
import com.aliyun.fastmodel.transform.graph.domain.Edge;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * RefObjectBuilder
 *
 * @author panguanjing
 * @date 2021/12/15
 */
public class RefObjectBuilderTest {

    RefRelationBuilder refObjectBuilder = new RefRelationBuilder();

    @Test
    public void buildGenericNode() {
        BaseStatement source = new RefRelation(
            QualifiedName.of("a.b"),
            new RefObject(QualifiedName.of("t1"), null, new Comment("c1")),
            new RefObject(QualifiedName.of("t2"), null, new Comment("c2")),
            RefDirection.LEFT_DIRECTION_RIGHT
        );
        GenericDialectNode<Edge> edgeGenericDialectNode = refObjectBuilder.buildGenericNode(source,
            TransformContext.builder().build());
        Edge node = edgeGenericDialectNode.getNode();
        String id = node.source();
        assertEquals(id, "t1");
    }
}