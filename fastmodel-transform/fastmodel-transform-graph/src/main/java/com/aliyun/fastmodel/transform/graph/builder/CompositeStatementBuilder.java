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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.statement.CompositeStatement;
import com.aliyun.fastmodel.transform.api.builder.BuilderAnnotation;
import com.aliyun.fastmodel.transform.api.builder.BuilderFactory;
import com.aliyun.fastmodel.transform.api.builder.StatementBuilder;
import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.api.dialect.DialectName;
import com.aliyun.fastmodel.transform.api.dialect.GenericDialectNode;
import com.aliyun.fastmodel.transform.graph.domain.Edge;
import com.aliyun.fastmodel.transform.graph.domain.Element;
import com.aliyun.fastmodel.transform.graph.domain.FmlGraph;
import com.aliyun.fastmodel.transform.graph.domain.Vertex;
import com.google.auto.service.AutoService;
import com.google.common.collect.Maps;

/**
 * 服务对象Builder
 *
 * @author panguanjing
 * @date 2021/12/12
 */
@BuilderAnnotation(values = CompositeStatement.class, dialect = DialectName.GRAPH)
@AutoService(StatementBuilder.class)
public class CompositeStatementBuilder implements StatementBuilder<TransformContext> {

    @Override
    public GenericDialectNode<FmlGraph> buildGenericNode(BaseStatement source, TransformContext context) {
        CompositeStatement compositeStatement = (CompositeStatement)source;
        //define the graph
        FmlGraph graph = new FmlGraph();
        List<BaseStatement> statementList = compositeStatement.getStatements();
        Map<Object, Vertex> vertexMap = Maps.newHashMapWithExpectedSize(16);
        List<Vertex> vertices = new ArrayList<>();
        List<Edge> edges = new ArrayList<>();
        for (BaseStatement baseStatement : statementList) {
            StatementBuilder builder = BuilderFactory.getInstance().getBuilder(baseStatement,
                DialectMeta.getByName(DialectName.GRAPH));
            GenericDialectNode<Element> baseDialectNode = builder.buildGenericNode(baseStatement, context);
            Element node = baseDialectNode.getNode();
            if (node instanceof Vertex) {
                Vertex node1 = (Vertex)node;
                vertices.add(node1);
                vertexMap.put(node1.id(), node1);
            } else if (node instanceof Edge) {
                Edge edge = (Edge)node;
                edges.add(edge);
            }
        }
        //add vertex first then add edge
        vertices.stream().forEach(v -> graph.addVertex(v));
        edges.stream().forEach(v -> graph.addEdge(v));
        return new GenericDialectNode<>(graph);
    }
}
