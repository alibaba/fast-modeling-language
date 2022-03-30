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

package com.aliyun.fastmodel.transform.graph;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.statement.CompositeStatement;
import com.aliyun.fastmodel.transform.api.Transformer;
import com.aliyun.fastmodel.transform.api.builder.BuilderFactory;
import com.aliyun.fastmodel.transform.api.builder.StatementBuilder;
import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.aliyun.fastmodel.transform.api.dialect.Dialect;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.api.dialect.DialectName;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.api.dialect.GenericDialectNode;
import com.aliyun.fastmodel.transform.graph.context.GraphTransformContext;
import com.aliyun.fastmodel.transform.graph.domain.FmlGraph;
import com.aliyun.fastmodel.transform.graph.exporter.Exporter;
import com.aliyun.fastmodel.transform.graph.exporter.ExporterFactory;
import com.google.auto.service.AutoService;
import com.google.common.collect.Lists;

/**
 * Guava Graph的转换类
 *
 * @author panguanjing
 * @date 2021/12/12
 */
@Dialect(DialectName.GRAPH)
@AutoService(Transformer.class)
public class GraphTransformer implements Transformer<BaseStatement> {

    @Override
    public DialectNode transform(BaseStatement source, TransformContext context) {
        boolean notCompositeStatement = source instanceof CompositeStatement;
        //convert to composite statment
        CompositeStatement compositeStatement = null;
        if (!notCompositeStatement) {
            compositeStatement = new CompositeStatement(Lists.newArrayList(source));
        } else {
            compositeStatement = (CompositeStatement)source;
        }
        StatementBuilder builder = BuilderFactory.getInstance().getBuilder(compositeStatement,
            DialectMeta.getByName(DialectName.GRAPH));
        GenericDialectNode<FmlGraph> genericDialectNode = builder.buildGenericNode(compositeStatement, context);
        GraphTransformContext graphTransformContext = new GraphTransformContext(context);
        Exporter exporter = ExporterFactory.getInstance().getExporter(graphTransformContext.getExportName());
        FmlGraph node = genericDialectNode.getNode();
        String graphResult = exporter.exportGraph(node);
        return new DialectNode(graphResult);
    }
}
