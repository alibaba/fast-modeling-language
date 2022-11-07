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

package com.aliyun.fastmodel.transform.hologres;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.statement.CompositeStatement;
import com.aliyun.fastmodel.transform.api.Transformer;
import com.aliyun.fastmodel.transform.api.builder.BuilderFactory;
import com.aliyun.fastmodel.transform.api.builder.StatementBuilder;
import com.aliyun.fastmodel.transform.api.builder.merge.MergeBuilder;
import com.aliyun.fastmodel.transform.api.builder.merge.impl.CreateTableMergeBuilder;
import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.api.client.dto.table.Table;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.aliyun.fastmodel.transform.api.dialect.Dialect;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.api.dialect.DialectName;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.hologres.client.converter.HologresClientConverter;
import com.aliyun.fastmodel.transform.hologres.context.HologresTransformContext;
import com.aliyun.fastmodel.transform.hologres.parser.HologresParser;
import com.google.auto.service.AutoService;

/**
 * 支持Hologres的转换处理
 *
 * @author panguanjing
 * @date 2021/3/7
 */
@Dialect(DialectName.Constants.HOLOGRES)
@AutoService(Transformer.class)
public class HologresTransformer implements Transformer<BaseStatement> {

    private final HologresClientConverter hologresClientConverter;

    private final HologresParser hologresParser;

    private final MergeBuilder mergeBuilder;

    public HologresTransformer() {
        this.hologresClientConverter = new HologresClientConverter();
        this.hologresParser = new HologresParser();
        this.mergeBuilder = new CreateTableMergeBuilder();
    }

    @Override
    public DialectNode transform(BaseStatement source, TransformContext context) {
        StatementBuilder builder = BuilderFactory.getInstance().getBuilder(source, DialectMeta.getHologres(), context);
        HologresTransformContext context1 = new HologresTransformContext(context);
        DialectNode build = builder.build(source, context1);
        return build;
    }

    @Override
    public BaseStatement reverse(DialectNode dialectNode, ReverseContext context) {
        //判断是否需要merge，如果需要merge，那么返回的merge后的语句
        Node node = hologresParser.parseNode(dialectNode.getNode(), context);
        if (node instanceof CompositeStatement && context.isMerge()) {
            CompositeStatement compositeStatement = (CompositeStatement)node;
            return mergeBuilder.merge(compositeStatement);
        }
        return (BaseStatement)node;
    }

    @Override
    public Node reverseTable(Table table, ReverseContext context) {
        return hologresClientConverter.covertToNode(table);
    }

    @Override
    public Table transformTable(Node table, TransformContext context) {
        HologresTransformContext hologresTransformContext = new HologresTransformContext(context);
        return hologresClientConverter.convertToTable(table, hologresTransformContext);
    }

    @Override
    public BaseClientProperty create(String name, String value) {
        return hologresClientConverter.getPropertyConverter().create(name, value);
    }
}
