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

package com.aliyun.aliyun.transform.zen;

import java.util.List;

import com.aliyun.aliyun.transform.zen.converter.ZenNodeConverter;
import com.aliyun.aliyun.transform.zen.converter.ZenNodeConverterFactory;
import com.aliyun.aliyun.transform.zen.parser.ZenParser;
import com.aliyun.aliyun.transform.zen.parser.ZenParserImpl;
import com.aliyun.aliyun.transform.zen.parser.tree.BaseZenNode;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.ListNode;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.transform.api.Transformer;
import com.aliyun.fastmodel.transform.api.builder.BuilderFactory;
import com.aliyun.fastmodel.transform.api.builder.StatementBuilder;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.aliyun.fastmodel.transform.api.dialect.Dialect;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.api.dialect.DialectName;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.api.dialect.IVersion;
import com.google.auto.service.AutoService;

/**
 * ZenTransformer
 *
 * @author panguanjing
 * @date 2021/8/16
 */
@AutoService(Transformer.class)
@Dialect(DialectName.Constants.ZEN)
public class ZenTransformer implements Transformer<Node> {

    private static final ZenParser ZEN_PARSER = new ZenParserImpl();

    @Override
    public Node reverse(DialectNode dialectNode, ReverseContext context) {
        BaseZenNode baseZenNode = ZEN_PARSER.parseNode(dialectNode.getNode());
        ZenNodeConverter zenNodeConverter = ZenNodeConverterFactory.getInstance().create(ColumnDefinition.class);
        List<ColumnDefinition> convert = zenNodeConverter.convert(baseZenNode);
        return new ListNode(convert);
    }

    @Override
    public DialectNode transform(Node source, TransformContext context) {
        BaseStatement baseStatement = null;
        if (source instanceof BaseStatement) {
            baseStatement = (BaseStatement)source;
        }
        if (baseStatement == null) {
            throw new UnsupportedOperationException("unsupported source expect statement:" + source.getClass());
        }
        StatementBuilder builder = BuilderFactory.getInstance().getBuilder(baseStatement,
            new DialectMeta(DialectName.ZEN, IVersion.getDefault()), context);
        return builder.build(baseStatement, context);
    }
}
