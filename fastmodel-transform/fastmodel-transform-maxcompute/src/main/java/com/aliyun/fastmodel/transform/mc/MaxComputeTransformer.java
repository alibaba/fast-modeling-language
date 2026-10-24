/*
 * Copyright 2020 Alibaba Group Holding Ltd.
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

package com.aliyun.fastmodel.transform.mc;

import com.aliyun.fastmodel.common.parser.ThrowingErrorListener;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.transform.api.Transformer;
import com.aliyun.fastmodel.transform.api.builder.BuilderFactory;
import com.aliyun.fastmodel.transform.api.builder.StatementBuilder;
import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.api.client.dto.table.Table;
import com.aliyun.fastmodel.transform.api.client.dto.table.TableConfig;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.aliyun.fastmodel.transform.api.dialect.Dialect;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.api.dialect.DialectName;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.mc.client.converter.MaxComputeClientConverter;
import com.aliyun.fastmodel.transform.mc.context.MaxComputeContext;
import com.aliyun.fastmodel.transform.mc.dialect.MaxComputeVersion.Constants;
import com.aliyun.fastmodel.transform.mc.parser.MaxComputeLanguageParser;
import com.google.auto.service.AutoService;

/**
 * MaxCompute Transformer
 *
 * @author panguanjing
 * @date 2020/10/16
 */
@Dialect(value = DialectName.Constants.MAXCOMPUTE, version = Constants.V2, defaultDialect = true)
@AutoService(Transformer.class)
public class MaxComputeTransformer implements Transformer<BaseStatement> {

    public static final ThrowingErrorListener LISTENER = new ThrowingErrorListener();

    private final MaxComputeLanguageParser maxComputeLanguageParser = new MaxComputeLanguageParser();

    private final MaxComputeClientConverter maxComputeClientConverter = new MaxComputeClientConverter();

    @Override
    public DialectNode transform(BaseStatement source, TransformContext context) {
        checkParam(source);
        //需要转换为maxcompute的context。
        MaxComputeContext maxComputeContext = new MaxComputeContext(context);
        StatementBuilder builder = BuilderFactory.getInstance().getBuilder(source, DialectMeta.getMaxCompute(), maxComputeContext);
        if (builder == null) {
            throw new UnsupportedOperationException(
                "UnSupported statement transform with target Dialect, source: " + source.getClass());
        }
        return builder.build(source, maxComputeContext);
    }

    protected void checkParam(BaseStatement source) {
        if (source == null) {
            throw new IllegalArgumentException("source  can't be null");
        }
    }

    @Override
    public BaseStatement reverse(DialectNode dialectNode, ReverseContext context) {
        return (BaseStatement)maxComputeLanguageParser.parseNode(dialectNode.getNode(), context);
    }

    @Override
    public Node reverseTable(Table table, ReverseContext context) {
        return maxComputeClientConverter.convertToNode(table, TableConfig.builder().build());
    }

    @Override
    public Table transformTable(Node table, TransformContext context) {
        return maxComputeClientConverter.convertToTable(table, new MaxComputeContext(context));
    }

    @Override
    public BaseClientProperty create(String name, String value) {
        return maxComputeClientConverter.getPropertyConverter().create(name, value);
    }
}