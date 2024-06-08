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

package com.aliyun.fastmodel.transform.hive;

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
import com.aliyun.fastmodel.transform.hive.client.converter.HiveClientConverter;
import com.aliyun.fastmodel.transform.hive.context.HiveTransformContext;
import com.aliyun.fastmodel.transform.hive.parser.HiveLanguageParser;
import com.google.auto.service.AutoService;

/**
 * Hive的转换器
 *
 * @author panguanjing
 * @date 2021/1/29
 */
@AutoService(Transformer.class)
@Dialect(DialectName.Constants.HIVE)
public class HiveTransformer implements Transformer<BaseStatement> {
    private HiveLanguageParser hiveLanguageParser = new HiveLanguageParser();

    private HiveClientConverter hiveClientConverter = new HiveClientConverter();

    @Override
    public DialectNode transform(BaseStatement source, TransformContext context) {
        if (source == null) {
            throw new IllegalArgumentException("source can't be null");
        }
        StatementBuilder builder = BuilderFactory.getInstance().getBuilder(source, DialectMeta.getHive(), context);
        if (builder == null) {
            throw new UnsupportedOperationException(
                "UnSupported statement transform with target Dialect, source: " + source.getClass());
        }
        HiveTransformContext hiveTransformContext = new HiveTransformContext(context);
        DialectNode build = builder.build(source, hiveTransformContext);
        return build;
    }

    @Override
    public BaseStatement reverse(DialectNode dialectNode, ReverseContext context) {
        return (BaseStatement)hiveLanguageParser.parseNode(dialectNode.getNode(), context);
    }

    @Override
    public Node reverseTable(Table table, ReverseContext context) {
        return hiveClientConverter.covertToNode(table, TableConfig.builder().build());
    }

    @Override
    public Table transformTable(Node table, TransformContext context) {
        return hiveClientConverter.convertToTable(table, new HiveTransformContext(context));
    }

    @Override
    public BaseClientProperty create(String name, String value) {
        return hiveClientConverter.getPropertyConverter().create(name, value);
    }

}
