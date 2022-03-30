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

package com.aliyun.fastmodel.transform.mysql;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.transform.api.Transformer;
import com.aliyun.fastmodel.transform.api.builder.BuilderFactory;
import com.aliyun.fastmodel.transform.api.builder.StatementBuilder;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.aliyun.fastmodel.transform.api.dialect.Dialect;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.api.dialect.DialectName;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.mysql.context.MysqlTransformContext;
import com.aliyun.fastmodel.transform.mysql.parser.MysqlTransformerParser;
import com.google.auto.service.AutoService;

/**
 * mysql 8.0的转换器
 * 也是默认的语法转换处理
 *
 * @author panguanjing
 * @date 2021/6/24
 */
@Dialect(value = DialectName.MYSQL, version = DialectMeta.DEFAULT_VERSION)
@AutoService(Transformer.class)
public class MysqlTransformer implements Transformer<BaseStatement> {

    MysqlTransformerParser mysqlTransformerParser = new MysqlTransformerParser();

    @Override
    public DialectNode transform(BaseStatement source, TransformContext context) {
        DialectMeta dialectMeta = new DialectMeta(DialectName.MYSQL, DialectMeta.DEFAULT_VERSION);
        StatementBuilder builder = BuilderFactory.getInstance().getBuilder(source, dialectMeta);
        if (builder == null) {
            throw new UnsupportedOperationException(
                "UnSupported statement transform with target Dialect, source: " + source.getClass());
        }
        MysqlTransformContext mysqlTransformContext = new MysqlTransformContext(context);
        return builder.build(source, mysqlTransformContext);
    }

    @Override
    public BaseStatement reverse(DialectNode dialectNode, ReverseContext context) {
        return (BaseStatement)mysqlTransformerParser.parseNode(dialectNode.getNode(), context);
    }
}
