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

package com.aliyun.fastmodel.transform.oracle;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.transform.api.Transformer;
import com.aliyun.fastmodel.transform.api.builder.BuilderFactory;
import com.aliyun.fastmodel.transform.api.builder.StatementBuilder;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.aliyun.fastmodel.transform.api.dialect.Dialect;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.api.dialect.DialectName;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.oracle.context.OracleContext;
import com.aliyun.fastmodel.transform.oracle.parser.OracleParser;
import com.google.auto.service.AutoService;

/**
 * Oracle 11 Transformer
 *
 * @author panguanjing
 * @date 2021/7/24
 */
@Dialect(value = DialectName.ORACLE)
@AutoService(Transformer.class)
public class Oracle11Transformer implements Transformer<BaseStatement> {

    @Override
    public DialectNode transform(BaseStatement source, TransformContext context) {
        OracleContext oracleContext = new OracleContext(context);
        StatementBuilder builder = BuilderFactory.getInstance().getBuilder(source,
            new DialectMeta(DialectName.ORACLE, DialectMeta.DEFAULT_VERSION));
        DialectNode build = builder.build(source, oracleContext);
        return build;
    }

    @Override
    public BaseStatement reverse(DialectNode dialectNode, ReverseContext context) {
        OracleParser oracleParser = new OracleParser();
        Node node = oracleParser.parseNode(dialectNode.getNode(), context);
        return (BaseStatement)node;
    }
}
