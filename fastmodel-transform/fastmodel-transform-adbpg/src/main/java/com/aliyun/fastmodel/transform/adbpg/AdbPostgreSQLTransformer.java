/*
 * Copyright [2024] [name of copyright owner]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.fastmodel.transform.adbpg;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.transform.adbpg.client.converter.AdbPostgreSQLClientConverter;
import com.aliyun.fastmodel.transform.adbpg.context.AdbPostgreSQLTransformContext;
import com.aliyun.fastmodel.transform.adbpg.parser.AdbPostgreSQLLanguageParser;
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
import com.aliyun.fastmodel.transform.api.dialect.DialectName.Constants;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.api.dialect.IVersion;
import com.google.auto.service.AutoService;

/**
 * Adb pg的转换处理
 *
 * @author panguanjing
 * @date 2024/10/13
 */
@Dialect(value = Constants.ADB_PG, defaultDialect = true)
@AutoService(Transformer.class)
public class AdbPostgreSQLTransformer implements Transformer<BaseStatement> {

    private final AdbPostgreSQLLanguageParser adbPostgreSQLParser = new AdbPostgreSQLLanguageParser();

    private final AdbPostgreSQLClientConverter adbPostgreSQLClientConverter = new AdbPostgreSQLClientConverter();

    @Override
    public DialectNode transform(BaseStatement source, TransformContext context) {
        DialectMeta dialectMeta = new DialectMeta(DialectName.ADB_PG, IVersion.getDefault());
        AdbPostgreSQLTransformContext oceanBaseContext = new AdbPostgreSQLTransformContext(context);
        StatementBuilder<TransformContext> builder = BuilderFactory.getInstance().getBuilder(source, dialectMeta, oceanBaseContext);
        if (builder == null) {
            throw new UnsupportedOperationException(
                "UnSupported statement transform with target Dialect, source: " + source.getClass());
        }
        return builder.build(source, oceanBaseContext);
    }

    @Override
    public BaseStatement reverse(DialectNode dialectNode, ReverseContext context) {
        return (BaseStatement)adbPostgreSQLParser.parseNode(dialectNode.getNode(), context);
    }

    @Override
    public Node reverseTable(Table table, ReverseContext context) {
        return adbPostgreSQLClientConverter.convertToNode(table, TableConfig.builder().build());
    }

    @Override
    public Table transformTable(Node table, TransformContext context) {
        return adbPostgreSQLClientConverter.convertToTable(table, new AdbPostgreSQLTransformContext(context));
    }

    @Override
    public BaseClientProperty create(String name, String value) {
        return adbPostgreSQLClientConverter.getPropertyConverter().create(name, value);
    }
}
