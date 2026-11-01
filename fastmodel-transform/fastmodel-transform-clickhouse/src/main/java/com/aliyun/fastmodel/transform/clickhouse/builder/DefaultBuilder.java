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

package com.aliyun.fastmodel.transform.clickhouse.builder;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.transform.api.builder.BuilderAnnotation;
import com.aliyun.fastmodel.transform.api.builder.StatementBuilder;
import com.aliyun.fastmodel.transform.api.dialect.DialectName.Constants;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.clickhouse.context.ClickHouseContext;
import com.aliyun.fastmodel.transform.clickhouse.format.ClickHouseOutVisitor;
import com.aliyun.fastmodel.transform.clickhouse.parser.tree.ClickHouseAlterTable;
import com.google.auto.service.AutoService;

/**
 * clickHouse默认的builder实现
 *
 * @author panguanjing
 */
@BuilderAnnotation(dialect = Constants.CLICKHOUSE, values = {BaseStatement.class})
@AutoService(StatementBuilder.class)
public class DefaultBuilder implements StatementBuilder<ClickHouseContext> {

    @Override
    public DialectNode build(BaseStatement source, ClickHouseContext context) {
        ClickHouseOutVisitor clickHouseOutVisitor = new ClickHouseOutVisitor(context);
        if (source instanceof ClickHouseAlterTable) {
            return new DialectNode(clickHouseOutVisitor.formatAlterTable((ClickHouseAlterTable)source), Boolean.TRUE);
        }
        Boolean process = clickHouseOutVisitor.process(source, 0);
        return new DialectNode(clickHouseOutVisitor.getBuilder().toString(), process);
    }
}
