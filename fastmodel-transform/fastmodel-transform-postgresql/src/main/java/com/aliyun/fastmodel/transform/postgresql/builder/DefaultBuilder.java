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

package com.aliyun.fastmodel.transform.postgresql.builder;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.transform.api.builder.BuilderAnnotation;
import com.aliyun.fastmodel.transform.api.builder.StatementBuilder;
import com.aliyun.fastmodel.transform.api.dialect.DialectName.Constants;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.postgresql.context.PostgreSQLTransformContext;
import com.aliyun.fastmodel.transform.postgresql.format.PostgreSQLOutVisitor;
import com.google.auto.service.AutoService;
import org.apache.commons.lang3.StringUtils;

import static com.aliyun.fastmodel.transform.api.context.TransformContext.SEMICOLON;

/**
 * PostgreSQL 默认 builder 实现
 *
 * @author panguanjing
 * @date 2026/3/31
 */
@BuilderAnnotation(dialect = Constants.POSTGRESQL, values = {BaseStatement.class})
@AutoService(StatementBuilder.class)
public class DefaultBuilder implements StatementBuilder<PostgreSQLTransformContext> {

    @Override
    public DialectNode build(BaseStatement source, PostgreSQLTransformContext context) {
        PostgreSQLOutVisitor visitor = new PostgreSQLOutVisitor(context);
        Boolean executable = visitor.process(source, 0);
        String result = visitor.getBuilder().toString();
        if (context.isAppendSemicolon() && !StringUtils.endsWith(result, SEMICOLON)) {
            result = result + SEMICOLON;
        }
        return new DialectNode(result, executable);
    }
}
