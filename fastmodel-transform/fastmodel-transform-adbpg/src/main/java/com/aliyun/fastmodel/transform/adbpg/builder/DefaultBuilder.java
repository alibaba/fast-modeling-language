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

package com.aliyun.fastmodel.transform.adbpg.builder;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.transform.adbpg.context.AdbPostgreSQLTransformContext;
import com.aliyun.fastmodel.transform.adbpg.format.AdbPostgreSQLOutVisitor;
import com.aliyun.fastmodel.transform.api.builder.BuilderAnnotation;
import com.aliyun.fastmodel.transform.api.builder.StatementBuilder;
import com.aliyun.fastmodel.transform.api.dialect.DialectName.Constants;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.google.auto.service.AutoService;

/**
 * 默认的builder实现
 *
 * @author panguanjing
 * @date 2021/3/8
 */
@BuilderAnnotation(dialect = Constants.ADB_PG, values = {BaseStatement.class})
@AutoService(StatementBuilder.class)
public class DefaultBuilder implements StatementBuilder<AdbPostgreSQLTransformContext> {

    @Override
    public DialectNode build(BaseStatement source, AdbPostgreSQLTransformContext context) {
        AdbPostgreSQLOutVisitor mysqlVisitor = new AdbPostgreSQLOutVisitor(context);
        Boolean process = mysqlVisitor.process(source, 0);
        String result = mysqlVisitor.getBuilder().toString();
        return new DialectNode(result, process);
    }
}
