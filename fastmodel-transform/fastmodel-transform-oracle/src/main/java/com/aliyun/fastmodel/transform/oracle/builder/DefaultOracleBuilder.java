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

package com.aliyun.fastmodel.transform.oracle.builder;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.transform.api.builder.BuilderAnnotation;
import com.aliyun.fastmodel.transform.api.builder.StatementBuilder;
import com.aliyun.fastmodel.transform.api.dialect.DialectName;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.oracle.context.OracleContext;
import com.aliyun.fastmodel.transform.oracle.format.OracleVisitor;
import com.google.auto.service.AutoService;

/**
 * 默认的builder实现
 *
 * @author panguanjing
 * @date 2021/7/24
 */
@BuilderAnnotation(dialect = DialectName.Constants.ORACLE, values = {BaseStatement.class})
@AutoService(StatementBuilder.class)
public class DefaultOracleBuilder implements StatementBuilder<OracleContext> {

    @Override
    public DialectNode build(BaseStatement source, OracleContext context) {
        OracleVisitor oracleVisitor = new OracleVisitor(context);
        Boolean process = oracleVisitor.process(source, 0);
        String result = oracleVisitor.getBuilder().toString();
        return new DialectNode(result, process);
    }
}
