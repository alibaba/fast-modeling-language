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

package com.aliyun.fastmodel.transform.hologres.builder;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.transform.api.builder.BuilderAnnotation;
import com.aliyun.fastmodel.transform.api.builder.StatementBuilder;
import com.aliyun.fastmodel.transform.api.dialect.DialectName;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.hologres.context.HologresTransformContext;
import com.aliyun.fastmodel.transform.hologres.format.HologresFormatter;
import com.google.auto.service.AutoService;

/**
 * 默认的builder实现
 *
 * @author panguanjing
 * @date 2021/3/8
 */
@BuilderAnnotation(dialect = DialectName.Constants.HOLOGRES, values = {BaseStatement.class})
@AutoService(StatementBuilder.class)
public class DefaultBuilder implements StatementBuilder<HologresTransformContext> {

    @Override
    public DialectNode build(BaseStatement source, HologresTransformContext context) {
        return HologresFormatter.format(source, context);
    }
}
