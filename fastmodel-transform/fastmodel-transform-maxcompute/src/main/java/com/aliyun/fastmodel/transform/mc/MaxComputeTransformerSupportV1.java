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

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.transform.api.Transformer;
import com.aliyun.fastmodel.transform.api.builder.BuilderFactory;
import com.aliyun.fastmodel.transform.api.builder.StatementBuilder;
import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.aliyun.fastmodel.transform.api.dialect.Dialect;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.api.dialect.DialectName;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.api.dialect.IVersion;
import com.aliyun.fastmodel.transform.mc.context.MaxComputeContext;
import com.aliyun.fastmodel.transform.mc.dialect.MaxComputeVersion;
import com.google.auto.service.AutoService;

/**
 * MaxCompute Transformer
 * 默认采用V1版本
 *
 * @author panguanjing
 * @date 2020/10/16
 */
@Dialect(value = DialectName.Constants.MAXCOMPUTE, version = MaxComputeVersion.Constants.V1)
@AutoService(Transformer.class)
public class MaxComputeTransformerSupportV1 extends MaxComputeTransformer {

    @Override
    public DialectNode transform(BaseStatement source, TransformContext context) {
        checkParam(source);
        MaxComputeContext maxComputeContext = new MaxComputeContext(context);
        maxComputeContext.setUseV1DataType(true);
        StatementBuilder builder = BuilderFactory.getInstance().getBuilder(source,
            DialectMeta.getMaxCompute(), context);
        if (builder == null) {
            throw new UnsupportedOperationException(
                "UnSupported statement transform with target Dialect, source: " + source.getClass());
        }
        return builder.build(source, maxComputeContext);
    }

}
