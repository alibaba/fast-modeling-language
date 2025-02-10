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

package com.aliyun.fastmodel.transform.hologres;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.transform.api.Transformer;
import com.aliyun.fastmodel.transform.api.builder.BuilderFactory;
import com.aliyun.fastmodel.transform.api.builder.StatementBuilder;
import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.aliyun.fastmodel.transform.api.dialect.Dialect;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.api.dialect.DialectName;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.hologres.context.HologresTransformContext;
import com.aliyun.fastmodel.transform.hologres.dialect.HologresVersion;
import com.aliyun.fastmodel.transform.hologres.dialect.HologresVersion.Constants;
import com.google.auto.service.AutoService;

/**
 * 支持HologresV3的转换处理,3.0和2.0走一样的逻辑
 *
 * @author panguanjing
 * @date 2023/6/26
 */
@Dialect(value = DialectName.Constants.HOLOGRES, version = Constants.V3)
@AutoService(Transformer.class)
public class HologresV3Transformer extends HologresV2Transformer {
    @Override
    public DialectNode transform(BaseStatement source, TransformContext context) {
        DialectMeta dialectMeta = DialectMeta.getByNameAndVersion(DialectName.HOLOGRES.getValue(), HologresVersion.V2);
        StatementBuilder builder = BuilderFactory.getInstance().getBuilder(source, dialectMeta, context);
        HologresTransformContext hologresTransformContext = new HologresTransformContext(context);
        hologresTransformContext.setUseAlterTableSetSentence(true);
        return builder.build(source, hologresTransformContext);
    }
}
