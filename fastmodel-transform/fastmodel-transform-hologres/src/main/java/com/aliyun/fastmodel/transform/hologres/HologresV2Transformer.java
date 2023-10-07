/*
 * Copyright (c)  2021. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
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
 * 支持HologresV2的转换处理
 *
 * @author panguanjing
 * @date 2023/6/26
 */
@Dialect(value = DialectName.Constants.HOLOGRES, version = Constants.V2)
@AutoService(Transformer.class)
public class HologresV2Transformer extends HologresTransformer {

    @Override
    public DialectNode transform(BaseStatement source, TransformContext context) {
        DialectMeta dialectMeta = DialectMeta.getByNameAndVersion(DialectName.HOLOGRES.getValue(),
            HologresVersion.V2);
        StatementBuilder builder = BuilderFactory.getInstance().getBuilder(source, dialectMeta, context);
        HologresTransformContext hologresTransformContext = new HologresTransformContext(context);
        DialectNode build = builder.build(source, hologresTransformContext);
        return build;
    }

}
