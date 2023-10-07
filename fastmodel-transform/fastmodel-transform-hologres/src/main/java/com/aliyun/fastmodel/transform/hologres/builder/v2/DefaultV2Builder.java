/*
 * Copyright (c)  2021. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.hologres.builder.v2;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.transform.api.builder.BuilderAnnotation;
import com.aliyun.fastmodel.transform.api.builder.StatementBuilder;
import com.aliyun.fastmodel.transform.api.dialect.DialectName;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.hologres.context.HologresTransformContext;
import com.aliyun.fastmodel.transform.hologres.dialect.HologresVersion;
import com.aliyun.fastmodel.transform.hologres.dialect.HologresVersion.Constants;
import com.aliyun.fastmodel.transform.hologres.format.HologresFormatter;
import com.google.auto.service.AutoService;

/**
 * 默认的builder实现
 *
 * @author panguanjing
 * @date 2021/3/8
 */
@BuilderAnnotation(dialect = DialectName.Constants.HOLOGRES, version = Constants.V2, values = {BaseStatement.class})
@AutoService(StatementBuilder.class)
public class DefaultV2Builder implements StatementBuilder<HologresTransformContext> {

    @Override
    public DialectNode build(BaseStatement source, HologresTransformContext context) {
        return HologresFormatter.format(source, context, HologresVersion.V2);
    }
}
