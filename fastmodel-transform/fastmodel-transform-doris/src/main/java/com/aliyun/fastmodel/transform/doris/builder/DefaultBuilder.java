/*
 * Copyright (c)  2021. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.doris.builder;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.transform.api.builder.BuilderAnnotation;
import com.aliyun.fastmodel.transform.api.builder.StatementBuilder;
import com.aliyun.fastmodel.transform.api.dialect.DialectName.Constants;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.doris.context.DorisContext;
import com.aliyun.fastmodel.transform.doris.format.DorisOutVisitor;
import com.google.auto.service.AutoService;

/**
 * 默认的builder实现
 * 支持doris的DDL输出
 *
 * @author panguanjing
 */
@BuilderAnnotation(dialect = Constants.DORIS, values = {BaseStatement.class})
@AutoService(StatementBuilder.class)
public class DefaultBuilder implements StatementBuilder<DorisContext> {

    @Override
    public DialectNode build(BaseStatement source, DorisContext context) {
        DorisOutVisitor outVisitor = new DorisOutVisitor(context);
        Boolean process = outVisitor.process(source, 0);
        StringBuilder builder = outVisitor.getBuilder();
        return new DialectNode(builder.toString(), process);
    }
}
