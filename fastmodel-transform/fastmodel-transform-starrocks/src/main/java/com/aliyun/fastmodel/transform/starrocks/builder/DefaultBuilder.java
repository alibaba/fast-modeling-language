/*
 * Copyright (c)  2021. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.starrocks.builder;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.transform.api.builder.BuilderAnnotation;
import com.aliyun.fastmodel.transform.api.builder.StatementBuilder;
import com.aliyun.fastmodel.transform.api.dialect.DialectName.Constants;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.starrocks.context.StarRocksContext;
import com.aliyun.fastmodel.transform.starrocks.format.StarRocksOutVisitor;
import com.google.auto.service.AutoService;

/**
 * 默认的builder实现
 *
 * @author panguanjing
 */
@BuilderAnnotation(dialect = Constants.STARROCKS, values = {BaseStatement.class})
@AutoService(StatementBuilder.class)
public class DefaultBuilder implements StatementBuilder<StarRocksContext> {

    @Override
    public DialectNode build(BaseStatement source, StarRocksContext context) {
        StarRocksOutVisitor starRocksVisitor = new StarRocksOutVisitor(context);
        Boolean process = starRocksVisitor.process(source, 0);
        StringBuilder builder = starRocksVisitor.getBuilder();
        return new DialectNode(builder.toString(), process);
    }
}
