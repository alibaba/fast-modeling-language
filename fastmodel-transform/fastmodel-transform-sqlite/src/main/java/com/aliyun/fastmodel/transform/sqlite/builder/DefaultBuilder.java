/*
 * Copyright (c)  2021. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.sqlite.builder;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.transform.api.builder.BuilderAnnotation;
import com.aliyun.fastmodel.transform.api.builder.StatementBuilder;
import com.aliyun.fastmodel.transform.api.dialect.DialectName.Constants;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.sqlite.context.SqliteContext;
import com.aliyun.fastmodel.transform.sqlite.format.SqliteVisitor;
import com.google.auto.service.AutoService;

/**
 * 默认的builder实现
 *
 * @author panguanjing
 * @date 2021/3/8
 */
@BuilderAnnotation(dialect = Constants.SQLITE, values = {BaseStatement.class})
@AutoService(StatementBuilder.class)
public class DefaultBuilder implements StatementBuilder<SqliteContext> {

    @Override
    public DialectNode build(BaseStatement source, SqliteContext context) {
        SqliteVisitor hologresVisitor = new SqliteVisitor(context);
        Boolean process = hologresVisitor.process(source, 0);
        StringBuilder builder = hologresVisitor.getBuilder();
        return new DialectNode(builder.toString(), process);
    }
}
