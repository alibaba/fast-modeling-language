/*
 * Copyright (c)  2021. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.adbmysql.builder;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.transform.adbmysql.context.AdbMysqlTransformContext;
import com.aliyun.fastmodel.transform.adbmysql.format.AdbMysqlVisitor;
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
@BuilderAnnotation(dialect = Constants.ADB_MYSQL, values = {BaseStatement.class})
@AutoService(StatementBuilder.class)
public class DefaultBuilder implements StatementBuilder<AdbMysqlTransformContext> {

    @Override
    public DialectNode build(BaseStatement source, AdbMysqlTransformContext context) {
        AdbMysqlVisitor mysqlVisitor = new AdbMysqlVisitor(context);
        Boolean process = mysqlVisitor.process(source, 0);
        String result = mysqlVisitor.getBuilder().toString();
        return new DialectNode(result, process);
    }
}
