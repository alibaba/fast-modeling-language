/*
 * Copyright (c)  2021. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.oceanbase.builder;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.transform.api.builder.BuilderAnnotation;
import com.aliyun.fastmodel.transform.api.builder.StatementBuilder;
import com.aliyun.fastmodel.transform.api.dialect.DialectName.Constants;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.oceanbase.context.OceanBaseContext;
import com.aliyun.fastmodel.transform.oceanbase.format.OceanBaseMysqlOutVisitor;
import com.google.auto.service.AutoService;

/**
 * 默认的builder实现
 * 支持oceanbase mysql的DDL输出
 *
 * @author panguanjing
 */
@BuilderAnnotation(dialect = Constants.OB_MYSQL, values = {BaseStatement.class})
@AutoService(StatementBuilder.class)
public class DefaultBuilder implements StatementBuilder<OceanBaseContext> {

    @Override
    public DialectNode build(BaseStatement source, OceanBaseContext context) {
        OceanBaseMysqlOutVisitor outVisitor = new OceanBaseMysqlOutVisitor(context);
        Boolean process = outVisitor.process(source, 0);
        StringBuilder builder = outVisitor.getBuilder();
        return new DialectNode(builder.toString(), process);
    }
}
