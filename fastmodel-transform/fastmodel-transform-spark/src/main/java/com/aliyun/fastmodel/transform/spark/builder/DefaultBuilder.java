/*
 * Copyright (c)  2021. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.spark.builder;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.transform.api.builder.BuilderAnnotation;
import com.aliyun.fastmodel.transform.api.builder.StatementBuilder;
import com.aliyun.fastmodel.transform.api.dialect.DialectName.Constants;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.spark.context.SparkTransformContext;
import com.aliyun.fastmodel.transform.spark.format.SparkVisitor;
import com.google.auto.service.AutoService;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

import static com.aliyun.fastmodel.transform.api.context.TransformContext.SEMICOLON;

/**
 * 默认的builder实现
 *
 * @author panguanjing
 * @date 2021/3/8
 */
@BuilderAnnotation(dialect = Constants.SPARK, values = {BaseStatement.class})
@AutoService(StatementBuilder.class)
public class DefaultBuilder implements StatementBuilder<SparkTransformContext> {

    @Override
    public DialectNode build(BaseStatement source, SparkTransformContext context) {
        SparkVisitor mysqlVisitor = new SparkVisitor(context);
        Boolean process = mysqlVisitor.process(source, 0);
        String result = mysqlVisitor.getBuilder().toString();
        if (BooleanUtils.isTrue(context.isAppendSemicolon()) && !StringUtils.endsWith(result, SEMICOLON)) {
            result = result + SEMICOLON;
        }
        return new DialectNode(result, process);
    }
}
