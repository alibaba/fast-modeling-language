/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.hologres.builder;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.statement.select.Query;
import com.aliyun.fastmodel.transform.api.builder.BuilderAnnotation;
import com.aliyun.fastmodel.transform.api.builder.StatementBuilder;
import com.aliyun.fastmodel.transform.api.dialect.DialectName;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.hologres.context.HologresTransformContext;
import com.aliyun.fastmodel.transform.hologres.format.HologresFormatter;
import com.aliyun.fastmodel.transform.hologres.parser.visitor.HologresRewriteVisitor;
import com.google.auto.service.AutoService;

/**
 * query builder 转换
 *
 * @author panguanjing
 * @date 2022/6/18
 */
@BuilderAnnotation(dialect = DialectName.Constants.HOLOGRES, values = {Query.class})
@AutoService(StatementBuilder.class)
public class QueryBuilder implements StatementBuilder<HologresTransformContext> {

    @Override
    public DialectNode build(BaseStatement source, HologresTransformContext context) {
        //将别名加上source
        HologresRewriteVisitor hologresQueryVisitor = new HologresRewriteVisitor(context);
        Node node = hologresQueryVisitor.process(source);
        return HologresFormatter.format((BaseStatement)node, context);
    }
}
