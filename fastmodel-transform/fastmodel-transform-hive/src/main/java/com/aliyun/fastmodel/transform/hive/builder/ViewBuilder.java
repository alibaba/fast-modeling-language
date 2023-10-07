/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.hive.builder;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.statement.table.CreateAdsTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateDimTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateDwsTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateFactTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.DropTable;
import com.aliyun.fastmodel.transform.api.builder.BuilderAnnotation;
import com.aliyun.fastmodel.transform.api.builder.StatementBuilder;
import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.aliyun.fastmodel.transform.api.dialect.DialectName.Constants;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.hive.context.HiveTransformContext;
import com.aliyun.fastmodel.transform.hive.format.HiveViewVisitor;
import com.google.auto.service.AutoService;
import org.apache.commons.lang3.StringUtils;

import static com.aliyun.fastmodel.transform.api.context.TransformContext.SEMICOLON;

/**
 * create view builder
 *
 * @author panguanjing
 * @date 2022/8/8
 */
@BuilderAnnotation(dialect = Constants.HIVE,
    values = {CreateTable.class, CreateDimTable.class, CreateFactTable.class,
        CreateDwsTable.class, CreateAdsTable.class, DropTable.class})
@AutoService(StatementBuilder.class)
public class ViewBuilder implements StatementBuilder<TransformContext> {

    @Override
    public DialectNode build(BaseStatement source, TransformContext context) {
        HiveTransformContext transformContext = new HiveTransformContext(context);
        HiveViewVisitor viewVisitor = new HiveViewVisitor(transformContext);
        Boolean executable = source.accept(viewVisitor, 0);
        String node = viewVisitor.getBuilder().toString();
        if (context.isAppendSemicolon() && StringUtils.isNotBlank(node)) {
            node = node + SEMICOLON;
        }
        return new DialectNode(node, executable);
    }

    @Override
    public boolean isMatch(BaseStatement source, TransformContext context) {
        return context.getViewSetting() != null && context.getViewSetting().isTransformToView();
    }
}
