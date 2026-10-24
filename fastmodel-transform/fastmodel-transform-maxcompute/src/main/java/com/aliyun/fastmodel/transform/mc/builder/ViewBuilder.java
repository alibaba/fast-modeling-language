/*
 * Copyright 2022 Alibaba Group Holding Ltd.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.aliyun.fastmodel.transform.mc.builder;

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
import com.aliyun.fastmodel.transform.api.dialect.DialectName;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.mc.context.MaxComputeContext;
import com.aliyun.fastmodel.transform.mc.format.MaxComputeViewVisitor;
import com.google.auto.service.AutoService;

import static com.aliyun.fastmodel.transform.api.context.TransformContext.SEMICOLON;

/**
 * 支持创建的操作处理
 *
 * @author panguanjing
 * @date 2022/5/24
 */
@BuilderAnnotation(dialect = DialectName.Constants.MAXCOMPUTE,
    values = {CreateTable.class, CreateDimTable.class, CreateFactTable.class, CreateDwsTable.class, CreateAdsTable.class, DropTable.class})
@AutoService(StatementBuilder.class)
public class ViewBuilder implements StatementBuilder<TransformContext> {

    @Override
    public DialectNode build(BaseStatement source, TransformContext context) {
        MaxComputeContext maxComputeContext = new MaxComputeContext(context);
        MaxComputeViewVisitor viewVisitor = new MaxComputeViewVisitor(maxComputeContext);
        Boolean executable = source.accept(viewVisitor, 0);
        String node = viewVisitor.getBuilder().toString();
        if (context.isAppendSemicolon()) {
            node = node + SEMICOLON;
        }
        return new DialectNode(node, executable);
    }

    @Override
    public boolean isMatch(BaseStatement source, TransformContext context) {
        return context.getViewSetting() != null && context.getViewSetting().isTransformToView();
    }
}
