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

package com.aliyun.fastmodel.transform.mc.format;

import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.transform.api.format.BaseSampleDataProvider;
import com.aliyun.fastmodel.transform.api.format.BaseViewVisitor;
import com.aliyun.fastmodel.transform.mc.context.MaxComputeContext;
import com.aliyun.fastmodel.transform.mc.util.MaxComputeSampleUtil;

/**
 * 针对create view的visitor
 *
 * @author panguanjing
 * @date 2022/5/25
 */
public class MaxComputeViewVisitor extends BaseViewVisitor<MaxComputeContext> {

    private final MaxComputeSampleUtil maxComputeSampleUtil = new MaxComputeSampleUtil();

    public MaxComputeViewVisitor(MaxComputeContext maxComputeContext) {
        super(maxComputeContext);
    }

    @Override
    protected BaseSampleDataProvider getProvider() {
        return maxComputeSampleUtil;
    }
}
