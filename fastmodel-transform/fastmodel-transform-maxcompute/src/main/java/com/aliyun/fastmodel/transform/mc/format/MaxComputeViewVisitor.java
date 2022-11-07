/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
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
