/*
 * Copyright (c)  2021. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.mc.format;

import com.aliyun.fastmodel.core.tree.expr.literal.CurrentDate;
import com.aliyun.fastmodel.core.tree.expr.literal.CurrentTimestamp;
import com.aliyun.fastmodel.transform.api.format.DefaultExpressionVisitor;

/**
 * MaxComputeExpressionVisitor
 *
 * @author panguanjing
 * @date 2021/4/13
 */
public class MaxComputeExpressionVisitor extends DefaultExpressionVisitor {

    @Override
    public String visitCurrentTimestamp(CurrentTimestamp currentTimestamp, Void context) {
        return "GETDATE()";
    }

    @Override
    public String visitCurrentDate(CurrentDate currentDate, Void context) {
        return "GETDATE()";
    }

}
