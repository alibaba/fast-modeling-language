/*
 * Copyright (c)  2020. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.core.formatter;

import com.aliyun.fastmodel.core.tree.expr.BaseExpression;

/**
 * 格式化处理
 *
 * @author panguanjing
 * @date 2020/10/30
 */
public class ExpressionFormatter {

    private ExpressionFormatter() {}

    public static String formatExpression(BaseExpression baseExpression) {
        return new ExpressionVisitor().process(baseExpression, null);
    }

}
