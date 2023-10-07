/*
 * Copyright 2021-2022 Alibaba Group Holding Ltd.
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

package com.aliyun.fastmodel.core.tree.util;

import java.util.List;

import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.LogicalBinaryExpression;
import com.aliyun.fastmodel.core.tree.expr.enums.LogicalOperator;
import com.google.common.collect.ImmutableList;

/**
 * ExpressionUtils
 *
 * @author panguanjing
 * @date 2021/3/18
 */
public final class ExpressionUtil {

    private ExpressionUtil() {

    }

    public static List<BaseExpression> extractPredicates(LogicalBinaryExpression expression) {
        return extractPredicates(expression.getOperator(), expression);
    }

    public static List<BaseExpression> extractPredicates(LogicalOperator operator, BaseExpression expression) {
        ImmutableList.Builder<BaseExpression> resultBuilder = ImmutableList.builder();
        extractPredicates(operator, expression, resultBuilder);
        return resultBuilder.build();
    }

    private static void extractPredicates(LogicalOperator operator, BaseExpression expression,
                                          ImmutableList.Builder<BaseExpression> resultBuilder) {
        if (expression instanceof LogicalBinaryExpression
            && ((LogicalBinaryExpression)expression).getOperator() == operator) {
            LogicalBinaryExpression logicalBinaryExpression = (LogicalBinaryExpression)expression;
            extractPredicates(operator, logicalBinaryExpression.getLeft(), resultBuilder);
            extractPredicates(operator, logicalBinaryExpression.getRight(), resultBuilder);
        } else {
            resultBuilder.add(expression);
        }
    }
}
