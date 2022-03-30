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

package com.aliyun.fastmodel.core.formatter;

import java.util.List;

import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.statement.select.order.OrderBy;
import com.google.common.base.Joiner;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

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

    public static String formatStringLiteral(String s) {
        if (s == null) {
            return null;
        }
        String result = s.replace("'", "''");
        return "'" + result + "'";
    }

    public static String formatName(QualifiedName name) {
        return name.getOriginalParts().stream()
            .map(ExpressionFormatter::formatExpression)
            .collect(joining("."));
    }

    public static String formatGroupingSet(List<BaseExpression> groupingSet) {
        return format("(%s)", Joiner.on(", ").join(groupingSet.stream()
            .map(ExpressionFormatter::formatExpression)
            .iterator()));
    }

    public static String formatOrderBy(OrderBy orderBy) {
        return new ExpressionVisitor().formatOrderBy(orderBy);
    }
}
