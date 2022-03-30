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

package com.aliyun.fastmodel.core.tree.expr.atom.datetime;

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.NodeLocation;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.atom.IntervalExpression;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import lombok.Getter;

/**
 * 日期时间操作处理
 * 近一天
 * CREATE TIMEPERIOD AS BETWEEN DATETIME_TRUNC(DATETIME_SUB($bizDate, INTERVAL 1 DAY), DAY) AND DATETIME_TRUNC($bizDate,
 * DAY)
 *
 * @author panguanjing
 * @date 2021/4/12
 */
@Getter
public class DateTimeAddExpression extends BaseDateTimeExpression {

    public DateTimeAddExpression(NodeLocation location, String origin,
                                 BaseExpression dateTimeExpression,
                                 IntervalExpression intervalExpression,
                                 StringLiteral startDate) {
        super(location, origin,
            dateTimeExpression, intervalExpression, startDate);
    }

    public DateTimeAddExpression(
        BaseExpression dateTimeExpression,
        IntervalExpression intervalExpression,
        StringLiteral startDate) {
        this(null, null, dateTimeExpression, intervalExpression, startDate);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDateTimeAddExpression(this, context);
    }
}
