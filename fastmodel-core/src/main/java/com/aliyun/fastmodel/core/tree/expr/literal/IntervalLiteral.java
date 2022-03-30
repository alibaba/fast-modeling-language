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

package com.aliyun.fastmodel.core.tree.expr.literal;

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.NodeLocation;
import com.aliyun.fastmodel.core.tree.expr.enums.DateTimeEnum;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2020/9/23
 */
@Getter
@EqualsAndHashCode(callSuper = false)
public class IntervalLiteral extends BaseLiteral {

    private final BaseLiteral value;

    private final DateTimeEnum fromDateTime;

    private final DateTimeEnum toDateTime;

    public IntervalLiteral(BaseLiteral value, DateTimeEnum fromDateTime) {
        this(null, null, value, fromDateTime, null);
    }

    public IntervalLiteral(NodeLocation location, String origin,
                           BaseLiteral value, DateTimeEnum fromDateTime,
                           DateTimeEnum toDateTime) {
        super(location, origin);
        this.value = value;
        this.fromDateTime = fromDateTime;
        this.toDateTime = toDateTime;
    }

    public IntervalLiteral(BaseLiteral value, DateTimeEnum fromDateTime,
                           DateTimeEnum toDateTime) {
        this(null, null, value, fromDateTime, toDateTime);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitIntervalLiteral(this, context);
    }

}
