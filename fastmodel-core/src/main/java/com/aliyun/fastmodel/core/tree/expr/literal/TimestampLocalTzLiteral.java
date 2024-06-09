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

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.NodeLocation;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * TimestampLocalTzLiteral
 *
 * @author panguanjing
 * @date 2020/9/25
 */
@Getter
@EqualsAndHashCode(callSuper = false)
public class TimestampLocalTzLiteral extends BaseLiteral {
    private final String timestamp;

    public TimestampLocalTzLiteral(String timestamp) {
        this(null, null, timestamp);
    }

    public TimestampLocalTzLiteral(NodeLocation location, String origin, String timestamp) {
        super(location, origin);
        this.timestamp = timestamp;
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        return visitor.visitTimestampLocalTzLiteral(this, context);
    }
}
