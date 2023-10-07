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

package com.aliyun.fastmodel.core.tree.expr.similar;

import java.util.List;

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.NodeLocation;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.google.common.collect.ImmutableList;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * @author panguanjing
 * @date 2020/9/23
 */
@Getter
@EqualsAndHashCode(callSuper = false)
public class BetweenPredicate extends BaseExpression {

    private final BaseExpression value;

    private final BaseExpression min;

    private final BaseExpression max;

    public BetweenPredicate(BaseExpression value, BaseExpression min,
                            BaseExpression max) {
        this(null, null, value, min, max);
    }

    public BetweenPredicate(NodeLocation location,
                            String origin,
                            BaseExpression value, BaseExpression min,
                            BaseExpression max) {
        super(location, origin);
        this.value = value;
        this.min = min;
        this.max = max;
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        return visitor.visitBetweenPredicate(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        ImmutableList.Builder<Node> builder = ImmutableList.builder();
        if (value != null) {
            builder.add(value);
        }
        if (min != null) {
            builder.add(min);
        }
        if (max != null) {
            builder.add(max);
        }
        return builder.build();
    }

}
