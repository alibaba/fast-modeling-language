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

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.NodeLocation;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.enums.LikeCondition;
import com.aliyun.fastmodel.core.tree.expr.enums.LikeOperator;
import com.google.common.collect.ImmutableList;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2020/9/23
 */
@Getter
@EqualsAndHashCode(callSuper = false)
public class LikePredicate extends BaseExpression {

    private final BaseExpression left;

    private final LikeCondition condition;

    private final BaseExpression target;

    private final LikeOperator operator;

    public LikePredicate(BaseExpression left, LikeCondition condition,
                         LikeOperator operator,
                         BaseExpression target) {
        this(null, null, left, operator, condition, target);
    }

    public LikePredicate(NodeLocation location, String origin,
                         BaseExpression left,
                         LikeOperator operator,
                         LikeCondition condition,
                         BaseExpression target) {
        super(location, origin);
        this.left = left;
        this.operator = operator;
        this.condition = condition;
        this.target = target;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitLikePredicate(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        ImmutableList.Builder<Node> builder = ImmutableList.builder();
        builder.add(left);
        builder.add(target);
        return builder.build();
    }
}
