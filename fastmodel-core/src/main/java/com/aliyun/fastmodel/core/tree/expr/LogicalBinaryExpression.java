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

package com.aliyun.fastmodel.core.tree.expr;

import java.util.List;

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.NodeLocation;
import com.aliyun.fastmodel.core.tree.expr.enums.LogicalOperator;
import com.google.common.collect.ImmutableList;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

/**
 * 逻辑表达式
 *
 * @author panguanjing
 * @date 2020/11/4
 */
@Getter
@EqualsAndHashCode(callSuper = false)
public class LogicalBinaryExpression extends BaseExpression {

    private final LogicalOperator operator;

    private final BaseExpression left;

    private final BaseExpression right;

    public LogicalBinaryExpression(LogicalOperator operator, BaseExpression left,
                                   BaseExpression right) {
        super(null, null);
        this.operator = operator;
        this.left = left;
        this.right = right;
    }

    public LogicalBinaryExpression(NodeLocation location,
                                   String origin,
                                   LogicalOperator operator, BaseExpression left,
                                   BaseExpression right) {
        super(location, origin);
        this.operator = operator;
        this.left = left;
        this.right = right;
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of(left, right);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalBinaryExpression(this, context);
    }
}
