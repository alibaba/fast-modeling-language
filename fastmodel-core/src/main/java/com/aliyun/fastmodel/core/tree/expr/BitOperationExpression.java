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
import com.aliyun.fastmodel.core.tree.expr.enums.BitOperator;
import com.google.common.collect.ImmutableList;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * 位运算
 *
 * @author panguanjing
 * @date 2020/11/9
 */
@Getter
@EqualsAndHashCode(callSuper = false)
public class BitOperationExpression extends BaseExpression {
    private final BitOperator operator;

    private final BaseExpression left;

    private final BaseExpression right;

    public BitOperationExpression(BitOperator operator, BaseExpression left,
                                  BaseExpression right) {
        super(null, null);
        this.operator = operator;
        this.left = left;
        this.right = right;
    }

    public BitOperationExpression(NodeLocation location,
                                  String origin,
                                  BitOperator operator, BaseExpression left,
                                  BaseExpression right) {
        super(location, origin);
        this.operator = operator;
        this.left = left;
        this.right = right;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitBitOperationExpression(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of(left, right);
    }
}
