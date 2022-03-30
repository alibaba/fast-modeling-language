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
import com.aliyun.fastmodel.core.tree.expr.enums.IsType;
import com.google.common.collect.ImmutableList;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * express is condition
 *
 * @author panguanjing
 * @date 2020/9/23
 */
@Getter
@EqualsAndHashCode(callSuper = false)
public class IsConditionExpression extends BaseExpression {
    private final BaseExpression expression;

    private final IsType isType;

    public IsConditionExpression(BaseExpression expression,
                                 IsType isType) {
        super(null, null);
        this.expression = expression;
        this.isType = isType;
    }

    public IsConditionExpression(NodeLocation location, String origin,
                                 BaseExpression expression,
                                 IsType isType) {
        super(location, origin);
        this.expression = expression;
        this.isType = isType;
    }

    @Override
    public List<? extends Node> getChildren() {
        if (expression != null) {
            return ImmutableList.of(expression);
        }
        return ImmutableList.of();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitIsConditionExpression(this, context);
    }
}
