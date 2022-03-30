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

package com.aliyun.fastmodel.core.tree.expr.atom;

import java.util.List;

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.NodeLocation;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.google.common.collect.ImmutableList;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

/**
 * When case
 *
 * @author panguanjing
 * @date 2020/11/2
 */
@Getter
@EqualsAndHashCode(callSuper = false)
public class WhenClause extends BaseExpression {
    private final BaseExpression operand;

    private final BaseExpression result;

    public WhenClause(BaseExpression operand, BaseExpression result) {
        this(null, null, operand, result);
    }

    public WhenClause(NodeLocation location,
                      String origin,
                      BaseExpression operand, BaseExpression result) {
        super(location, origin);
        this.operand = operand;
        this.result = result;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitWhenClause(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of(operand, result);
    }
}
