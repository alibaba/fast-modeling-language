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

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.NodeLocation;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
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
public class SimpleCaseExpression extends BaseExpression {
    /**
     * 如果caseExpr不为空，那么为case语句， 那么则为when语句
     */
    private final BaseExpression operand;
    /**
     * when then 语句
     */
    private final List<WhenClause> whenClauses;
    /**
     * else expr 可能为空
     */
    private final BaseExpression defaultValue;

    public SimpleCaseExpression(NodeLocation location, String origin,
                                BaseExpression operand,
                                List<WhenClause> whenClauses,
                                BaseExpression defaultValue) {
        super(location, origin);
        this.operand = operand;
        this.whenClauses = whenClauses;
        this.defaultValue = defaultValue;
    }

    public SimpleCaseExpression(BaseExpression operand,
                                List<WhenClause> whenClauses,
                                BaseExpression defaultValue) {
        this(null, null, operand, whenClauses, defaultValue);
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        return visitor.visitSimpleCaseExpression(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        ImmutableList.Builder<Node> nodes = ImmutableList.builder();
        nodes.add(operand);
        nodes.addAll(whenClauses);
        if (defaultValue != null) {
            nodes.add(defaultValue);
        }
        return nodes.build();
    }

}
