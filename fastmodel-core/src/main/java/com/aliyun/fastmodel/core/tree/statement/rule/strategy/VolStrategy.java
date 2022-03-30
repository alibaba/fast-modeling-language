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

package com.aliyun.fastmodel.core.tree.statement.rule.strategy;

import java.util.ArrayList;
import java.util.List;

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.expr.ArithmeticBinaryExpression;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.statement.rule.RuleStrategy;
import com.aliyun.fastmodel.core.tree.statement.rule.function.BaseFunction;
import com.aliyun.fastmodel.core.tree.statement.rule.function.VolFunction;
import lombok.Getter;
import lombok.ToString;

/**
 * 波动率策略
 *
 * @author panguanjing
 * @date 2021/5/29
 */
@Getter
@ToString
public class VolStrategy extends RuleStrategy {
    private final VolFunction volFunction;

    private final VolOperator volOperator;

    private final VolInterval volInterval;

    public VolStrategy(VolFunction volFunction,
                       VolOperator volOperator,
                       VolInterval volInterval) {
        this.volFunction = volFunction;
        this.volOperator = volOperator;
        this.volInterval = volInterval;
    }

    @Override
    public List<? extends Node> getChildren() {
        List<Node> list = new ArrayList<>();
        list.add(volFunction);
        list.add(volInterval);
        return list;
    }

    @Override
    public BaseExpression toExpression() {
        return new ArithmeticBinaryExpression(
            volOperator.getArithmeticOperator(),
            volFunction,
            volInterval
        );
    }

    @Override
    public BaseFunction getFunction() {
        return volFunction;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitVolStrategy(this, context);
    }
}
