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

import java.util.List;

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.ComparisonExpression;
import com.aliyun.fastmodel.core.tree.expr.enums.ComparisonOperator;
import com.aliyun.fastmodel.core.tree.expr.literal.BaseLiteral;
import com.aliyun.fastmodel.core.tree.statement.rule.RuleStrategy;
import com.aliyun.fastmodel.core.tree.statement.rule.function.BaseFunction;
import com.google.common.collect.ImmutableList;
import lombok.Getter;
import lombok.ToString;

/**
 * 固定值策略
 *
 * @author panguanjing
 * @date 2021/5/29
 */
@Getter
@ToString
public class FixedStrategy extends RuleStrategy {

    private final ComparisonOperator comparisonOperator;

    private final BaseLiteral expectValue;

    private final BaseFunction baseFunction;

    public FixedStrategy(BaseFunction baseFunction, ComparisonOperator comparisonOperator,
                         BaseLiteral expectValue) {
        this.comparisonOperator = comparisonOperator;
        this.expectValue = expectValue;
        this.baseFunction = baseFunction;
    }

    @Override
    public BaseFunction getFunction() {
        return baseFunction;
    }

    @Override
    public BaseExpression toExpression() {
        return new ComparisonExpression(
            comparisonOperator,
            baseFunction,
            expectValue
        );
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitFixedStrategy(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of(baseFunction);
    }
}
