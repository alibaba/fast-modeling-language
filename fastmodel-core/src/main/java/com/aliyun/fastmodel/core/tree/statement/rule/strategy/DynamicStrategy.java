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
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.atom.FunctionCall;
import com.aliyun.fastmodel.core.tree.expr.literal.BaseLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.LongLiteral;
import com.aliyun.fastmodel.core.tree.statement.rule.RuleStrategy;
import com.aliyun.fastmodel.core.tree.statement.rule.function.BaseFunction;
import com.google.common.collect.ImmutableList;
import lombok.Getter;
import lombok.ToString;

/**
 * 动态阈值策略
 *
 * @author panguanjing
 * @date 2021/5/30
 */
@Getter
@ToString
public class DynamicStrategy extends RuleStrategy {

    public static final QualifiedName DYNMIAC = QualifiedName.of("DYNMIAC");
    /**
     * 采样固定值函数
     */
    private final BaseFunction baseFunction;

    /**
     * 动态阈值算法时间周期
     */
    private final BaseLiteral number;

    public DynamicStrategy(BaseFunction baseFunction, BaseLiteral number) {
        this.baseFunction = baseFunction;
        this.number = number;
    }

    public Integer getNumberValue() {
        if (number instanceof LongLiteral) {
            LongLiteral longLiteral = (LongLiteral)number;
            return longLiteral.getValue().intValue();
        }
        return 0;
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of(baseFunction);
    }

    @Override
    public BaseFunction getFunction() {
        return baseFunction;
    }

    @Override
    public BaseExpression toExpression() {
        return new FunctionCall(DYNMIAC, false, ImmutableList.of(baseFunction, number));
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDynamicStrategy(this, context);
    }
}
