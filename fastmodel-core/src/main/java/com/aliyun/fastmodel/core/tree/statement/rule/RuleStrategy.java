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

package com.aliyun.fastmodel.core.tree.statement.rule;

import com.aliyun.fastmodel.core.tree.AbstractNode;
import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.statement.rule.function.BaseFunction;

/**
 * 规则策略
 *
 * @author panguanjing
 * @date 2021/5/29
 */
public abstract class RuleStrategy extends AbstractNode {

    /**
     * 返回策略中函数方式
     *
     * @return
     */
    public abstract BaseFunction getFunction();

    /**
     * 转换为表达式对象
     *
     * @return
     */
    public abstract BaseExpression toExpression();

    /**
     * accept
     *
     * @param visitor
     * @param context
     * @param <R>
     * @param <C>
     * @return
     */
    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitRuleStrategy(this, context);
    }

}
