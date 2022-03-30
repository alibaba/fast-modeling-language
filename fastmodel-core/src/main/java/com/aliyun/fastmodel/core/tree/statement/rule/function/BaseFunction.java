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

package com.aliyun.fastmodel.core.tree.statement.rule.function;

import java.util.List;

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.NodeLocation;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import lombok.EqualsAndHashCode;

/**
 * 固定值函数
 *
 * @author panguanjing
 * @date 2021/5/29
 */
@EqualsAndHashCode(callSuper = true)
public abstract class BaseFunction extends BaseExpression {

    public BaseFunction() {
        this(null);
    }

    public BaseFunction(NodeLocation location) {
        super(location);
    }

    /**
     * 函数名
     *
     * @return
     */
    public abstract BaseFunctionName funcName();

    /**
     * 函数参数
     *
     * @return
     */
    public abstract List<BaseExpression> arguments();

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitBaseFunction(this, context);
    }
}
