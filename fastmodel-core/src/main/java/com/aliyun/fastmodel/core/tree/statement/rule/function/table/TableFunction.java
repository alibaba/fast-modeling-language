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

package com.aliyun.fastmodel.core.tree.statement.rule.function.table;

import java.util.List;

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.statement.rule.function.BaseFunction;
import com.aliyun.fastmodel.core.tree.statement.rule.function.BaseFunctionName;
import com.google.common.base.Preconditions;

/**
 * TableFunction
 *
 * @author panguanjing
 * @date 2021/5/30
 */
public class TableFunction extends BaseFunction {

    private final BaseFunctionName baseFunctionName;

    private final List<BaseExpression> arguments;

    public TableFunction(BaseFunctionName baseFunctionName,
                         List<BaseExpression> arguments) {
        this.baseFunctionName = baseFunctionName;
        this.arguments = arguments;
        Preconditions.checkNotNull(baseFunctionName, "function Name can't be null");
        Preconditions.checkArgument(baseFunctionName.isTableFunction(), "function must be table function");
    }

    @Override
    public BaseFunctionName funcName() {
        return baseFunctionName;
    }

    @Override
    public List<BaseExpression> arguments() {
        return arguments;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitTableFunction(this, context);
    }
}
