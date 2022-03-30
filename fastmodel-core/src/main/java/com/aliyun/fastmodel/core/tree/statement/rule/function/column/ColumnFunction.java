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

package com.aliyun.fastmodel.core.tree.statement.rule.function.column;

import java.util.List;

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.atom.TableOrColumn;
import com.aliyun.fastmodel.core.tree.statement.rule.function.BaseFunction;
import com.aliyun.fastmodel.core.tree.statement.rule.function.BaseFunctionName;
import com.google.common.collect.ImmutableList;
import lombok.Getter;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/5/30
 */
@Getter
public class ColumnFunction extends BaseFunction {

    private final BaseFunctionName functionName;

    /**
     * 里的值
     */
    private final TableOrColumn column;

    /**
     * 列的类型
     */
    private final BaseDataType baseDataType;

    public ColumnFunction(BaseFunctionName functionName, TableOrColumn column,
                          BaseDataType baseDataType) {
        this.functionName = functionName;
        this.column = column;
        this.baseDataType = baseDataType;
    }

    @Override
    public BaseFunctionName funcName() {
        return functionName;
    }

    @Override
    public List<BaseExpression> arguments() {
        return ImmutableList.of(column);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitColumnFunction(this, context);
    }
}

