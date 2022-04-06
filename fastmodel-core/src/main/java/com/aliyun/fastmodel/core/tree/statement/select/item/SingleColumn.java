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

package com.aliyun.fastmodel.core.tree.statement.select.item;

import java.util.List;

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.NodeLocation;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.google.common.collect.ImmutableList;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2020/11/3
 */
@Getter
@EqualsAndHashCode(callSuper = false)
public class SingleColumn extends SelectItem {

    private final BaseExpression expression;

    private final Identifier alias;

    private final boolean existAs;

    public SingleColumn(BaseExpression expression) {
        this(null, expression);
    }

    public SingleColumn(BaseExpression expression, Identifier alias) {
        this(null, expression, alias, false);
    }

    public SingleColumn(NodeLocation location, BaseExpression expression) {
        this(location, expression, null, false);
    }

    public SingleColumn(NodeLocation location, Identifier alias) {
        this(location, null, alias, false);
    }

    public SingleColumn(NodeLocation location, BaseExpression expression, Identifier alias, boolean existAs) {
        super(location);
        this.alias = alias;
        this.expression = expression;
        this.existAs = existAs;
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of(expression);
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        return visitor.visitSingleColumn(this, context);
    }

    @Override
    public String toString() {
        if (alias != null) {
            String separator = existAs ? " AS " : " ";
            return expression.toString() + separator + alias;
        }
        return expression.toString();
    }
}

