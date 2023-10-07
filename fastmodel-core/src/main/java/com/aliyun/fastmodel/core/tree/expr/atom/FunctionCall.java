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
import java.util.Objects;

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.NodeLocation;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.enums.NullTreatment;
import com.aliyun.fastmodel.core.tree.expr.window.Window;
import com.aliyun.fastmodel.core.tree.statement.select.order.OrderBy;
import com.google.common.collect.ImmutableList;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * Function:
 *
 * @author panguanjing
 * @date 2020/9/23
 */
@Getter
@EqualsAndHashCode(callSuper = false)
public class FunctionCall extends BaseExpression {

    private final QualifiedName funcName;

    private final boolean distinct;

    private final List<BaseExpression> arguments;

    private final Window window;

    private final NullTreatment nullTreatment;

    private final BaseExpression filter;

    private final OrderBy orderBy;

    public FunctionCall(QualifiedName funcName,
                        boolean distinct,
                        List<BaseExpression> arguments, Window window,
                        NullTreatment nullTreatment, BaseExpression filter,
                        OrderBy orderBy) {
        this(null, null, funcName, distinct, arguments, window, nullTreatment, filter, orderBy);
    }

    public FunctionCall(NodeLocation location, String origin,
                        QualifiedName funcName, boolean distinct,
                        List<BaseExpression> arguments, Window window,
                        NullTreatment nullTreatment, BaseExpression filter,
                        OrderBy orderBy) {
        super(location, origin);
        this.funcName = Objects.requireNonNull(funcName);
        this.distinct = distinct;
        this.arguments = arguments;
        this.window = window;
        this.nullTreatment = nullTreatment;
        this.filter = filter;
        this.orderBy = orderBy;
    }

    public FunctionCall(QualifiedName qualifiedName, boolean distinct, List<BaseExpression> argument) {
        this(null, null, qualifiedName, distinct, argument, null, null, null, null);
    }

    public FunctionCall(NodeLocation location, String origin, QualifiedName qualifiedName, boolean distinct,
                        List<BaseExpression> argument) {
        this(location, origin, qualifiedName, distinct, argument, null, null, null, null);
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        return visitor.visitFunctionCall(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        ImmutableList.Builder<Node> nodes = ImmutableList.builder();
        if (arguments != null) {
            nodes.addAll(arguments);
        }
        if (filter != null) {
            nodes.add(filter);
        }
        return nodes.build();
    }

}
