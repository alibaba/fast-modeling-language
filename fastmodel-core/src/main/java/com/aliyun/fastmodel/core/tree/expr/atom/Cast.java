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
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.google.common.collect.ImmutableList;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * @author panguanjing
 * @date 2020/9/23
 */
@Getter
@EqualsAndHashCode(callSuper = false)
public class Cast extends BaseExpression {
    /**
     * 原表达式
     */
    private final BaseExpression expression;

    /**
     * 需要转的类型
     */
    private final BaseDataType dataType;

    public Cast(NodeLocation location, String origin,
                BaseExpression expression, BaseDataType dataType) {
        super(location, origin);
        this.expression = expression;
        this.dataType = dataType;
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        return visitor.visitCast(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of(expression, dataType);
    }

}
