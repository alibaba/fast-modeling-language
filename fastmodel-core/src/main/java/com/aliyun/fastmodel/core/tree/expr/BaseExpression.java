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

package com.aliyun.fastmodel.core.tree.expr;

import java.util.List;

import com.aliyun.fastmodel.core.formatter.ExpressionFormatter;
import com.aliyun.fastmodel.core.tree.AbstractNode;
import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.NodeLocation;
import com.google.common.collect.ImmutableList;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;

/**
 * 表达式对象:
 *
 * @author panguanjing
 * @date 2020/9/23
 */
@Getter
@Setter
public abstract class BaseExpression extends AbstractNode {

    /**
     * 原始的文本表达式
     */
    private String origin;

    /**
     * 是否被括号包围
     */
    private boolean parenthesized;

    public BaseExpression(NodeLocation location) {
        super(location);
    }

    public BaseExpression(NodeLocation location, String origin) {
        super(location);
        this.origin = origin;
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of();
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        return visitor.visitExpression(this, context);
    }

    /**
     * 优先从origin里获取，如果没有，那么读取toString内容
     *
     * @return
     */
    public String getOrigin() {
        if (StringUtils.isNotBlank(origin)) {
            return origin;
        }
        return toString();
    }

    @Override
    public String toString() {
        return ExpressionFormatter.formatExpression(this);
    }
}
