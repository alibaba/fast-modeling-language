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

package com.aliyun.fastmodel.core.tree.statement.layer;

import java.util.List;

import com.aliyun.fastmodel.core.tree.AbstractNode;
import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.NodeLocation;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.google.common.collect.ImmutableList;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2020/12/1
 */
@Getter
@EqualsAndHashCode(callSuper = false)
public class Checker extends AbstractNode {

    /**
     * 检查器类型
     */
    private final CheckerType checkerType;

    /**
     * 表达式
     */
    private final StringLiteral expression;

    /**
     * 检查器名称
     */
    private final Identifier checkerName;

    /**
     * 检查器的备注
     */
    private final Comment comment;

    private final Boolean enable;

    public Checker(CheckerType checkerType, StringLiteral expression,
                   Identifier checkerName, Comment comment, Boolean enable) {
        this(null, checkerType, expression, checkerName, comment, enable);
    }

    public Checker(NodeLocation location, CheckerType checkerType,
                   StringLiteral expression, Identifier checkerName,
                   Comment comment, Boolean enable) {
        super(location);
        this.checkerType = checkerType;
        this.expression = expression;
        this.checkerName = checkerName;
        this.comment = comment;
        this.enable = enable;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitChecker(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of();
    }
}
