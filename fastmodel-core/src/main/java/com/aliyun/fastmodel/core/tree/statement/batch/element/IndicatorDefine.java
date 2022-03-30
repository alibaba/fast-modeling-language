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

package com.aliyun.fastmodel.core.tree.statement.batch.element;

import java.util.List;

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.batch.AbstractBatchElement;
import com.google.common.collect.ImmutableList;
import lombok.Getter;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/2/4
 */
@Getter
public class IndicatorDefine extends AbstractBatchElement {

    /**
     * 派生指标code
     */
    private final Identifier code;

    /**
     * 名称
     */
    private final Comment comment;

    /**
     * 修饰词列表
     */
    private final List<Identifier> adjuncts;

    /**
     * 引用的原子指标
     */
    private final Identifier reference;

    /**
     * 表达式
     */
    private final BaseExpression baseExpression;

    public IndicatorDefine(Identifier code, Comment comment,
                           List<Identifier> adjuncts, Identifier reference,
                           BaseExpression baseExpression) {
        this.code = code;
        this.comment = comment;
        this.adjuncts = adjuncts;
        this.reference = reference;
        this.baseExpression = baseExpression;
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitIndicatorDefine(this, context);
    }
}
