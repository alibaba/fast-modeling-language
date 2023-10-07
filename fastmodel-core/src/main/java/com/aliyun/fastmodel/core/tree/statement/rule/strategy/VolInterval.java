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

package com.aliyun.fastmodel.core.tree.statement.rule.strategy;

import java.util.List;

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.google.common.collect.ImmutableList;
import lombok.Getter;

/**
 * 定义波动率的区间，以中括号结束： [0.15, 0.20]
 *
 * @author panguanjing
 * @date 2021/5/29
 */
@Getter
public class VolInterval extends BaseExpression {

    private final Number start;

    private final Number end;

    public VolInterval(Number start, Number end) {
        super(null);
        this.start = start;
        this.end = end;
    }

    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitVolInterval(this, context);
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        return (R)accept((AstVisitor)visitor, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of();
    }
}
