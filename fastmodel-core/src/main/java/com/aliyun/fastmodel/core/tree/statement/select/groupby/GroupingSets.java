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

package com.aliyun.fastmodel.core.tree.statement.select.groupby;

import java.util.List;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.google.common.collect.ImmutableList;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2020/11/10
 */
@Getter
@Setter
@EqualsAndHashCode(callSuper = false)
public class GroupingSets extends GroupingElement {

    private final List<List<BaseExpression>> sets;

    public GroupingSets(List<List<BaseExpression>> sets) {this.sets = sets;}

    @Override
    public List<BaseExpression> getExpressions() {
        return sets.stream().flatMap(List::stream).collect(Collectors.toList());
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of();
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        return visitor.visitGroupingSets(this, context);
    }
}
