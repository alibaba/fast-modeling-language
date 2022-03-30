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

package com.aliyun.fastmodel.core.tree.statement.select.order;

import java.util.List;

import com.aliyun.fastmodel.core.tree.AbstractNode;
import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.NodeLocation;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.google.common.collect.ImmutableList;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * OrderRefColumn*
 *
 * @author panguanjing
 * @date 2020/10/21
 */
@ToString
@Getter
@EqualsAndHashCode(callSuper = false)
public class SortItem extends AbstractNode {

    /**
     * order by Spec
     */
    private final Ordering ordering;

    /**
     * 表达式
     */
    private final BaseExpression sortKey;

    /**
     * null Ordering
     */
    private final NullOrdering nullOrdering;

    public SortItem(NodeLocation location,
                    BaseExpression sortKey, Ordering ordering,
                    NullOrdering nullOrdering) {
        super(location);
        this.ordering = ordering;
        this.sortKey = sortKey;
        this.nullOrdering = nullOrdering;
    }

    public SortItem(BaseExpression sortKey, Ordering ordering,
                    NullOrdering nullOrdering) {
        this.ordering = ordering;
        this.sortKey = sortKey;
        this.nullOrdering = nullOrdering;
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of(sortKey);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitSortItem(this, context);
    }
}

