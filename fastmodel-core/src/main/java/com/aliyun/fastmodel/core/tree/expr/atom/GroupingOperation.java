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
import java.util.stream.Collectors;

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.NodeLocation;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.DereferenceExpression;
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2020/11/24
 */
@Getter
@EqualsAndHashCode(callSuper = false)
public class GroupingOperation extends BaseExpression {
    private final List<BaseExpression> groupingColumns;

    public GroupingOperation(NodeLocation location, String origin,
                             List<QualifiedName> groupingColumns) {
        super(location, origin);
        Preconditions.checkNotNull(groupingColumns);
        this.groupingColumns = groupingColumns.stream().map(
            DereferenceExpression::from
        ).collect(Collectors.toList());
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        return visitor.visitGroupingOperation(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        return groupingColumns;
    }
}
