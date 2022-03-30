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

import com.aliyun.fastmodel.core.tree.AbstractNode;
import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.NodeLocation;
import com.google.common.collect.ImmutableList;
import lombok.Getter;
import lombok.Setter;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2020/10/20
 */
@Getter
@Setter
public class GroupBy extends AbstractNode {

    private boolean distinct;

    private List<GroupingElement> groupingElements;

    public GroupBy() {
        this(null, false, null);
    }

    public GroupBy(boolean distinct,
                   List<GroupingElement> groupingElements) {
        this.distinct = distinct;
        this.groupingElements = groupingElements;
    }

    public GroupBy(NodeLocation location, boolean distinct,
                   List<GroupingElement> groupingElements) {
        super(location);
        this.distinct = distinct;
        this.groupingElements = groupingElements;
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.copyOf(groupingElements);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitGroupBy(this, context);
    }
}
