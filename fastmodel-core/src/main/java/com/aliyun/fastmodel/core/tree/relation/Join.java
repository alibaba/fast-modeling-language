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

package com.aliyun.fastmodel.core.tree.relation;

import java.util.List;

import com.aliyun.fastmodel.core.tree.AbstractNode;
import com.aliyun.fastmodel.core.tree.BaseRelation;
import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.NodeLocation;
import com.aliyun.fastmodel.core.tree.relation.join.JoinCriteria;
import com.aliyun.fastmodel.core.tree.relation.join.JoinToken;
import com.google.common.collect.ImmutableList;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * join statement
 *
 * @author panguanjing
 * @date 2020/10/20
 */
@Getter
@ToString
@EqualsAndHashCode(callSuper = false)
public class Join extends BaseRelation {

    /**
     * join statement
     */
    private final JoinCriteria criteria;

    /**
     * join的类型，包括常用的left，right等等
     */
    private final JoinToken joinToken;

    /**
     * 左边的表达式
     */
    private final BaseRelation left;

    /**
     * 右边的表达式
     */
    private final BaseRelation right;

    public Join(NodeLocation location,
                JoinToken joinToken, BaseRelation left, BaseRelation right, JoinCriteria criteria) {
        super(location);
        this.criteria = criteria;
        this.joinToken = joinToken;
        this.left = left;
        this.right = right;
    }

    public Join(JoinToken joinToken, BaseRelation left, BaseRelation right, JoinCriteria criteria) {
        this(null, joinToken, left, right, criteria);
    }

    @Override
    public List<? extends Node> getChildren() {
        ImmutableList.Builder<Node> nodeBuilder = ImmutableList.builder();
        nodeBuilder.add(left);
        nodeBuilder.add(right);
        if (criteria != null) {
            List<AbstractNode> nodes = criteria.getNodes();
            nodeBuilder.addAll(nodes);
        }
        return nodeBuilder.build();
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        return visitor.visitJoin(this, context);
    }
}
