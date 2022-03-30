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

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.BaseRelation;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.NodeLocation;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.google.common.collect.ImmutableList;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * 别名的关系
 *
 * @author panguanjing
 * @date 2020/11/3
 */
@Getter
@Builder
@EqualsAndHashCode(callSuper = false)
@ToString
public class AliasedRelation extends BaseRelation {

    private final BaseRelation relation;

    private final Identifier alias;

    private final List<Identifier> columnNames;

    public AliasedRelation(BaseRelation relation, Identifier alias) {
        this(null, relation, alias, null);
    }

    public AliasedRelation(BaseRelation relation, Identifier alias,
                           List<Identifier> columnNames) {
        this(null, relation, alias, columnNames);
    }

    public AliasedRelation(NodeLocation location,
                           BaseRelation relation, Identifier alias,
                           List<Identifier> columnNames) {
        super(location);
        this.relation = relation;
        this.alias = alias;
        this.columnNames = columnNames;
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of(relation);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAliasedRelation(this, context);
    }
}
