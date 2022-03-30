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

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.NodeLocation;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.google.common.collect.ImmutableList;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

/**
 * 没有case只有when
 *
 * @author panguanjing
 * @date 2020/11/4
 */
@Getter
@EqualsAndHashCode(callSuper = false)
public class SearchedCaseExpression extends BaseExpression {

    private final List<WhenClause> whenClauseList;

    private final BaseExpression defaultValue;

    public SearchedCaseExpression(NodeLocation location, String origin,
                                  List<WhenClause> whenClauseList,
                                  BaseExpression defaultValue) {
        super(location, origin);
        this.whenClauseList = whenClauseList;
        this.defaultValue = defaultValue;
    }

    public SearchedCaseExpression(List<WhenClause> whenClauseList,
                                  BaseExpression defaultValue) {
        super(null, null);
        this.whenClauseList = whenClauseList;
        this.defaultValue = defaultValue;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitSearchedCaseExpression(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        ImmutableList.Builder<Node> nodes = ImmutableList.builder();
        nodes.addAll(whenClauseList);
        if (defaultValue != null) {
            nodes.add(defaultValue);
        }
        return nodes.build();
    }
}
