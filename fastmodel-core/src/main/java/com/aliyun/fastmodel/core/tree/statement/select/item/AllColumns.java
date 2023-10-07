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

package com.aliyun.fastmodel.core.tree.statement.select.item;

import java.util.List;

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.NodeLocation;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2020/11/3
 */
@Getter
@EqualsAndHashCode(callSuper = false)
public class AllColumns extends SelectItem {

    private final List<Identifier> aliases;

    private final BaseExpression target;

    private final boolean existAs;

    public AllColumns(BaseExpression target) {
        this(target, null);
    }

    public AllColumns(BaseExpression target, List<Identifier> aliases) {
        this(null, target, aliases, false);
    }

    public AllColumns(NodeLocation location, BaseExpression target, List<Identifier> aliases, boolean existAs) {
        super(location);
        this.aliases = aliases;
        this.target = target;
        this.existAs = existAs;
    }

    @Override
    public List<? extends Node> getChildren() {
        ImmutableList.Builder<Node> builder = new Builder<>();
        if (target != null) {
            builder.add(target);
        }
        builder.addAll(aliases);
        return builder.build();
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        return visitor.visitAllColumns(this, context);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();

        if (target != null) {
            builder.append(target).append(".");
        }
        builder.append("*");

        if (aliases != null && !aliases.isEmpty()) {
            String separator = existAs ? " AS " : " ";
            builder.append(separator);
            builder.append("(");
            Joiner.on(", ").appendTo(builder, aliases);
            builder.append(")");
        }

        return builder.toString();
    }

}
