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

package com.aliyun.fastmodel.core.tree.relation.querybody;

import java.util.List;

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.NodeLocation;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.BaseRelation;
import com.aliyun.fastmodel.core.tree.statement.select.Hint;
import com.aliyun.fastmodel.core.tree.statement.select.Offset;
import com.aliyun.fastmodel.core.tree.statement.select.order.OrderBy;
import com.aliyun.fastmodel.core.tree.statement.select.Select;
import com.aliyun.fastmodel.core.tree.statement.select.groupby.GroupBy;
import com.google.common.collect.ImmutableList;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2020/10/30
 */
@Getter
@Setter
@Builder
@EqualsAndHashCode(callSuper = false)
@ToString
public class QuerySpecification extends BaseQueryBody {

    private Select select;

    private List<Hint> hints;

    private BaseRelation from;

    private BaseExpression where;

    private GroupBy groupBy;

    private BaseExpression having;

    private OrderBy orderBy;

    private Offset offset;

    private Node limit;

    public QuerySpecification(Select select, BaseRelation from) {
        this(null, select, null, from, null, null, null, null, null, null);
    }

    public QuerySpecification(NodeLocation location,
                              Select select, List<Hint> hints, BaseRelation from, BaseExpression where,
                              GroupBy groupBy, BaseExpression having,
                              OrderBy orderBy, Offset offset, Node limit) {
        super(location);
        this.select = select;
        this.hints = hints;
        this.from = from;
        this.where = where;
        this.groupBy = groupBy;
        this.having = having;
        this.orderBy = orderBy;
        this.offset = offset;
        this.limit = limit;
    }

    public QuerySpecification(Select select, List<Hint> hints, BaseRelation from, BaseExpression where,
                              GroupBy groupBy, BaseExpression having,
                              OrderBy orderBy, Offset offset,
                              Node limit) {
        this(null, select, hints, from, where, groupBy, having, orderBy, offset, limit);
    }

    @Override
    public List<? extends Node> getChildren() {
        ImmutableList.Builder<Node> nodes = ImmutableList.builder();
        nodes.add(select);
        if (hints != null && !hints.isEmpty()) {
            nodes.addAll(hints);
        }
        if (from != null) {
            nodes.add(from);
        }
        if (where != null) {
            nodes.add(where);
        }
        if (groupBy != null) {
            nodes.add(groupBy);
        }
        if (having != null) {
            nodes.add(having);
        }
        if (orderBy != null) {
            nodes.add(orderBy);
        }
        if (offset != null) {
            nodes.add(offset);
        }
        if (limit != null) {
            nodes.add(limit);
        }
        return nodes.build();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitQuerySpecification(this, context);
    }
}
