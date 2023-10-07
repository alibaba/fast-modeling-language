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

package com.aliyun.fastmodel.core.tree.statement.select;

import java.util.List;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.NodeLocation;
import com.aliyun.fastmodel.core.tree.relation.querybody.BaseQueryBody;
import com.aliyun.fastmodel.core.tree.statement.constants.StatementType;
import com.aliyun.fastmodel.core.tree.statement.select.order.OrderBy;
import com.google.common.collect.ImmutableList;
import lombok.Data;
import lombok.EqualsAndHashCode;
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
@EqualsAndHashCode(callSuper = false)
public class Query extends BaseStatement {

    /**
     * with
     */
    private With with;

    /**
     * query object
     */
    private BaseQueryBody queryBody;

    /**
     * order by statement
     */
    private OrderBy orderBy;

    /**
     * offset内容
     */
    private Offset offset;

    /**
     * Limit Statement
     */
    private Node limit;

    /**
     * 语句类型
     */
    private StatementType statementType;

    public Query(With with, BaseQueryBody queryBody,
                 OrderBy orderBy,
                 Offset offset, Node limit) {
        this(null, null, with, queryBody, orderBy, offset, limit);
    }

    public Query(NodeLocation nodeLocation, String origin, With with, BaseQueryBody queryBody,
                 OrderBy orderBy,
                 Offset offset, Node limit) {
        super(nodeLocation, origin);
        this.with = with;
        this.queryBody = queryBody;
        this.orderBy = orderBy;
        this.offset = offset;
        this.limit = limit;
        this.setStatementType(StatementType.SELECT);
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        return visitor.visitQuery(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        ImmutableList.Builder<Node> nodeBuilder = ImmutableList.builder();
        if (with != null) {
            nodeBuilder.add(with);
        }
        if (queryBody != null) {
            nodeBuilder.add(queryBody);
        }
        if (orderBy != null) {
            nodeBuilder.add(orderBy);
        }
        if (offset != null) {
            nodeBuilder.add(offset);
        }
        if (limit != null) {
            nodeBuilder.add(limit);
        }
        return nodeBuilder.build();
    }
}

