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

package com.aliyun.fastmodel.core.tree.statement.show;

import java.util.List;

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.NodeLocation;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.BaseQueryStatement;
import com.aliyun.fastmodel.core.tree.statement.constants.ShowObjectsType;
import com.aliyun.fastmodel.core.tree.statement.constants.StatementType;
import com.aliyun.fastmodel.core.tree.statement.select.Limit;
import com.aliyun.fastmodel.core.tree.statement.select.Offset;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * 基本的show语句的
 *
 * @author panguanjing
 * @date 2020/11/30
 */
@EqualsAndHashCode(callSuper = true)
@Getter
public class ShowObjects extends BaseQueryStatement {

    private final ConditionElement conditionElement;

    private final Boolean full;

    private final ShowObjectsType showType;

    private final QualifiedName tableName;

    private final Offset offset;

    private final Limit limit;

    public ShowObjects(ConditionElement conditionElement, Boolean full,
                       Identifier unit, ShowObjectsType showType) {
        this(null, null, conditionElement, full, unit, showType, null, null, null);
    }

    public ShowObjects(ConditionElement conditionElement,
                       Identifier unit, ShowObjectsType showType, QualifiedName tableName) {
        this(null, null, conditionElement, false, unit, showType, tableName, null, null);
    }

    public ShowObjects(NodeLocation nodeLocation, String origin, ConditionElement conditionElement, Boolean full,
                       Identifier unit, ShowObjectsType showType,
                       QualifiedName tableName, Offset offset, Limit limit) {
        super(nodeLocation, origin, unit);
        this.tableName = tableName;
        Preconditions.checkNotNull(showType, "show Type can't be null");
        this.conditionElement = conditionElement;
        this.full = full;
        this.showType = showType;
        this.offset = offset;
        this.limit = limit;
        setStatementType(StatementType.SHOW);
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowObjects(this, context);
    }
}
