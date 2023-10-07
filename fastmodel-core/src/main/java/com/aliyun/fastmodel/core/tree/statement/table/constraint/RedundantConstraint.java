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

package com.aliyun.fastmodel.core.tree.statement.table.constraint;

import java.util.List;

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.constants.ConstraintType;
import com.google.common.collect.ImmutableList;
import lombok.Getter;

/**
 * 冗余字段约束
 *
 * @author panguanjing
 * @date 2021/8/9
 */
@Getter
public class RedundantConstraint extends BaseConstraint {

    private final Identifier column;

    private final QualifiedName joinColumn;

    private final List<Identifier> redundantColumns;

    public RedundantConstraint(Identifier constraintName,
                               Identifier column,
                               QualifiedName joinColumn,
                               List<Identifier> redundantColumns) {
        super(constraintName, ConstraintType.REDUNDANT, true);
        this.column = column;
        this.joinColumn = joinColumn;
        this.redundantColumns = redundantColumns;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitRedundantConstraint(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        ImmutableList.Builder<Node> builder = ImmutableList.builder();
        if (column != null) {
            builder.add(column);
        }
        if (joinColumn != null) {
            builder.add(joinColumn);
        }
        if (redundantColumns != null) {
            builder.addAll(redundantColumns);
        }
        return builder.build();
    }
}
