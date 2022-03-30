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
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.constants.ConstraintType;
import com.google.common.collect.ImmutableList;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * 主键的约束语句
 *
 * @author panguanjing
 * @date 2020/9/4
 */
@Getter
@EqualsAndHashCode(callSuper = false)
public class PrimaryConstraint extends BaseConstraint {

    /**
     * 对应的列的列表
     */
    private final List<Identifier> colNames;

    public PrimaryConstraint(Identifier constraintName,
                             List<Identifier> colNames) {
        this(constraintName, colNames, true);
    }

    public PrimaryConstraint(Identifier constraintName,
                             List<Identifier> colNames,
                             Boolean enable) {
        super(constraintName, ConstraintType.PRIMARY_KEY, enable);
        this.colNames = colNames;
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.copyOf(colNames);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitPrimaryConstraint(this, context);
    }
}
