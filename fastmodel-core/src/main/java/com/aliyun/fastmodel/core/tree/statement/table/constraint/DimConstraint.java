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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * 维度的约束的语句
 *
 * @author panguanjing
 * @date 2020/9/4
 */
@Getter
@EqualsAndHashCode(callSuper = false)
public class DimConstraint extends BaseConstraint {
    /**
     * 源表的列名
     */
    private final List<Identifier> colNames;

    /**
     * 关联的表
     */
    private final QualifiedName referenceTable;

    /**
     * 关联的列列表
     */
    private final List<Identifier> referenceColNames;

    public DimConstraint(Identifier constraintName, QualifiedName referenceTable) {
        this(constraintName, null, referenceTable, null);
    }

    public DimConstraint(Identifier constraintName, List<Identifier> colNames, QualifiedName referenceTable,
                         List<Identifier> referenceColNames) {
        super(constraintName, ConstraintType.DIM_KEY, true);
        Preconditions.checkNotNull(referenceTable, "referenceTable must not null");
        this.colNames = colNames;
        this.referenceTable = referenceTable;
        this.referenceColNames = referenceColNames;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDimConstraint(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        ImmutableList.Builder<Node> node = ImmutableList.builder();
        node.addAll(colNames);
        node.addAll(referenceColNames);
        return node.build();
    }

}
