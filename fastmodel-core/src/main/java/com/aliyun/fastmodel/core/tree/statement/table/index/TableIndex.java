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

package com.aliyun.fastmodel.core.tree.statement.table.index;

import java.util.List;

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.constants.ConstraintType;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.google.common.collect.ImmutableList;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * table index
 *
 * @author panguanjing
 * @date 2021/8/30
 */
@Getter
@EqualsAndHashCode(callSuper = false)
public class TableIndex extends BaseConstraint {

    private final Identifier indexName;

    private final List<IndexColumnName> indexColumnNames;

    private final List<Property> properties;

    public TableIndex(Identifier indexName,
        List<IndexColumnName> indexColumnNames,
        List<Property> properties) {
        super(indexName, ConstraintType.INDEX);
        this.indexName = indexName;
        this.indexColumnNames = indexColumnNames;
        this.properties = properties;
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitTableIndex(this, context);
    }
}
