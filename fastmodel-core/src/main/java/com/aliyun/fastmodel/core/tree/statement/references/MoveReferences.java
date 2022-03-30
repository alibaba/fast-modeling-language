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

package com.aliyun.fastmodel.core.tree.statement.references;

import java.util.List;

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.statement.BaseOperatorStatement;
import com.aliyun.fastmodel.core.tree.statement.constants.ShowType;
import com.aliyun.fastmodel.core.tree.statement.constants.StatementType;
import com.google.common.collect.ImmutableList;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * Move references
 *
 * @author panguanjing
 * @date 2022/2/14
 */
@Getter
@EqualsAndHashCode(callSuper = true)
public class MoveReferences extends BaseOperatorStatement {

    private final ShowType showType;

    private final QualifiedName from;

    private final QualifiedName to;

    private final List<Property> properties;

    public MoveReferences(ShowType showType, QualifiedName from, QualifiedName to, List<Property> properties) {
        super(from);
        this.showType = showType;
        this.from = from;
        this.to = to;
        this.properties = properties;
        this.setStatementType(StatementType.REFERENCES);
    }

    public MoveReferences(ShowType showType, QualifiedName from, QualifiedName to) {
        this(showType, from, to, null);
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitMoveReferences(this, context);
    }
}
