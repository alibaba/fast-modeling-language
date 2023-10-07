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
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.BaseQueryStatement;
import com.aliyun.fastmodel.core.tree.statement.constants.ShowType;
import com.aliyun.fastmodel.core.tree.statement.constants.StatementType;
import com.google.common.collect.ImmutableList;
import lombok.Getter;

/**
 * show <support_type> references from <code> with (properties)
 *
 * @author panguanjing
 * @date 2022/2/17
 */
@Getter
public class ShowReferences extends BaseQueryStatement {

    private final ShowType showType;

    private final QualifiedName qualifiedName;

    private final List<Property> properties;

    public ShowReferences(Identifier businessUnit, ShowType showType, QualifiedName qualifiedName, List<Property> properties) {
        super(businessUnit);
        this.properties = properties;
        this.showType = showType;
        this.qualifiedName = qualifiedName;
        this.setStatementType(StatementType.SHOW);
    }

    public ShowReferences(ShowType showType, QualifiedName qualifiedName, List<Property> properties) {
        this(null, showType, qualifiedName, properties);
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowReferences(this, context);
    }
}
