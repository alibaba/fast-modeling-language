/*
 * Copyright [2024] [name of copyright owner]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.fastmodel.transform.api.extension.tree.constraint.desc;

import java.util.List;

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.transform.api.extension.visitor.ExtensionAstVisitor;
import lombok.Getter;

/**
 * OrderByConstraint
 *
 * @author panguanjing
 * @date 2023/12/13
 */
@Getter
public class OrderByConstraint extends NonKeyConstraint {

    public static final String TYPE = "OrderBy";

    private final List<Identifier> columns;

    public OrderByConstraint(Identifier constraintName, Boolean enable, List<Identifier> columns) {
        super(constraintName, enable, TYPE);
        this.columns = columns;
    }

    public OrderByConstraint(Identifier constraintName, List<Identifier> columns) {
        super(constraintName, true, TYPE);
        this.columns = columns;
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        ExtensionAstVisitor<R, C> extensionVisitor = (ExtensionAstVisitor<R, C>)visitor;
        return extensionVisitor.visitOrderByConstraint(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        return columns;
    }
}
