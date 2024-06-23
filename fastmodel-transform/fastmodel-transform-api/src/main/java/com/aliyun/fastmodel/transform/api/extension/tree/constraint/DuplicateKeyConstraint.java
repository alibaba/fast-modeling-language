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

package com.aliyun.fastmodel.transform.api.extension.tree.constraint;

import java.util.List;

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.CustomConstraint;
import com.aliyun.fastmodel.transform.api.extension.visitor.ExtensionAstVisitor;
import lombok.Getter;

/**
 * aggregate constraint
 *
 * @author panguanjing
 * @date 2023/9/11
 */
@Getter
public class DuplicateKeyConstraint extends CustomConstraint {

    public static final String TYPE = "DUPLICATE";

    private final List<Identifier> columns;

    public DuplicateKeyConstraint(Identifier constraintName, List<Identifier> columns, Boolean enable) {
        super(constraintName, enable, TYPE);
        this.columns = columns;
    }

    public DuplicateKeyConstraint(Identifier constraintName, List<Identifier> columns) {
        this(constraintName, columns, true);
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        ExtensionAstVisitor<R, C> extensionVisitor = (ExtensionAstVisitor<R, C>)visitor;
        return extensionVisitor.visitDuplicateConstraint(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        return columns;
    }
}
