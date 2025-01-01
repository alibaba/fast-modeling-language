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

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.CustomConstraint;
import com.aliyun.fastmodel.transform.api.extension.visitor.ExtensionAstVisitor;
import lombok.Getter;

/**
 * @author 子梁
 * @date 2024/5/16
 */
@Getter
public class WaterMarkConstraint extends CustomConstraint {

    public static final String TYPE = "WATERMARK";

    private final Identifier column;
    private final StringLiteral expression;

    public WaterMarkConstraint(Identifier constraintName, Boolean enable, Identifier column, StringLiteral expression) {
        super(constraintName, enable, TYPE);
        this.column = column;
        this.expression = expression;
    }

    public WaterMarkConstraint(Identifier constraintName, Identifier column, StringLiteral expression) {
        super(constraintName, true, TYPE);
        this.column = column;
        this.expression = expression;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        ExtensionAstVisitor<R, C> extensionVisitor = (ExtensionAstVisitor<R, C>)visitor;
        return extensionVisitor.visitWaterMarkConstraint(this, context);
    }

}
