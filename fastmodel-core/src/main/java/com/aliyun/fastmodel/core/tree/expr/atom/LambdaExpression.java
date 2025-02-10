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

package com.aliyun.fastmodel.core.tree.expr.atom;

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.NodeLocation;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * lambda expression
 *
 * @author panguanjing
 * @date 2024/10/4
 */
@Getter
@EqualsAndHashCode(callSuper = false)
public class LambdaExpression extends BaseExpression {

    private final Identifier identifier;

    private final BaseExpression expression;

    public LambdaExpression(NodeLocation location, Identifier identifier, BaseExpression expression) {
        super(location);
        this.identifier = identifier;
        this.expression = expression;
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        return visitor.visitLambdaExpression(this, context);
    }
}
