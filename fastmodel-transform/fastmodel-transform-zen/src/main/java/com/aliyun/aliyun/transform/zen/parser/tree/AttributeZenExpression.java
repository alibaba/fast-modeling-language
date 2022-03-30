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

package com.aliyun.aliyun.transform.zen.parser.tree;

import com.aliyun.aliyun.transform.zen.parser.BaseZenAstVisitor;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import lombok.Getter;

/**
 * 属性类的表达式
 *
 * @author panguanjing
 * @date 2021/7/14
 */
@Getter
public class AttributeZenExpression extends BaseAtomicZenExpression {
    private final BaseAtomicZenExpression baseZenExpression;
    private final Identifier identifier;
    private final StringLiteral stringLiteral;

    public AttributeZenExpression(BaseAtomicZenExpression baseZenExpression,
                                  Identifier identifier,
                                  StringLiteral stringLiteral) {
        this.baseZenExpression = baseZenExpression;
        this.identifier = identifier;
        this.stringLiteral = stringLiteral;
    }

    @Override
    public <R, C> R accept(BaseZenAstVisitor<R, C> visitor, C context) {
        return visitor.visitAttributeZenExpression(this, context);
    }
}
