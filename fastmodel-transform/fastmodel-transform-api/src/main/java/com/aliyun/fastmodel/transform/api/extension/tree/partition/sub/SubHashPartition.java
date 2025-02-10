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

package com.aliyun.fastmodel.transform.api.extension.tree.partition.sub;

import java.util.List;

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.literal.LongLiteral;
import com.aliyun.fastmodel.transform.api.extension.visitor.ExtensionAstVisitor;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import lombok.Getter;

/**
 * sub hash partition
 *
 * @author panguanjing
 * @date 2024/2/6
 */
@Getter
public class SubHashPartition extends BaseSubPartition {

    private final BaseExpression expression;
    private final LongLiteral subpartitionCount;

    public SubHashPartition(BaseExpression expression, LongLiteral subpartitionCount) {
        Preconditions.checkNotNull(expression, "expression can't be null");
        this.expression = expression;
        this.subpartitionCount = subpartitionCount;
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of(expression, subpartitionCount);
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        ExtensionAstVisitor<R, C> extensionAstVisitor = (ExtensionAstVisitor<R, C>)visitor;
        return extensionAstVisitor.visitSubHashPartition(this, context);
    }
}
