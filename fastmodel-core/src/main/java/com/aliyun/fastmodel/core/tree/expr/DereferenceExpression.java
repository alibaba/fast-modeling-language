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

package com.aliyun.fastmodel.core.tree.expr;

import java.util.List;

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.NodeLocation;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.google.common.collect.ImmutableList;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * 支持
 *
 * @author panguanjing
 * @date 2020/11/5
 */
@Getter
@EqualsAndHashCode(callSuper = false)
public class DereferenceExpression extends BaseExpression {
    private final BaseExpression base;
    private final Identifier field;

    public DereferenceExpression(BaseExpression base, Identifier field) {
        super(null, null);
        this.base = base;
        this.field = field;
    }

    public DereferenceExpression(NodeLocation location,
                                 String origin,
                                 BaseExpression base, Identifier field) {
        super(location, origin);
        this.base = base;
        this.field = field;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDereferenceExpression(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of(base, field);
    }

    public static BaseExpression from(QualifiedName name) {
        BaseExpression result = null;

        for (String part : name.getParts()) {
            if (result == null) {
                result = new Identifier(part);
            } else {
                result = new DereferenceExpression(result, new Identifier(part));
            }
        }

        return result;
    }

}
