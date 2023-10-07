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

package com.aliyun.fastmodel.core.tree.statement.table.constraint;

import java.util.List;

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.constants.ConstraintType;
import com.google.common.collect.ImmutableList;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * 非空约束
 *
 * @author panguanjing
 * @date 2020/11/6
 */
@Getter
@EqualsAndHashCode(callSuper = false)
public class NotNullConstraint extends BaseConstraint {

    public NotNullConstraint(Identifier constraintName, Boolean enable) {
        super(constraintName, ConstraintType.NOT_NULL, enable);
    }

    public NotNullConstraint(Identifier constraintName) {
        this(constraintName, true);
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitNotNullConstraint(this, context);
    }
}


