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

import java.util.Objects;

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.constants.ConstraintType;
import com.aliyun.fastmodel.core.tree.statement.table.TableElement;
import com.aliyun.fastmodel.core.tree.util.IdentifierUtil;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.Setter;

/**
 * 限制的语句, 定义下基本的约束名称和类型
 *
 * @author panguanjing
 * @date 2020/9/4
 */
@Getter
public abstract class BaseConstraint extends TableElement {

    /**
     * 约束名称
     */
    @Setter
    private Identifier name;

    /**
     * 约束类型
     * {@link ConstraintType}
     */
    private final ConstraintType constraintType;

    /**
     * 是否启用
     */
    private final Boolean enable;

    public BaseConstraint(Identifier constraintName, ConstraintType constraintType) {
        this(constraintName, constraintType, true);
    }

    public BaseConstraint(Identifier constraintName, ConstraintType constraintType, Boolean enable) {
        Preconditions.checkNotNull(constraintName);
        name = constraintName;
        this.constraintType = constraintType;
        this.enable = enable;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {return true;}
        if (o == null || getClass() != o.getClass()) {return false;}
        BaseConstraint that = (BaseConstraint)o;
        boolean equalName = Objects.equals(name, that.name);
        if (!equalName) {
            //如果是系统生成的，那么忽略这个比对
            equalName = IdentifierUtil.isSysIdentifier(name) && IdentifierUtil.isSysIdentifier(that.name);
        }
        return equalName && constraintType == that.constraintType && Objects
            .equals(enable, that.enable);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, constraintType, enable);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitConstraint(this, context);
    }
}
