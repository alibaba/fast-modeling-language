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

package com.aliyun.fastmodel.core.tree.statement.table;

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.statement.BaseOperatorStatement;
import com.aliyun.fastmodel.core.tree.statement.constants.StatementType;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * 增加约束
 * <p>
 * DSL举例
 * <pre>
 *         ALTER TABLE unit.t1 ADD CONSTRAINT c1 DIM REFERENCES unit.dim_table
 *     </pre>
 *
 * @author panguanjing
 * @date 2020/9/7
 */
@Getter
@EqualsAndHashCode(callSuper = true)
public class AddConstraint extends BaseOperatorStatement {

    /**
     * constraintStatement
     */
    private final BaseConstraint constraintStatement;

    public AddConstraint(QualifiedName qualifiedName, BaseConstraint constraintStatement) {
        super(qualifiedName);
        this.constraintStatement = constraintStatement;
        setStatementType(StatementType.TABLE);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAddConstraint(this, context);
    }

}
