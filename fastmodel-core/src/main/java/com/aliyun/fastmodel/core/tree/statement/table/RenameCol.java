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
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.BaseOperatorStatement;
import com.aliyun.fastmodel.core.tree.statement.constants.StatementType;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * 修改类信息
 * <p>
 * {code}
 * ALTER TABLE a.b change column col1 rename to newColName;
 * {code}
 *
 * @author panguanjing
 * @date 2021/2/20
 */
@Getter
@EqualsAndHashCode(callSuper = true)
public class RenameCol extends BaseOperatorStatement {

    private final Identifier oldColName;
    private final Identifier newColName;

    public RenameCol(QualifiedName qualifiedName, Identifier oldColName,
                     Identifier newColName) {
        super(qualifiedName);
        this.oldColName = oldColName;
        this.newColName = newColName;
        setStatementType(StatementType.TABLE);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitRenameCol(this, context);
    }
}
