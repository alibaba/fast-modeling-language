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
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.BaseSetComment;
import com.aliyun.fastmodel.core.tree.statement.constants.StatementType;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * 修改列的备注
 * <p>
 * {code}
 * ALTER TABLE a.b CHANGE COLUMN col1 SET COMMENT 'comment'
 * {code}
 *
 * @author panguanjing
 * @date 2021/2/20
 */
@Getter
@EqualsAndHashCode(callSuper = true)
public class SetColComment extends BaseSetComment {

    private final Identifier changeColumn;

    public SetColComment(QualifiedName qualifiedName,
                         Identifier changeColumn, Comment comment) {
        super(qualifiedName, comment);
        this.changeColumn = changeColumn;
        setStatementType(StatementType.TABLE);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitSetColComment(this, context);
    }
}
