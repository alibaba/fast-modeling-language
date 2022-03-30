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

package com.aliyun.fastmodel.core.tree.statement.delete;

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.statement.BaseOperatorStatement;
import com.aliyun.fastmodel.core.tree.statement.constants.DeleteType;
import com.aliyun.fastmodel.core.tree.statement.show.WhereCondition;
import lombok.Getter;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/1/15
 */
@Getter
public class Delete extends BaseOperatorStatement {

    private final WhereCondition whereCondition;

    /**
     * default is table
     */
    private final DeleteType deleteType;

    public Delete(QualifiedName qualifiedName, WhereCondition whereCondition) {
        this(qualifiedName, whereCondition, DeleteType.TABLE);
    }

    public Delete(QualifiedName qualifiedName,
                  WhereCondition whereCondition,
                  DeleteType deleteType) {
        super(qualifiedName);
        this.whereCondition = whereCondition;
        this.deleteType = deleteType;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDelete(this, context);
    }
}
