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

package com.aliyun.fastmodel.core.tree.statement.businessunit;

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.statement.BaseSetComment;
import com.aliyun.fastmodel.core.tree.statement.constants.StatementType;

/**
 * 设置业务单元备注
 * <p>
 * DSL举例
 * <pre>
 *         ALTER BU bu_name SET COMMENT 'new_comment';
 *     </pre>
 *
 * @author panguanjing
 * @date 2020/9/27
 */
public class SetBusinessUnitComment extends BaseSetComment {

    public SetBusinessUnitComment(QualifiedName qualifiedName, Comment identifier) {
        super(qualifiedName, identifier);
        setStatementType(StatementType.BUSINESSUNIT);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitSetBuComment(this, context);
    }
}
