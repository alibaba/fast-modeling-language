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

package com.aliyun.fastmodel.core.tree.statement.layer;

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.statement.BaseSetComment;
import com.aliyun.fastmodel.core.tree.statement.constants.StatementType;

/**
 * 设置数仓分层的处理
 * <p>
 * 设置数仓分层的备注
 * <pre>
 *     ALTER LAYER dingtalk.ods SET COMMENT 'comment';
 * </pre>
 * @author panguanjing
 * @date 2020/11/30
 */
public class SetLayerComment extends BaseSetComment {
    public SetLayerComment(QualifiedName qualifiedName,
                           Comment comment) {
        super(qualifiedName, comment);
        this.setStatementType(StatementType.LAYER);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitSetLayerComment(this, context);
    }
}
