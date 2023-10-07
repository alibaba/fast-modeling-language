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

package com.aliyun.fastmodel.core.tree.statement.indicator;

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.BaseSetComment;
import com.aliyun.fastmodel.core.tree.statement.constants.StatementType;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

/**
 * 修改指标备注
 * <p>
 * DSL举例
 * <pre>
 *         ALTER INDICATOR unit.idc1 SET COMMENT 'comment'
 *     </pre>
 *
 * @author panguanjing
 * @date 2020/9/27
 */
@EqualsAndHashCode(callSuper = true)
public class SetIndicatorComment extends BaseSetComment {

    public SetIndicatorComment(QualifiedName qualifiedName, Comment comment) {
        super(qualifiedName, comment);
        this.setStatementType(StatementType.INDICATOR);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitSetIndicatorComment(this, context);
    }
}
