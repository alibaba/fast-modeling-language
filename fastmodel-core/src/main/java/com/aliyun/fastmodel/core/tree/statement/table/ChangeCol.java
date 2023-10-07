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

import java.util.List;

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.literal.BaseLiteral;
import com.aliyun.fastmodel.core.tree.statement.BaseOperatorStatement;
import com.aliyun.fastmodel.core.tree.statement.constants.ColumnCategory;
import com.aliyun.fastmodel.core.tree.statement.constants.StatementType;
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * 修改列名称语句
 * <p>
 * DSL举例
 * <pre>
 *         -- 去除列的not null限制
 *  ALTER TABLE unit.table_name1 CHANGE COLUMN
 *                          old new varchar(20) not null disable COMMENT 'comment'
 *         --增加not null限制
 * ALTER TABLE unit.table_name1 CHANGE COLUMN
 *       old new varchar(20) not null COMMENT 'comment'
 *
 *
 *
 *     </pre>
 *
 * @author panguanjing
 * @date 2020/9/7
 */
@Getter
@EqualsAndHashCode(callSuper = true)
public class ChangeCol extends BaseOperatorStatement {

    /**
     * 原来列的名字
     */
    private final Identifier oldColName;

    private final ColumnDefinition columnDefinition;

    public ChangeCol(QualifiedName qualifiedName, Identifier oldColName, ColumnDefinition newColumnDefine) {
        super(qualifiedName);
        Preconditions.checkNotNull(newColumnDefine, "new column define can't be null");
        this.oldColName = oldColName;
        columnDefinition = newColumnDefine;
        setStatementType(StatementType.TABLE);
    }

    public Identifier getNewColName() {return getColumnDefinition().getColName();}

    public BaseDataType getDataType() {return getColumnDefinition().getDataType();}

    public Comment getComment() {return getColumnDefinition().getComment();}

    public Boolean getPrimary() {return getColumnDefinition().getPrimary();}

    public Boolean getNotNull() {return getColumnDefinition().getNotNull();}

    public BaseLiteral getDefaultValue() {return getColumnDefinition().getDefaultValue();}

    public ColumnCategory getCategory() {return getColumnDefinition().getCategory();}

    public List<Property> getColumnProperties() {return getColumnDefinition().getColumnProperties();}

    public String getCommentValue() {return getColumnDefinition().getCommentValue();}

    public boolean isPropertyEmpty() {return getColumnDefinition().isPropertyEmpty();}

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitChangeCol(this, context);
    }
}
