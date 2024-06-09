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

import java.util.EnumSet;
import java.util.List;
import java.util.Optional;

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.BaseOperatorStatement;
import com.aliyun.fastmodel.core.tree.statement.constants.ColumnCategory;
import com.aliyun.fastmodel.core.tree.statement.constants.StatementType;
import com.google.common.base.Preconditions;
import lombok.Data;
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

    /**
     * 新的列信息
     */
    private final ColumnDefinition columnDefinition;

    /**
     * change状态
     */
    private Optional<ChangeState> changeState;

    public ChangeCol(QualifiedName qualifiedName, Identifier oldColName, ColumnDefinition newColumnDefine) {
        this(qualifiedName, oldColName, newColumnDefine, Optional.empty());
    }

    public ChangeCol(QualifiedName qualifiedName, Identifier oldColName, ColumnDefinition newColumnDefine, Optional<ChangeState> changeState) {
        super(qualifiedName);
        Preconditions.checkNotNull(newColumnDefine, "new column define can't be null");
        this.oldColName = oldColName;
        columnDefinition = newColumnDefine;
        this.changeState = changeState;
        setStatementType(StatementType.TABLE);
    }

    public Identifier getNewColName() {return getColumnDefinition().getColName();}

    public BaseDataType getDataType() {return getColumnDefinition().getDataType();}

    public Comment getComment() {return getColumnDefinition().getComment();}

    public Boolean getPrimary() {return getColumnDefinition().getPrimary();}

    public Boolean getNotNull() {return getColumnDefinition().getNotNull();}

    public BaseExpression getDefaultValue() {return getColumnDefinition().getDefaultValue();}

    public ColumnCategory getCategory() {return getColumnDefinition().getCategory();}

    public List<Property> getColumnProperties() {return getColumnDefinition().getColumnProperties();}

    public String getCommentValue() {return getColumnDefinition().getCommentValue();}

    public boolean isPropertyEmpty() {return getColumnDefinition().isPropertyEmpty();}

    public boolean change(ChangeType changeType) {
        if (changeState == null || this.changeState.isEmpty()) {
            return false;
        }
        return changeState.get().changeTypes.contains(changeType);
    }

    public enum ChangeType {
        /**
         * 改变名称
         */
        NAME,
        /**
         * 改变别名
         */
        ALIAS,
        /**
         * 改变数据类型
         */
        DATA_TYPE,
        /**
         * 改变非空
         */
        NOT_NULL,
        /**
         * 改变默认值
         */
        DEFAULT_VALUE,
        /**
         * 改变主键
         */
        PRIMARY,
        /**
         * 改变注释
         */
        COMMENT,
        /**
         * 改变属性
         */
        COLUMN_PROPERTIES,

        /**
         * 分类
         */
        CATEGORY,

        /**
         * 维度
         */
        DIM,

        /**
         * 指标
         */
        REF_INDICATORS;
    }

    /**
     * 定义修改列的状态
     */
    @Data
    public static class ChangeState {

        private EnumSet<ChangeType> changeTypes = EnumSet.noneOf(ChangeType.class);

        public void setChangeName() {
            changeTypes.add(ChangeType.NAME);
        }

        public void setChangeAlias() {
            changeTypes.add(ChangeType.ALIAS);
        }

        public void setChangeDataType() {
            changeTypes.add(ChangeType.DATA_TYPE);
        }

        public void setChangeNotNull() {
            changeTypes.add(ChangeType.NOT_NULL);
        }

        public void setChangeDefaultValue() {
            changeTypes.add(ChangeType.DEFAULT_VALUE);
        }

        public void setChangePrimary() {
            changeTypes.add(ChangeType.PRIMARY);
        }

        public void setChangeComment() {
            changeTypes.add(ChangeType.COMMENT);
        }

        public void setChangeColumnProperties() {
            changeTypes.add(ChangeType.COLUMN_PROPERTIES);
        }

        public void setChangeCategory() {
            changeTypes.add(ChangeType.CATEGORY);
        }

        public void setChangeDim() {
            changeTypes.add(ChangeType.DIM);
        }

        public void setChangeRefIndicators() {
            changeTypes.add(ChangeType.REF_INDICATORS);
        }

        public boolean nothingChange() {
            return changeTypes.isEmpty();
        }
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitChangeCol(this, context);
    }
}
