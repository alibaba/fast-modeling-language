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

import com.aliyun.fastmodel.core.tree.AliasedName;
import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.literal.BaseLiteral;
import com.aliyun.fastmodel.core.tree.statement.constants.ColumnCategory;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * 表列
 *
 * @author panguanjing
 * @date 2020/9/4
 */
@Getter
@EqualsAndHashCode(callSuper = false)
public class ColumnDefinition extends TableElement {
    /**
     * 列名
     */
    private final Identifier colName;

    /**
     * 别名
     */
    private final AliasedName aliasedName;
    /**
     * 数据类型
     */
    private final BaseDataType dataType;
    /**
     * 备注
     */
    private final Comment comment;

    /**
     * 是否为主键
     */
    private final Boolean primary;

    /**
     * 是否为空
     */
    private Boolean notNull;

    /**
     * 默认值
     */
    private final BaseLiteral defaultValue;

    /**
     * 分类
     */
    private final ColumnCategory category;

    /**
     * 列属性
     */
    private final List<Property> columnProperties;

    /**
     * 列关联指标维度
     */
    private final QualifiedName refDimension;

    /**
     * 关联的指标列表
     */
    private final List<Identifier> refIndicators;

    public static ColumnBuilder builder() {
        return new ColumnBuilder();
    }

    protected ColumnDefinition(ColumnBuilder columnBuilder) {
        Preconditions.checkNotNull(columnBuilder.colName, "colName can't be null");
        colName = columnBuilder.colName;
        dataType = columnBuilder.dataType;
        comment = columnBuilder.comment;
        primary = columnBuilder.primary;
        notNull = columnBuilder.notNull;
        defaultValue = columnBuilder.defaultValue;
        category = columnBuilder.category;
        columnProperties = columnBuilder.columnProperties;
        aliasedName = columnBuilder.aliasedName;
        refDimension = columnBuilder.refDimension;
        refIndicators = columnBuilder.refIndicators;
    }

    public String getCommentValue() {
        if (comment == null) {
            return null;
        }
        return comment.getComment();
    }

    public String getAliasValue() {
        if (aliasedName == null) {
            return null;
        }
        return aliasedName.getName();
    }

    public boolean isPropertyEmpty() {
        return columnProperties == null || columnProperties.isEmpty();
    }

    public void setNotNull(Boolean b) {
        notNull = true;
    }

    public static class ColumnBuilder {
        /**
         * 列名
         */
        private Identifier colName;

        /**
         * 别名
         */
        private AliasedName aliasedName;

        /**
         * 列类型
         */
        private BaseDataType dataType;
        /**
         * 备注
         */
        private Comment comment;

        /**
         * 是否为主键
         */
        private Boolean primary;

        /**
         * 是否为空
         */
        private Boolean notNull;

        /**
         * 默认值
         */
        private BaseLiteral defaultValue;

        /**
         * 分类
         */
        private ColumnCategory category;

        /**
         * 列属性
         */
        private List<Property> columnProperties;

        /**
         * 关联指标维度
         */
        private QualifiedName refDimension;

        /**
         * 关联指标列表
         */
        private List<Identifier> refIndicators;

        public ColumnBuilder colName(Identifier colName) {
            this.colName = colName;
            return this;
        }

        public ColumnBuilder dataType(BaseDataType dataType) {
            this.dataType = dataType;
            return this;
        }

        public ColumnBuilder comment(Comment comment) {
            this.comment = comment;
            return this;
        }

        public ColumnBuilder primary(Boolean primary) {
            this.primary = primary;
            return this;
        }

        public ColumnBuilder notNull(Boolean notNull) {
            this.notNull = notNull;
            return this;
        }

        public ColumnBuilder defaultValue(BaseLiteral defaultValue) {
            this.defaultValue = defaultValue;
            return this;
        }

        public ColumnBuilder category(ColumnCategory category) {
            this.category = category;
            return this;
        }

        public ColumnBuilder properties(List<Property> properties) {
            columnProperties = properties;
            return this;
        }

        public ColumnBuilder aliasedName(AliasedName aliasedName) {
            this.aliasedName = aliasedName;
            return this;
        }

        public ColumnBuilder refDimension(QualifiedName qualifiedName) {
            refDimension = qualifiedName;
            return this;
        }

        public ColumnBuilder refIndicators(List<Identifier> refIndicators) {
            this.refIndicators = refIndicators;
            return this;
        }

        public ColumnDefinition build() {
            return new ColumnDefinition(this);
        }
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitColumnDefine(this, context);
    }
}
