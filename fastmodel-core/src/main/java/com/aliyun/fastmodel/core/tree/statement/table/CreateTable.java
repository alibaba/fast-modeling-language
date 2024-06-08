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

import com.aliyun.fastmodel.core.semantic.table.CreateTableSemanticCheck;
import com.aliyun.fastmodel.core.tree.AliasedName;
import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.BaseCreate;
import com.aliyun.fastmodel.core.tree.statement.constants.StatementType;
import com.aliyun.fastmodel.core.tree.statement.element.CreateElement;
import com.aliyun.fastmodel.core.tree.statement.element.MultiComment;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.index.TableIndex;
import com.aliyun.fastmodel.core.tree.statement.table.type.ITableDetailType;
import com.google.common.collect.ImmutableList;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * 创建表的语句
 *
 * @author panguanjing
 * @date 2020/9/3
 */
@Getter
@EqualsAndHashCode(callSuper = true)
public class CreateTable extends BaseCreate {

    /**
     * 列名类型
     */
    private final List<ColumnDefinition> columnDefines;

    /**
     * 约束语句
     */
    private final List<BaseConstraint> constraintStatements;

    /**
     * 索引语句
     */
    private final List<TableIndex> tableIndexList;

    /**
     * 表的详细类型
     */
    private final ITableDetailType tableDetailType;

    /**
     * 分区内容
     */
    private final PartitionedBy partitionedBy;

    /**
     * 是否replace信息
     */
    private final Boolean createOrReplace;

    /**
     * column comment elements
     */
    @EqualsAndHashCode.Exclude
    private final List<MultiComment> columnCommentElements;

    protected CreateTable(TableBuilder tableBuilder) {
        super(CreateElement.builder()
            .qualifiedName(tableBuilder.tableName)
            .notExists(tableBuilder.ifNotExist)
            .comment(tableBuilder.comment)
            .properties(tableBuilder.properties)
            .aliasedName(tableBuilder.aliasedName)
            .createOrReplace(tableBuilder.createOrReplace)
            .build());
        columnDefines = tableBuilder.columnDefines;
        constraintStatements = tableBuilder.constraints;
        tableDetailType = tableBuilder.detailType;
        partitionedBy = tableBuilder.partition;
        createOrReplace = tableBuilder.createOrReplace;
        columnCommentElements = tableBuilder.commentElements;
        tableIndexList = tableBuilder.tableIndexList;
        setStatementType(StatementType.TABLE);
        new CreateTableSemanticCheck().check(this);
    }

    public static TableBuilder builder() {
        return new TableBuilder();
    }

    /**
     * 提供比较方便获取TableType的方法
     *
     * @return String
     */
    public String getTableType() {
        return tableDetailType != null ? tableDetailType.getParent().getCode() : null;
    }

    /**
     * 是否列为空
     *
     * @return
     */
    public boolean isColumnEmpty() {
        return columnDefines == null || columnDefines.isEmpty();
    }

    /**
     * 是否注释的元素为空
     *
     * @return
     */
    public boolean isCommentElementEmpty() {
        return columnCommentElements == null || columnCommentElements.isEmpty();
    }

    /**
     * 是否分区分空
     *
     * @return
     */
    public boolean isPartitionEmpty() {
        return partitionedBy == null || partitionedBy.getColumnDefinitions() == null || partitionedBy
            .getColumnDefinitions().isEmpty();
    }

    /**
     * 是否约束为空
     *
     * @return
     */
    public boolean isConstraintEmpty() {
        return constraintStatements == null || constraintStatements.isEmpty();
    }

    /**
     * 是否索引为空
     *
     * @return
     */
    public boolean isIndexEmpty() {
        return tableIndexList == null || tableIndexList.isEmpty();
    }

    public ColumnDefinition getColumn(Identifier colName) {
        if (!isColumnEmpty()) {
            for (ColumnDefinition c : getColumnDefines()) {
                if (!Objects.equals(c.getColName(), colName)) {continue;}
                return c;
            }
        }
        if (!isPartitionEmpty()) {
            for (ColumnDefinition c : getPartitionedBy().getColumnDefinitions()) {
                if (!Objects.equals(c.getColName(), colName)) {
                    continue;
                }
                return c;
            }
        }
        return null;
    }

    @Override
    public List<? extends Node> getChildren() {
        ImmutableList.Builder<Node> builder = ImmutableList.builder();
        if (!isColumnEmpty()) {
            builder.addAll(columnDefines);
        }
        if (!isConstraintEmpty()) {
            builder.addAll(constraintStatements);
        }
        return builder.build();
    }

    public static class TableBuilder<T extends TableBuilder> {

        protected List<ColumnDefinition> columnDefines;
        protected QualifiedName tableName;
        protected List<Property> properties;
        protected Comment comment;
        protected boolean ifNotExist;
        protected List<BaseConstraint> constraints;
        protected ITableDetailType detailType;
        protected PartitionedBy partition;
        protected Boolean createOrReplace;
        protected List<MultiComment> commentElements;
        protected AliasedName aliasedName;
        protected List<TableIndex> tableIndexList;

        public T columns(List<ColumnDefinition> columnDefines) {
            this.columnDefines = columnDefines;
            return (T)this;
        }

        public T tableName(QualifiedName tableName) {
            this.tableName = tableName;
            return (T)this;
        }

        public T properties(List<Property> properties) {
            this.properties = properties;
            return (T)this;
        }

        public T comment(Comment comment) {
            this.comment = comment;
            return (T)this;
        }

        public T ifNotExist(Boolean ifNotExist) {
            this.ifNotExist = ifNotExist;
            return (T)this;
        }

        public T constraints(List<BaseConstraint> constraints) {
            this.constraints = constraints;
            return (T)this;
        }

        public T detailType(ITableDetailType detailType) {
            this.detailType = detailType;
            return (T)this;
        }

        public T partition(PartitionedBy partitionedBy) {
            partition = partitionedBy;
            return (T)this;
        }

        public T createOrReplace(Boolean createOrReplace) {
            this.createOrReplace = createOrReplace;
            return (T)this;
        }

        public T insertColumnComment(MultiComment commentElement) {
            if (commentElements == null) {
                commentElements = new ArrayList<>();
            }
            commentElements.add(commentElement);
            return (T)this;
        }

        public T columnComments(List<MultiComment> commentElements) {
            this.commentElements = commentElements;
            return (T)this;
        }

        public T aliasedName(AliasedName aliasedName) {
            this.aliasedName = aliasedName;
            return (T)this;
        }

        public T tableIndex(List<TableIndex> tableIndexList) {
            this.tableIndexList = tableIndexList;
            return (T)this;
        }

        public CreateTable build() {
            return new CreateTable(this);
        }
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateTable(this, context);
    }
}
