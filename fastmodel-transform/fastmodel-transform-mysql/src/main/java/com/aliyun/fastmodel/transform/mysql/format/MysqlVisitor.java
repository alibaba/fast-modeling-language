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

package com.aliyun.fastmodel.transform.mysql.format;

import java.util.Iterator;
import java.util.List;

import com.aliyun.fastmodel.core.formatter.ExpressionFormatter;
import com.aliyun.fastmodel.core.formatter.FastModelVisitor;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.datatype.GenericDataType;
import com.aliyun.fastmodel.core.tree.datatype.NumericParameter;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.constants.ConstraintType;
import com.aliyun.fastmodel.core.tree.statement.script.RefObject;
import com.aliyun.fastmodel.core.tree.statement.script.RefRelation;
import com.aliyun.fastmodel.core.tree.statement.table.AddCols;
import com.aliyun.fastmodel.core.tree.statement.table.AddConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.AddPartitionCol;
import com.aliyun.fastmodel.core.tree.statement.table.ChangeCol;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.DropConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.DropPartitionCol;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableProperties;
import com.aliyun.fastmodel.core.tree.statement.table.UnSetTableProperties;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.DimConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.PrimaryConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.UniqueConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.index.TableIndex;
import com.aliyun.fastmodel.transform.api.datatype.DataTypeConverter;
import com.aliyun.fastmodel.transform.api.format.DefaultExpressionVisitor;
import com.aliyun.fastmodel.transform.mysql.context.MysqlTransformContext;
import com.google.common.collect.ImmutableList;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;

import static com.aliyun.fastmodel.core.formatter.ExpressionFormatter.formatName;
import static java.util.stream.Collectors.joining;

/**
 * 基于FML的visitor处理内容
 *
 * @author panguanjing
 * @date 2021/6/24
 */
public class MysqlVisitor extends FastModelVisitor {

    private final MysqlTransformContext mysqlTransformContext;

    private final DataTypeConverter dataTypeTransformer;

    public MysqlVisitor(MysqlTransformContext mysqlTransformContext) {
        this.mysqlTransformContext = mysqlTransformContext;
        dataTypeTransformer = this.mysqlTransformContext != null ? mysqlTransformContext.getDataTypeTransformer()
            : null;
    }

    @Override
    public Boolean visitCreateTable(CreateTable node, Integer indent) {
        boolean columnEmpty = node.isColumnEmpty();
        //maxcompute不支持没有列的表
        boolean executable = true;
        if (columnEmpty) {
            executable = false;
        }
        builder.append("CREATE TABLE ");
        if (node.isNotExists()) {
            builder.append("IF NOT EXISTS ");
        }
        String tableName = node.getIdentifier();
        builder.append(tableName);
        if (!columnEmpty) {
            builder.append("\n(\n");
            String elementIndent = indentString(indent + 1);
            String columnList = formatColumnList(node.getColumnDefines(), elementIndent);
            builder.append(columnList);
            if (!node.isPartitionEmpty()) {
                builder.append(",\n");
                String list = formatColumnList(node.getPartitionedBy().getColumnDefinitions(), elementIndent);
                builder.append(list);
            }
            if (!node.isConstraintEmpty()) {
                appendConstraint(node, indent);
            }
            if (!node.isIndexEmpty()) {
                Iterator<TableIndex> iterator = node.getTableIndexList().iterator();
                while (iterator.hasNext()) {
                    builder.append(",\n");
                    process(iterator.next(), indent + 1);
                }
            }
            builder.append("\n").append(")");
        }
        builder.append(formatComment(node.getComment()));
        return executable;
    }

    private void appendConstraint(CreateTable node, Integer indent) {
        Iterator<BaseConstraint> iterator = node.getConstraintStatements().iterator();
        while (iterator.hasNext()) {
            BaseConstraint next = iterator.next();
            if (next instanceof PrimaryConstraint || next instanceof UniqueConstraint) {
                builder.append(",\n");
                process(next, indent + 1);
            }
        }
    }

    @Override
    public Boolean visitPrimaryConstraint(PrimaryConstraint primaryConstraint, Integer indent) {
        builder.append(indentString(indent)).append("PRIMARY KEY(");
        builder.append(
            primaryConstraint.getColNames().stream().map(ExpressionFormatter::formatExpression).collect(joining(",")));
        builder.append(")");
        return true;
    }

    @Override
    public Boolean visitAddCols(AddCols addCols, Integer context) {
        builder.append("ALTER TABLE ").append(getCode(addCols.getQualifiedName()));
        builder.append(" ADD\n").append('(').append("\n");
        String elementIndent = indentString(context + 1);
        String columnList = formatColumnList(addCols.getColumnDefineList(), elementIndent);
        builder.append(columnList);
        builder.append("\n").append(')');
        return true;
    }

    @Override
    public Boolean visitDropConstraint(DropConstraint dropConstraint, Integer context) {
        if (dropConstraint.getConstraintType() == null) {
            super.visitDropConstraint(dropConstraint, context);
            return false;
        }
        ConstraintType constraintType = dropConstraint.getConstraintType();
        if (constraintType == ConstraintType.PRIMARY_KEY) {
            builder.append("ALTER TABLE ").append(getCode(dropConstraint.getQualifiedName()));
            builder.append(" DROP PRIMARY KEY");
        } else if (constraintType == ConstraintType.DIM_KEY) {
            builder.append("ALTER TABLE ").append(getCode(dropConstraint.getQualifiedName()));
            if (mysqlTransformContext.isGenerateForeignKey()) {
                builder.append(" DROP FOREIGN KEY ").append(formatExpression(dropConstraint.getConstraintName()));
            } else {
                super.visitDropConstraint(dropConstraint, context);
                return false;
            }
        } else {
            super.visitDropConstraint(dropConstraint, context);
            return false;
        }
        return true;
    }

    @Override
    public Boolean visitChangeCol(ChangeCol renameCol, Integer context) {
        builder.append("ALTER TABLE ").append(getCode(renameCol.getQualifiedName()));
        builder.append(" CHANGE COLUMN ").append(ExpressionFormatter.formatExpression(renameCol.getOldColName()));
        builder.append(" ").append(formatColumnDefinition(renameCol.getColumnDefinition(), 0));
        return true;
    }

    @Override
    public Boolean visitAddConstraint(AddConstraint addConstraint, Integer context) {
        BaseConstraint constraintStatement = addConstraint.getConstraintStatement();
        if (constraintStatement instanceof PrimaryConstraint) {
            PrimaryConstraint constraint = (PrimaryConstraint)constraintStatement;
            List<Identifier> colNames = constraint.getColNames();
            builder.append("ALTER TABLE ").append(getCode(addConstraint.getQualifiedName()));
            builder.append(" ADD CONSTRAINT ").append(formatExpression(constraint.getName()));
            builder.append(" PRIMARY KEY ");
            builder.append("(");
            builder.append(constraint.getColNames().stream().map(Identifier::getValue).collect(joining(",")));
            builder.append(")");
        } else if (constraintStatement instanceof DimConstraint) {
            if (!mysqlTransformContext.isGenerateForeignKey()) {
                super.visitAddConstraint(addConstraint, context);
                return false;
            } else {
                DimConstraint dimConstraint = (DimConstraint)constraintStatement;
                List<Identifier> colNames = dimConstraint.getColNames();
                if (CollectionUtils.isEmpty(colNames)) {
                    super.visitAddConstraint(addConstraint, context);
                    return false;
                }
                builder.append("ALTER TABLE ").append(getCode(addConstraint.getQualifiedName()));
                builder.append(" ADD CONSTRAINT ").append(
                    formatExpression(addConstraint.getConstraintStatement().getName()));
                builder.append(" FOREIGN KEY (");

                builder.append(colNames.stream().map(Identifier::getValue).collect(joining(","))).append(")");
                List<Identifier> referenceColNames = dimConstraint.getReferenceColNames();
                builder.append(" REFERENCES ").append(dimConstraint.getReferenceTable()).append("(").append(
                    referenceColNames.stream().map(Identifier::getValue).collect(joining(","))).append(")");
            }
        } else {
            super.visitAddConstraint(addConstraint, context);
            return false;
        }
        return true;
    }

    @Override
    public Boolean visitAddPartitionCol(AddPartitionCol addPartitionCol, Integer context) {
        super.visitAddPartitionCol(addPartitionCol, context);
        return false;
    }

    @Override
    public Boolean visitDropPartitionCol(DropPartitionCol dropPartitionCol, Integer context) {
        super.visitDropPartitionCol(dropPartitionCol, context);
        return false;
    }

    @Override
    public Boolean visitUnSetTableProperties(UnSetTableProperties unSetTableProperties, Integer context) {
        super.visitUnSetTableProperties(unSetTableProperties, context);
        return false;
    }

    @Override
    public Boolean visitSetTableProperties(SetTableProperties setTableProperties, Integer context) {
        super.visitSetTableProperties(setTableProperties, context);
        return false;
    }

    @Override
    protected String getCode(QualifiedName qualifiedName) {
        return qualifiedName.getSuffix();
    }

    @Override
    protected String formatColumnDefinition(ColumnDefinition column, Integer max) {
        BaseDataType dataType = column.getDataType();
        StringBuilder sb = new StringBuilder();
        sb.append(formatColName(column.getColName(), max));
        sb.append(" ").append(formatExpression(convert(dataType)));
        Boolean notNull = column.getNotNull();
        if (BooleanUtils.isTrue(notNull)) {
            sb.append(" NOT NULL");
        } else if (BooleanUtils.isFalse(notNull)) {
            sb.append(" NULL");
        }
        if (column.getDefaultValue() != null) {
            sb.append(" DEFAULT ").append(formatExpression(column.getDefaultValue()));
        }
        boolean isPrimary = column.getPrimary() != null && column.getPrimary();
        if (isPrimary) {
            if (mysqlTransformContext.isAutoIncrement() && DataTypeEnums.isIntDataType(dataType.getTypeName())) {
                sb.append(" AUTO_INCREMENT ");
            }
            List<Property> columnProperties = column.getColumnProperties();
            sb.append(" PRIMARY KEY");
        }
        sb.append(formatComment(column.getComment()));
        return sb.toString();
    }

    @Override
    protected BaseDataType convert(BaseDataType dataType) {
        if (dataTypeTransformer != null) {
            return dataTypeTransformer.convert(dataType);
        }
        DataTypeEnums typeName = dataType.getTypeName();
        if (typeName == DataTypeEnums.STRING) {
            return new GenericDataType(new Identifier(DataTypeEnums.VARCHAR.name()),
                ImmutableList.of(new NumericParameter(mysqlTransformContext.getVarcharLength().toString())));
        } else if (typeName == DataTypeEnums.ARRAY || typeName == DataTypeEnums.MAP
            || typeName == DataTypeEnums.STRUCT) {
            return new GenericDataType(new Identifier(DataTypeEnums.JSON.name()));
        } else if (typeName == DataTypeEnums.BOOLEAN) {
            return new GenericDataType(new Identifier(DataTypeEnums.CHAR.name()),
                ImmutableList.of(new NumericParameter("1")));
        }
        return dataType;
    }

    @Override
    public Boolean visitRefEntityStatement(RefRelation refEntityStatement, Integer context) {
        //转为dim constraint
        RefObject left = refEntityStatement.getLeft();
        RefObject right = refEntityStatement.getRight();
        List<Identifier> columnList = left.getAttrNameList();
        List<Identifier> rightColumnList = right.getAttrNameList();
        if (CollectionUtils.isEmpty(columnList) || CollectionUtils.isEmpty(rightColumnList)) {
            return false;
        }
        //ALTER TABLE `a` ADD CONSTRAINT `name` FOREIGN KEY (`a`) REFERENCES `b` (`a`);
        builder.append("ALTER TABLE ").append(formatName(left.getMainName()));
        builder.append(" ADD CONSTRAINT ").append(formatName(refEntityStatement.getQualifiedName()));
        String collect = columnList.stream().map(identifier -> {
            return formatExpression(identifier);
        }).collect(joining(","));
        builder.append(" FOREIGN KEY (").append(collect).append(")");
        builder.append(" REFERENCES ").append(formatName(right.getMainName()));
        String rightReference = rightColumnList.stream().map(identifier -> {
            return formatExpression(identifier);
        }).collect(joining(","));
        builder.append(" (").append(rightReference).append(")");
        return true;
    }

    @Override
    protected String formatExpression(BaseExpression baseExpression) {
        return new DefaultExpressionVisitor().process(baseExpression);
    }
}
