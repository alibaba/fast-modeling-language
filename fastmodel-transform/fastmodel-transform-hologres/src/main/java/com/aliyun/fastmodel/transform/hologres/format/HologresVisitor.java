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

package com.aliyun.fastmodel.transform.hologres.format;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import com.aliyun.fastmodel.core.formatter.ExpressionFormatter;
import com.aliyun.fastmodel.core.formatter.FastModelVisitor;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.datatype.GenericDataType;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.literal.BaseLiteral;
import com.aliyun.fastmodel.core.tree.statement.table.AddCols;
import com.aliyun.fastmodel.core.tree.statement.table.AddConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.AddPartitionCol;
import com.aliyun.fastmodel.core.tree.statement.table.ChangeCol;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.DropCol;
import com.aliyun.fastmodel.core.tree.statement.table.DropPartitionCol;
import com.aliyun.fastmodel.core.tree.statement.table.DropTable;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableComment;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableProperties;
import com.aliyun.fastmodel.core.tree.statement.table.UnSetTableProperties;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.PrimaryConstraint;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.aliyun.fastmodel.transform.api.format.DefaultExpressionVisitor;
import com.aliyun.fastmodel.transform.hologres.context.HologresTransformContext;

import static java.util.stream.Collectors.joining;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/4/15
 */
public class HologresVisitor extends FastModelVisitor {

    private final HologresTransformContext context;

    public HologresVisitor(HologresTransformContext context) {
        this.context = context;
    }

    @Override
    public Boolean visitCreateTable(CreateTable node, Integer indent) {
        boolean columnEmpty = node.isColumnEmpty();
        //maxcompute不支持没有列的表
        boolean executable = true;
        if (columnEmpty) {
            executable = false;
        }
        builder.append("BEGIN;\n");
        builder.append("CREATE TABLE ");
        if (node.isNotExists()) {
            builder.append("IF NOT EXISTS ");
        }
        String tableName = node.getIdentifier();
        builder.append(tableName);
        String elementIndent = indentString(indent + 1);
        if (!columnEmpty) {
            builder.append(" (\n");
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
            builder.append("\n").append(")");
        }
        if (!node.isPartitionEmpty()) {
            builder.append(" PARTITION BY LIST(").append(
                node.getPartitionedBy().getColumnDefinitions().stream().map(x -> x.getColName().getValue())
                    .collect(joining(","))).append(")");
        }
        builder.append(";\n");
        builder.append(callSetProperty(getCode(node.getQualifiedName()), "orientation", context.getOrientation()));
        builder.append(callSetProperty(getCode(node.getQualifiedName()), "time_to_live_in_seconds",
            String.valueOf(context.getTimeToLiveInSeconds())));
        if (node.getComment() != null) {
            builder.append(commentTable(getCode(node.getQualifiedName()), node.getCommentValue()));
        } else {
            if (node.getAliasedNameValue() != null) {
                builder.append(commentTable(getCode(node.getQualifiedName()), node.getAliasedNameValue()));
            }
        }
        if (!columnEmpty) {
            for (ColumnDefinition columnDefinition : node.getColumnDefines()) {
                if (columnDefinition.getComment() != null) {
                    builder.append(
                        commentColumn(getCode(node.getQualifiedName()), columnDefinition.getColName().getValue(),
                            columnDefinition.getCommentValue()));
                } else if (columnDefinition.getAliasValue() != null) {
                    builder.append(
                        commentColumn(getCode(node.getQualifiedName()), columnDefinition.getColName().getValue(),
                            columnDefinition.getAliasValue()));
                }
            }
        }
        builder.append("\nCOMMIT;");
        return executable;
    }

    private void appendConstraint(CreateTable node, Integer indent) {
        Iterator<BaseConstraint> iterator = node.getConstraintStatements().iterator();
        while (iterator.hasNext()) {
            BaseConstraint next = iterator.next();
            //hologres只有primary key定义
            if (!(next instanceof PrimaryConstraint)) {
                continue;
            }
            builder.append(",\n");
            process(next, indent + 1);
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

    private String callSetProperty(String code, String key, String value) {
        String format = "CALL SET_TABLE_PROPERTY('%s', '%s', '%s');\n";
        return String.format(format, code, key, value);
    }

    private String commentTable(String code, String comment) {
        String format = "COMMENT ON TABLE %s IS '%s';\n";
        return String.format(format, code, comment);
    }

    private String commentColumn(String code, String column, String comment) {
        String format = "COMMENT ON COLUMN %s.%s IS '%s';\n";
        return String.format(format, code, column, comment);
    }

    @Override
    protected String formatColumnDefinition(ColumnDefinition column, Integer max) {
        StringBuilder sb = appendNameAndType(column, max);
        boolean isPrimary = column.getPrimary() != null && column.getPrimary();
        if (isPrimary) {
            sb.append(" PRIMARY KEY");
        }
        boolean isNotNull = column.getNotNull() != null && column.getNotNull();
        if (!isPrimary && isNotNull) {
            sb.append(" NOT NULL");
        }
        return sb.toString();
    }

    private StringBuilder appendNameAndType(ColumnDefinition column, Integer max) {
        BaseDataType dataType = column.getDataType();
        StringBuilder sb = new StringBuilder()
            .append(formatColName(column.getColName(), max))
            .append(" ")
            .append(formatExpression(convert(dataType)));
        return sb;
    }

    @Override
    protected BaseDataType convert(BaseDataType dataType) {
        DataTypeEnums typeName = dataType.getTypeName();
        if (typeName == DataTypeEnums.STRING) {
            return new GenericDataType(new Identifier(DataTypeEnums.TEXT.name()), null);
        } else if (typeName == DataTypeEnums.DATETIME) {
            return DataTypeUtil.simpleType(DataTypeEnums.TIMESTAMP);
        } else if (typeName == DataTypeEnums.ARRAY || typeName == DataTypeEnums.MAP
            || typeName == DataTypeEnums.STRUCT) {
            return DataTypeUtil.simpleType(DataTypeEnums.JSON);
        }
        return dataType;
    }

    @Override
    public Boolean visitAddCols(AddCols addCols, Integer context) {
        builder.append("BEGIN;\n");
        builder.append("ALTER TABLE IF EXISTS ").append(getCode(addCols.getQualifiedName()));
        String columnList = addCols.getColumnDefineList().stream()
            .map(element -> " ADD COLUMN " + appendNameAndType(element, 0).toString()).collect(joining(","));
        builder.append(columnList).append(";\n");
        for (ColumnDefinition columnDefinition : addCols.getColumnDefineList()) {
            if (columnDefinition.getComment() != null) {
                builder.append(
                    commentColumn(getCode(addCols.getQualifiedName()), columnDefinition.getColName().getValue(),
                        columnDefinition.getCommentValue()));
            }
        }
        builder.append("\nCOMMIT;");
        return true;
    }

    @Override
    public Boolean visitDropTable(DropTable dropTable, Integer context) {
        builder.append("DROP TABLE IF EXISTS ").append(getCode(dropTable.getQualifiedName()));
        return true;
    }

    @Override
    public Boolean visitSetTableComment(SetTableComment setTableComment, Integer context) {
        builder.append(
            commentTable(getCode(setTableComment.getQualifiedName()), setTableComment.getComment().getComment()));
        return true;
    }

    @Override
    public Boolean visitSetTableProperties(SetTableProperties setTableProperties, Integer context) {
        builder.append("BEGIN;\n");
        List<Property> properties = setTableProperties.getProperties();
        for (Property p : properties) {
            builder.append(
                callSetProperty(getCode(setTableProperties.getQualifiedName()), p.getName(), p.getValue()));
        }
        builder.append("\nCOMMIT;");
        return true;
    }

    @Override
    public Boolean visitDropCol(DropCol dropCol, Integer context) {
        super.visitDropCol(dropCol, context);
        return false;
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
    public Boolean visitAddConstraint(AddConstraint addConstraint, Integer context) {
        super.visitAddConstraint(addConstraint, context);
        return false;
    }

    @Override
    public Boolean visitChangeCol(ChangeCol changeCol, Integer context) {
        Identifier oldColName = changeCol.getOldColName();
        Identifier newColName = changeCol.getNewColName();
        if (Objects.equals(oldColName, newColName)) {
            BaseLiteral defaultValue = changeCol.getDefaultValue();
            if (defaultValue == null) {
                super.visitChangeCol(changeCol, context);
                return false;
            } else {
                builder.append("ALTER TABLE ").append(getCode(changeCol.getQualifiedName()));
                builder.append(" ALTER COLUMN ").append(formatExpression(oldColName));
                builder.append(" SET DEFAULT ").append(formatExpression(defaultValue));
                return true;
            }
        } else {
            builder.append("ALTER TABLE ").append(getCode(changeCol.getQualifiedName()));
            builder.append(" RENAME COLUMN ").append(formatExpression(oldColName));
            builder.append(" TO ").append(formatExpression(newColName));
            return true;
        }
    }

    @Override
    protected String formatExpression(BaseExpression baseExpression) {
        return new DefaultExpressionVisitor().process(baseExpression);
    }

    @Override
    protected String getCode(QualifiedName qualifiedName) {
        return qualifiedName.getSuffix();
    }
}
