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

package com.aliyun.fastmodel.transform.oracle.format;

import java.util.Iterator;
import java.util.List;

import com.aliyun.fastmodel.core.formatter.FastModelVisitor;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.statement.table.AddCols;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.DropTable;
import com.aliyun.fastmodel.core.tree.statement.table.SetColComment;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableComment;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.transform.api.format.DefaultExpressionVisitor;
import com.aliyun.fastmodel.transform.oracle.context.OracleContext;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.BooleanUtils;

import static java.util.stream.Collectors.joining;

/**
 * OracleVisitor
 *
 * @author panguanjing
 * @date 2021/7/24
 */
public class OracleVisitor extends FastModelVisitor {
    private final OracleContext oracleContext;

    public OracleVisitor(OracleContext context) {
        oracleContext = context;
    }

    @Override
    public Boolean visitCreateTable(CreateTable node, Integer indent) {
        boolean columnEmpty = node.isColumnEmpty();
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
        String elementIndent = indentString(indent + 1);
        if (!columnEmpty) {
            builder.append(" (\n");
            String columnList = formatColumnList(node.getColumnDefines(), elementIndent);
            builder.append(columnList);
            if (!node.isPartitionEmpty()) {
                builder.append(",\n");
                columnList = formatColumnList(node.getPartitionedBy().getColumnDefinitions(), elementIndent);
                builder.append(columnList);
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
        builder.append(";");
        if (node.getComment() != null) {
            builder.append("\n");
            builder.append(commentTable(getCode(node.getQualifiedName()), node.getCommentValue()));
        } else {
            if (node.getAliasedNameValue() != null) {
                builder.append("\n");
                builder.append(commentTable(getCode(node.getQualifiedName()), node.getAliasedNameValue()));
            }
        }
        if (!columnEmpty) {
            List<String> commentString = Lists.newArrayList();
            for (ColumnDefinition columnDefinition : node.getColumnDefines()) {
                if (columnDefinition.getComment() != null) {
                    String str = commentColumn(getCode(node.getQualifiedName()),
                        columnDefinition.getColName().getValue(),
                        columnDefinition.getCommentValue());
                    commentString.add(str);
                } else if (columnDefinition.getAliasValue() != null) {
                    String str = commentColumn(getCode(node.getQualifiedName()),
                        columnDefinition.getColName().getValue(),
                        columnDefinition.getAliasValue());
                    commentString.add(str);
                }
            }
            if (!commentString.isEmpty()) {
                builder.append("\n");
                builder.append(commentString.stream().collect(joining(";\n")));
            }

        }
        return executable;
    }

    @Override
    public Boolean visitSetTableComment(SetTableComment setTableComment, Integer context) {
        builder.append(
            commentTable(getCode(setTableComment.getQualifiedName()), setTableComment.getComment().getComment()));
        return true;
    }

    @Override
    public Boolean visitSetColComment(SetColComment setColComment, Integer context) {
        builder.append(
            commentColumn(getCode(setColComment.getQualifiedName()), setColComment.getChangeColumn().getValue(),
                setColComment.getComment().getComment()));
        return true;
    }

    @Override
    protected BaseDataType convert(BaseDataType dataType) {
        return super.convert(dataType);
    }

    @Override
    public Boolean visitAddCols(AddCols addCols, Integer context) {
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
        return true;
    }

    @Override
    public Boolean visitDropTable(DropTable dropTable, Integer context) {
        builder.append("DROP TABLE IF EXISTS ").append(getCode(dropTable.getQualifiedName()));
        return true;
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
        } else {
            if (BooleanUtils.isFalse(column.getNotNull())) {
                sb.append(" NULL");
            }
        }
        return sb.toString();
    }

    private StringBuilder appendNameAndType(ColumnDefinition column, Integer max) {
        BaseDataType dataType = column.getDataType();
        StringBuilder sb = new StringBuilder()
            .append(formatColName(column.getColName(), max))
            .append(" ").append(convert(dataType));
        return sb;
    }

    private void appendConstraint(CreateTable node, Integer indent) {
        Iterator<BaseConstraint> iterator = node.getConstraintStatements().iterator();
        while (iterator.hasNext()) {
            BaseConstraint next = iterator.next();
            //hologres只有primary key定义
            builder.append(",\n");
            process(next, indent + 1);
        }
    }

    private String callSetProperty(String code, String key, String value) {
        String format = "CALL SET_TABLE_PROPERTY('%s', '%s', '%s');\n";
        return String.format(format, code, key, value);
    }

    private String commentTable(String code, String comment) {
        String format = "COMMENT ON TABLE %s IS '%s';";
        return String.format(format, code, comment);
    }

    private String commentColumn(String code, String column, String comment) {
        String format = "COMMENT ON COLUMN %s.%s IS '%s';";
        return String.format(format, code, column, comment);
    }

    @Override
    protected String formatExpression(BaseExpression baseExpression) {
        return new DefaultExpressionVisitor().process(baseExpression);
    }
}
