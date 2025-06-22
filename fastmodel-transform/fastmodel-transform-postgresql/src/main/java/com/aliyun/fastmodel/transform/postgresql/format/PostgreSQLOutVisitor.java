package com.aliyun.fastmodel.transform.postgresql.format;

import java.util.List;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.core.formatter.FastModelVisitor;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.statement.table.AddCols;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.PrimaryConstraint;
import com.aliyun.fastmodel.transform.api.extension.client.property.ExtensionPropertyKey;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.desc.NonKeyConstraint;
import com.aliyun.fastmodel.transform.postgresql.context.PostgreSQLTransformContext;
import com.aliyun.fastmodel.transform.postgresql.parser.visitor.PostgreSQLExpressionVisitor;
import com.aliyun.fastmodel.transform.postgresql.parser.visitor.PostgreSQLVisitor;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import static java.util.stream.Collectors.joining;

/**
 * PostgreSQLOutVisitor
 *
 * @author panguanjing
 * @date 2024/10/20
 */
public class PostgreSQLOutVisitor extends FastModelVisitor implements PostgreSQLVisitor<Boolean, Integer> {

    @Override
    public Boolean visitCreateTable(CreateTable node, Integer indent) {
        boolean columnEmpty = node.isColumnEmpty();
        boolean executable = !columnEmpty;
        builder.append("BEGIN;\n");
        builder.append("CREATE TABLE ");
        if (node.isNotExists()) {
            builder.append("IF NOT EXISTS ");
        }
        String tableCode = getCode(node.getQualifiedName());
        builder.append(tableCode);
        String elementIndent = indentString(indent + 1);
        List<ColumnDefinition> columnDefines = node.getColumnDefines();
        if (!columnEmpty) {
            builder.append(" (\n");
            String columnList = formatColumnList(columnDefines, elementIndent);
            builder.append(columnList);
            if (!node.isConstraintEmpty()) {
                appendConstraint(node, indent);
            }
            builder.append("\n").append(")");
        }
        appendWithClause(node);
        if (!node.isConstraintEmpty()) {
            List<BaseConstraint> baseConstraints = node.getConstraintStatements().stream()
                .filter(c -> c instanceof NonKeyConstraint).collect(Collectors.toList());
            for (BaseConstraint constraint : baseConstraints) {
                builder.append("\n");
                process(constraint, indent);
            }
        }
        if (!node.isPartitionEmpty()) {
            if (!isEndNewLine(builder.toString())) {
                builder.append("\n");
            }
            process(node.getPartitionedBy(), indent);
        }
        builder.append(";\n");
        builder.append("COMMIT;");
        return executable;
    }

    @Override
    protected String formatColumnDefinition(ColumnDefinition column, Integer max) {
        StringBuilder sb = appendNameAndType(column, max);
        if (column.getDataType() == null) {
            //如果没有数据类型，那么只返回名称即可
            return sb.toString();
        }
        boolean isPrimary = column.getPrimary() != null && column.getPrimary();
        if (isPrimary) {
            sb.append(" PRIMARY KEY");
        }
        boolean isNotNull = column.getNotNull() != null && column.getNotNull();
        if (!isPrimary && isNotNull) {
            sb.append(" NOT NULL");
        }
        if (column.getDefaultValue() != null) {
            String defaultValue = formatExpression(column.getDefaultValue());
            sb.append(" DEFAULT ").append(defaultValue);
        }
        if (column.getColumnProperties() != null && !column.getColumnProperties().isEmpty()) {
            Property property = column.getColumnProperties().stream().filter(
                p -> StringUtils.equalsIgnoreCase(p.getName(), ExtensionPropertyKey.COLUMN_CHECK.getValue())).findFirst().orElse(null);
            if (property != null) {
                sb.append(" CHECK ").append("(").append(property.getValue()).append(")");
            }
        }
        return sb.toString();
    }

    private StringBuilder appendNameAndType(ColumnDefinition column, Integer max) {
        BaseDataType dataType = column.getDataType();
        BaseDataType convert = convert(dataType);
        String colName = formatColName(column.getColName(), max);
        if (convert == null) {
            return new StringBuilder().append(colName);
        }
        String expression = formatExpression(convert);
        return new StringBuilder()
            .append(colName)
            .append(" ")
            .append(expression);
    }

    protected void appendConstraint(CreateTable node, Integer indent) {
        for (BaseConstraint next : node.getConstraintStatements()) {
            if (next instanceof NonKeyConstraint) {
                continue;
            }
            builder.append(",\n");
            process(next, indent + 1);
        }
    }

    protected void appendWithClause(CreateTable node) {
        if (CollectionUtils.isEmpty(node.getProperties())) {
            return;
        }
        if (!isEndNewLine(builder.toString())) {
            builder.append(StringUtils.LF);
        }
        builder.append("WITH ( ");
        String properties = node.getProperties().stream().map(p -> String.format("%s = %s", p.getName(), p.getValue())).collect(joining(","));
        builder.append(properties);
        builder.append(" )");
    }

    @Override
    public Boolean visitPrimaryConstraint(PrimaryConstraint primaryConstraint, Integer indent) {
        builder.append(indentString(indent)).append("PRIMARY KEY(");
        builder.append(
            primaryConstraint.getColNames().stream().map(
                c -> formatColName(c, 0)
            ).collect(joining(",")));
        builder.append(")");
        return true;
    }

    @Override
    public Boolean visitAddCols(AddCols addCols, Integer indent) {
        builder.append("ALTER TABLE ").append(getCode(addCols.getQualifiedName()));
        String columnList = addCols.getColumnDefineList().stream()
            .map(element -> " ADD COLUMN " + appendNameAndType(element, 0)).collect(joining(","));
        builder.append(columnList).append(";");
        for (ColumnDefinition columnDefinition : addCols.getColumnDefineList()) {
            if (columnDefinition.getCommentValue() != null) {
                builder.append("\n");
                builder.append(
                    commentColumn(getCode(addCols.getQualifiedName()),
                        formatColName(columnDefinition.getColName(), 0),
                        columnDefinition.getCommentValue()));
            }
        }
        return true;
    }

    @Override
    protected String formatExpression(BaseExpression baseExpression) {
        return new PostgreSQLExpressionVisitor(PostgreSQLTransformContext.builder().build()).process(baseExpression);
    }

    private String commentColumn(String code, String column, String comment) {
        String format = "COMMENT ON COLUMN %s.%s IS %s;";
        if (comment == null) {
            return StringUtils.EMPTY;
        }
        return String.format(format, code, column, formatStringLiteral(comment));
    }
}
