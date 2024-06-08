package com.aliyun.fastmodel.transform.sqlite.format;

import com.aliyun.fastmodel.core.formatter.FastModelVisitor;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.transform.api.datatype.DataTypeConverter;
import com.aliyun.fastmodel.transform.sqlite.context.SqliteContext;
import com.aliyun.fastmodel.transform.sqlite.datatype.Fml2SqliteDataTypeConverter;

/**
 * sqlite visitor
 *
 * @author panguanjing
 * @date 2023/8/14
 */
public class SqliteVisitor extends FastModelVisitor {

    private final SqliteContext sqliteContext;

    private Fml2SqliteDataTypeConverter fml2SqliteDataTypeConverter;

    public SqliteVisitor(SqliteContext context) {
        this.sqliteContext = context;
        this.fml2SqliteDataTypeConverter = new Fml2SqliteDataTypeConverter();
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

            builder.append("\n").append(")");
        }
        return executable;
    }

    @Override
    protected String formatColumnDefinition(ColumnDefinition column, Integer max) {
        BaseDataType dataType = column.getDataType();
        StringBuilder sb = new StringBuilder();
        sb.append(formatColName(column.getColName(), max));
        sb.append(" ").append(formatExpression(convert(dataType)));
        return sb.toString();
    }

    @Override
    protected BaseDataType convert(BaseDataType dataType) {
        if (sqliteContext == null || sqliteContext.getDataTypeTransformer() == null) {
            return fml2SqliteDataTypeConverter.convert(dataType);
        } else {
            DataTypeConverter dataTypeTransformer = sqliteContext.getDataTypeTransformer();
            return dataTypeTransformer.convert(dataType);
        }
    }

    @Override
    protected String formatExpression(BaseExpression baseExpression) {
        return new SqliteExpressionVisitor().process(baseExpression);
    }
}
