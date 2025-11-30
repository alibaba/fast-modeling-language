/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.hologres.format;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.common.utils.StripUtils;
import com.aliyun.fastmodel.core.formatter.FastModelVisitor;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.datatype.GenericDataType;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName.Dimension;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.CompositeStatement;
import com.aliyun.fastmodel.core.tree.statement.table.AddCols;
import com.aliyun.fastmodel.core.tree.statement.table.AddConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.AddPartitionCol;
import com.aliyun.fastmodel.core.tree.statement.table.ChangeCol;
import com.aliyun.fastmodel.core.tree.statement.table.ChangeCol.ChangeType;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.DropCol;
import com.aliyun.fastmodel.core.tree.statement.table.DropPartitionCol;
import com.aliyun.fastmodel.core.tree.statement.table.DropTable;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.core.tree.statement.table.RenameTable;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableComment;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableProperties;
import com.aliyun.fastmodel.core.tree.statement.table.UnSetTableProperties;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.PrimaryConstraint;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.aliyun.fastmodel.core.tree.util.PropertyUtil;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.LogicalPartitionedBy;
import com.aliyun.fastmodel.transform.api.util.StringJoinUtil;
import com.aliyun.fastmodel.transform.hologres.client.property.HologresPropertyKey;
import com.aliyun.fastmodel.transform.hologres.context.HologresTransformContext;
import com.aliyun.fastmodel.transform.hologres.dialect.HologresVersion;
import com.aliyun.fastmodel.transform.hologres.parser.tree.BeginWork;
import com.aliyun.fastmodel.transform.hologres.parser.tree.CommitWork;
import com.aliyun.fastmodel.transform.hologres.parser.tree.datatype.HologresDataTypeName;
import com.aliyun.fastmodel.transform.hologres.parser.util.BuilderUtil;
import com.aliyun.fastmodel.transform.hologres.parser.util.HologresPropertyUtil;
import com.aliyun.fastmodel.transform.hologres.parser.visitor.HologresVisitor;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

import static java.util.stream.Collectors.joining;

/**
 * 遍历node节点操作处理
 *
 * @author panguanjing
 * @date 2021/4/15
 */
public class HologresAstVisitor extends FastModelVisitor implements HologresVisitor<Boolean, Integer> {

    /**
     * transform context
     */
    private final HologresTransformContext context;

    /**
     * hologres version
     */
    private final HologresVersion hologresVersion;

    /**
     * 用于修改动态表sql的前后缀
     */
    private static final String TASK_DEFINITION_STUB = "$_dataworks_system_$";

    public HologresAstVisitor(HologresTransformContext context, HologresVersion hologresVersion) {
        this.context = context;
        this.hologresVersion = hologresVersion;
    }

    public HologresAstVisitor(HologresTransformContext context) {
        this(context, HologresVersion.V1);
    }

    @Override
    public Boolean visitCompositeStatement(CompositeStatement compositeStatement, Integer context) {
        return super.visitCompositeStatement(compositeStatement, context);
    }

    @Override
    public Boolean visitCreateTable(CreateTable node, Integer indent) {
        Boolean foreignTable = isForeignTable(node);
        Boolean dynamicTable = isDynamicTable(node);
        if (foreignTable) {
            return visitCreateForeignTable(node, indent);
        } else if (dynamicTable) {
            return visitDynamicTable(node, indent);
        } else {
            return visitCreateInnerTable(node, indent);
        }
    }

    private Boolean visitDynamicTable(CreateTable node, Integer indent) {
        builder.append("BEGIN;\n");
        printExtensionIfNeed(node);
        builder.append("CREATE DYNAMIC TABLE ");
        if (node.isNotExists()) {
            builder.append("IF NOT EXISTS ");
        }
        String tableCode = getCode(node.getQualifiedName());
        builder.append(tableCode);
        String elementIndent = indentString(indent + 1);
        boolean columnEmpty = node.isColumnEmpty();
        if (!columnEmpty) {
            builder.append(" (\n");
            String column = formatColumnList(node.getColumnDefines(), elementIndent);
            builder.append(column);
            builder.append("\n").append(")");
        }
        if (!node.isPartitionEmpty()) {
            builder.append(" PARTITION BY LIST(").append(
                node.getPartitionedBy().getColumnDefinitions().stream().map(x -> formatExpression(x.getColName()))
                    .collect(joining(","))).append(")");
        }
        // with
        List<Property> properties = node.getProperties();
        List<String> excludeKeys = Lists.newArrayList(
            HologresPropertyKey.DYNAMIC.getValue(),
            HologresPropertyKey.TASK_DEFINITION.getValue()
        );
        String p = formatProperty(elementIndent, properties.stream().filter(
            prop -> !excludeKeys.contains(prop.getName())
        ).collect(Collectors.toList()));
        if (StringUtils.isNotBlank(p)) {
            builder.append("\nWITH (\n");
            builder.append(p);
            builder.append("\n)");
        }
        String sql = PropertyUtil.getPropertyValue(properties, HologresPropertyKey.TASK_DEFINITION.getValue());
        // query
        if (StringUtils.isNotBlank(sql)) {
            builder.append(" AS\n");
            builder.append(sql);
            if (!sql.endsWith(";")) {
                builder.append(";");
            }
        }
        builder.append("\n");
        builder.append("COMMIT;");
        if (node.getCommentValue() != null) {
            builder.append("\n");
            BuilderUtil.addTransaction(builder, () -> commentTable(tableCode, node.getCommentValue()));
        }
        return true;
    }

    private String formatProperty(String indent, List<Property> properties) {
        if (properties == null || properties.isEmpty()) {
            return StringUtils.EMPTY;
        }
        return properties.stream().map(
            x -> indent + x.getName() + "=" + formatStringLiteral(x.getValue())
        ).collect(joining(",\n"));
    }

    private Boolean isDynamicTable(CreateTable node) {
        if (node == null) {
            return false;
        }
        List<Property> properties = node.getProperties();
        if (CollectionUtils.isEmpty(properties)) {
            return false;
        }
        return properties.stream().anyMatch(p -> StringUtils.equalsIgnoreCase(p.getName(), HologresPropertyKey.DYNAMIC.getValue()));
    }

    private Boolean visitCreateInnerTable(CreateTable node, Integer indent) {
        boolean columnEmpty = node.isColumnEmpty();
        boolean executable = !columnEmpty;
        builder.append("BEGIN;\n");
        printExtensionIfNeed(node);
        builder.append("CREATE TABLE ");
        if (node.isNotExists()) {
            builder.append("IF NOT EXISTS ");
        }
        String tableCode = getCode(node.getQualifiedName());
        builder.append(tableCode);
        String elementIndent = indentString(indent + 1);
        PartitionedBy partitionedBy = node.getPartitionedBy();
        List<ColumnDefinition> columnDefines = merge(node.getColumnDefines(), partitionedBy);
        if (!columnEmpty) {
            builder.append(" (\n");
            String columnList = formatColumnList(columnDefines, elementIndent);
            builder.append(columnList);
            if (!node.isConstraintEmpty()) {
                appendConstraint(node, indent);
            }
            builder.append("\n").append(")");
        }
        if (!node.isPartitionEmpty()) {
            if (partitionedBy instanceof LogicalPartitionedBy) {
                builder.append("LOGICAL");
            }
            builder.append(" PARTITION BY LIST(").append(
                partitionedBy.getColumnDefinitions().stream().map(x -> formatExpression(x.getColName()))
                    .collect(joining(","))).append(")");
        }
        builder.append(";\n");
        List<Property> properties = node.getProperties();
        if (CollectionUtils.isNotEmpty(properties)) {
            String propertiesValue = buildSetProperties(node.getQualifiedName(), properties);
            builder.append(propertiesValue);
        }
        if (node.getComment() != null && node.getComment().getComment() != null) {
            builder.append(commentTable(tableCode, node.getCommentValue()));
        }
        if (!columnEmpty) {
            for (ColumnDefinition columnDefinition : columnDefines) {
                if (columnDefinition.getComment() == null || columnDefinition.getComment().getComment() == null) {
                    continue;
                }
                builder.append("\n");
                builder.append(commentColumn(tableCode, formatColName(columnDefinition.getColName(), 0), columnDefinition.getCommentValue()));
            }
        }
        builder.append("\n");
        builder.append("COMMIT;");
        return executable;
    }

    private void printExtensionIfNeed(CreateTable node) {
        if (node.isColumnEmpty()) {
            return;
        }
        Optional<ColumnDefinition> first = node.getColumnDefines().stream()
            .filter(c -> c.getDataType() != null)
            .filter(c -> StringUtils.equalsIgnoreCase(c.getDataType().getTypeName().getValue(), HologresDataTypeName.ROARING_BITMAP.getValue()))
            .findFirst();
        if (!first.isPresent()) {
            return;
        }
        builder.append("CREATE EXTENSION IF NOT EXISTS ROARINGBITMAP;\n");
    }

    private Boolean visitCreateForeignTable(CreateTable node, Integer indent) {
        boolean columnEmpty = node.isColumnEmpty();
        boolean executable = !columnEmpty;
        builder.append("BEGIN;\n");
        builder.append("CREATE FOREIGN TABLE ");
        if (node.isNotExists()) {
            builder.append("IF NOT EXISTS ");
        }
        String tableCode = getCode(node.getQualifiedName());
        builder.append(tableCode);
        String elementIndent = indentString(indent + 1);
        List<ColumnDefinition> columnDefines = merge(node.getColumnDefines(), node.getPartitionedBy());
        if (!columnEmpty) {
            builder.append(" (\n");
            String columnList = formatColumnList(columnDefines, elementIndent);
            builder.append(columnList);
            if (!node.isConstraintEmpty()) {
                appendConstraint(node, indent);
            }
            builder.append("\n").append(")");
        }
        String serverAndOptions = buildServerAndOptions(node);
        builder.append(serverAndOptions);
        builder.append(";\n");
        builder.append("COMMIT;");
        return executable;
    }

    private List<ColumnDefinition> merge(List<ColumnDefinition> columnDefines, PartitionedBy partitionedBy) {
        if (partitionedBy == null || !partitionedBy.isNotEmpty()) {
            return columnDefines;
        }
        List<ColumnDefinition> list = Lists.newArrayList(columnDefines);
        List<ColumnDefinition> partitionedByColumnDefinitions = partitionedBy.getColumnDefinitions();
        for (ColumnDefinition columnDefinition : partitionedByColumnDefinitions) {
            if (contains(list, columnDefinition)) {
                continue;
            }
            list.add(columnDefinition);
        }
        return list;
    }

    private boolean contains(List<ColumnDefinition> list, ColumnDefinition columnDefinition) {
        return list.stream().anyMatch(definition -> Objects.equals(definition.getColName(), columnDefinition.getColName()));
    }

    private void appendConstraint(CreateTable node, Integer indent) {
        for (BaseConstraint next : node.getConstraintStatements()) {
            // hologres只有primary key定义
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
            primaryConstraint.getColNames().stream().map(
                c -> formatColName(c, 0)
            ).collect(joining(",")));
        builder.append(")");
        return true;
    }

    private String callSetProperty(QualifiedName code, String key, String value) {
        // 增加校验，判断是否支持print，只有明确定义了不支持print
        HologresPropertyKey propertyKey = HologresPropertyKey.getByValue(key);
        if (propertyKey != null && !propertyKey.isSupportPrint()) {
            return null;
        }
        String result = HologresPropertyUtil.getPropertyValue(hologresVersion, key, value);
        // 因为hologres没有三段氏，所以这里不再删除schema的双引号
        List<Identifier> originalParts = code.getOriginalParts();
        String schema = null;
        String tableName = null;
        if (originalParts.size() == 3) {
            schema = formatExpression(originalParts.get(1));
            tableName = formatExpression(originalParts.get(2));
        } else if (originalParts.size() == 2) {
            schema = formatExpression(originalParts.get(0));
            tableName = formatExpression(originalParts.get(1));
        } else if (originalParts.size() == 1) {
            tableName = formatExpression(originalParts.get(0));
        }
        // 如果是列属性
        String format = "CALL SET_TABLE_PROPERTY('%s', '%s', '%s');";
        if (schema == null) {
            return String.format(format, tableName, key, result);
        } else {
            return String.format(format, schema + "." + tableName, key, result);
        }
    }

    private String commentTable(String code, String comment) {
        String format = "COMMENT ON TABLE %s IS %s;";
        if (comment == null) {
            return String.format(format, code, "NULL");
        }
        return String.format(format, code, formatStringLiteral(comment));
    }

    private String commentColumn(String code, String column, String comment) {
        String format = "COMMENT ON COLUMN %s.%s IS %s;";
        if (comment == null) {
            return String.format(format, code, column, "NULL");
        }
        return String.format(format, code, column, formatStringLiteral(comment));
    }

    @Override
    protected String formatColumnDefinition(ColumnDefinition column, Integer max) {
        StringBuilder sb = appendNameAndType(column, max);
        if (column.getDataType() == null) {
            // 如果没有数据类型，那么只返回名称即可
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
            sb.append(" DEFAULT ").append(formatExpression(column.getDefaultValue()));
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

    @Override
    protected String formatColName(Identifier colName, Integer size) {
        String value = new HologresExpressionVisitor(context).visitIdentifier(colName, null);
        return StringUtils.rightPad(value, size);
    }

    @Override
    protected BaseDataType convert(BaseDataType dataType) {
        if (dataType == null) {
            return null;
        }
        IDataTypeName typeName = dataType.getTypeName();
        if (StringUtils.equalsIgnoreCase(typeName.getValue(), DataTypeEnums.STRING.getValue())) {
            return new GenericDataType(DataTypeEnums.TEXT.name());
        } else if (StringUtils.equalsIgnoreCase(typeName.getValue(), DataTypeEnums.DATETIME.getValue())) {
            return DataTypeUtil.simpleType(DataTypeEnums.TIMESTAMP);
        } else if (typeName.getDimension() == Dimension.MULTIPLE) {
            return DataTypeUtil.simpleType(DataTypeEnums.JSON);
        }
        return dataType;
    }

    @Override
    public Boolean visitAddCols(AddCols addCols, Integer context) {
        BuilderUtil.addTransaction(builder, () -> {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("ALTER TABLE IF EXISTS ").append(getCode(addCols.getQualifiedName()));
            String columnList = addCols.getColumnDefineList().stream()
                .map(element -> " ADD COLUMN " + appendNameAndType(element, 0)).collect(joining(","));
            stringBuilder.append(columnList).append(";");
            for (ColumnDefinition columnDefinition : addCols.getColumnDefineList()) {
                if (columnDefinition.getCommentValue() != null) {
                    stringBuilder.append("\n");
                    stringBuilder.append(
                        commentColumn(getCode(addCols.getQualifiedName()),
                            formatColName(columnDefinition.getColName(), 0),
                            columnDefinition.getCommentValue()));
                }
            }
            return stringBuilder.toString();
        });
        return true;
    }

    @Override
    public Boolean visitDropTable(DropTable dropTable, Integer context) {
        builder.append("DROP TABLE IF EXISTS ").append(getCode(dropTable.getQualifiedName()));
        return true;
    }

    @Override
    public Boolean visitSetTableComment(SetTableComment setTableComment, Integer context) {
        BuilderUtil.addTransaction(builder, () -> {
            return commentTable(getCode(setTableComment.getQualifiedName()), setTableComment.getComment().getComment());
        });
        return true;
    }

    @Override
    public Boolean visitSetTableProperties(SetTableProperties setTableProperties, Integer indent) {
        List<Property> propertyList = setTableProperties.getProperties().stream().filter(
            p -> {
                HologresPropertyKey byValue = HologresPropertyKey.getByValue(p.getName());
                return byValue != null && byValue.isSupportPrint();
            }
        ).collect(Collectors.toList());
        if (propertyList.isEmpty()) {
            return false;
        }
        boolean useAlterTableSetSentence = this.context.isUseAlterTableSetSentence();
        if (useAlterTableSetSentence) {
            BuilderUtil.addTransaction(builder, () -> buildSetPropertiesUseAlter(setTableProperties, propertyList));
        } else {
            QualifiedName tableName = StringJoinUtil.join(
                null,
                this.context.getSchema(),
                setTableProperties.getQualifiedName().getSuffix()
            );
            BuilderUtil.addTransaction(builder, () -> buildSetProperties(tableName, propertyList));
        }
        return true;
    }

    private String buildSetPropertiesUseAlter(SetTableProperties setTableProperties, List<Property> propertyList) {
        // ALTER TABLE <schema_name>.<table_name> SET (dictionary_encoding_columns = '[columnName{:[on|off|auto]}[,...]]');
        // 针对Task_definition进行单独设置
        String prefix = "ALTER TABLE " + getCode(setTableProperties.getQualifiedName())
            + " SET ";
        String propertyValue = PropertyUtil.getPropertyValue(propertyList, HologresPropertyKey.TASK_DEFINITION.getValue());
        List<String> list = Lists.newArrayList();
        if (propertyValue != null) {
            String taskDefinitionBuilder = prefix + HologresPropertyKey.TASK_DEFINITION.getValue() + " = "
                + TASK_DEFINITION_STUB
                + StringUtils.LF
                + propertyValue
                + StringUtils.LF
                + TASK_DEFINITION_STUB + ";";
            list.add(taskDefinitionBuilder);
        }
        String value = propertyList.stream().filter(p -> {
            return !StringUtils.equalsIgnoreCase(p.getName(), HologresPropertyKey.TASK_DEFINITION.getValue());
        }).map(p -> {
            String result = HologresPropertyUtil.getPropertyValue(hologresVersion, p.getName(), p.getValue());
            // 将code中的双引号去除
            return p.getName() + "=" + StripUtils.addStrip(result);
        }).collect(joining(",", "(", ")"));
        list.add(prefix + value + ";");
        return String.join("\n", list);
    }

    private String buildSetProperties(QualifiedName qualifiedName, List<Property> properties) {
        StringBuilder stringBuilder = new StringBuilder();
        Iterator<Property> iterator = properties.iterator();
        if (iterator.hasNext()) {
            Property p = iterator.next();
            String value = callSetProperty(qualifiedName, p.getName(), p.getValue());
            stringBuilder.append(value);
            while (iterator.hasNext()) {
                p = iterator.next();
                String str = callSetProperty(qualifiedName, p.getName(), p.getValue());
                if (StringUtils.isBlank(str)) {
                    continue;
                }
                stringBuilder.append("\n");
                stringBuilder.append(str);
            }
        }
        return stringBuilder.toString();
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
    public Boolean visitRenameTable(RenameTable renameTable, Integer context) {
        builder.append("ALTER TABLE ");
        builder.append(getCode(renameTable.getQualifiedName()));
        builder.append(" RENAME TO ").append(renameTable.getTarget().getSuffix());
        return true;
    }

    @Override
    public Boolean visitChangeCol(ChangeCol changeCol, Integer context) {
        Identifier oldColName = changeCol.getOldColName();
        Identifier newColName = changeCol.getNewColName();
        String code = getCode(changeCol.getQualifiedName());
        List<String> changeValue = Lists.newArrayList();
        if (Objects.equals(oldColName, newColName)) {
            BaseExpression defaultValue = changeCol.getDefaultValue();
            if (defaultValue != null) {
                String builder = "ALTER TABLE " + code
                    + " ALTER COLUMN " + formatColName(oldColName, 0)
                    + " SET DEFAULT " + formatExpression(defaultValue)
                    + ";";
                changeValue.add(builder);
            }
            // 如果改了默认值
            if (changeCol.change(ChangeType.DEFAULT_VALUE)) {
                if (defaultValue == null) {
                    String builder = "ALTER TABLE " + code
                        + " ALTER COLUMN " + formatColName(oldColName, 0)
                        + " DROP DEFAULT;";
                    changeValue.add(builder);
                }
            }
            Comment comment = changeCol.getColumnDefinition().getComment();
            if (comment != null) {
                StringBuilder builder = new StringBuilder();
                String commentColumn = commentColumn(code, formatColName(newColName, 0), comment.getComment());
                builder.append(commentColumn);
                changeValue.add(builder.toString());
            }
        } else {
            String builder = "ALTER TABLE " + code
                + " RENAME COLUMN " + formatExpression(oldColName)
                + " TO " + formatExpression(newColName)
                + ";";
            changeValue.add(builder);
        }
        if (changeValue.isEmpty()) {
            super.visitChangeCol(changeCol, context);
            return false;
        }
        String join = Joiner.on("\n").join(changeValue);
        BuilderUtil.addTransaction(builder, () -> join);
        return true;
    }

    @Override
    protected String formatExpression(BaseExpression baseExpression) {
        return new HologresExpressionVisitor(context).process(baseExpression);
    }

    @Override
    protected String getCode(QualifiedName qualifiedName) {
        // hologres的2.x版本不支持3段式的创建，1.x支持，为了兼容统一采用2段式的创建
        QualifiedName tableName = StringJoinUtil.join(
            null,
            this.context.getSchema(),
            qualifiedName.getSuffix()
        );
        return formatName(tableName);
    }

    @Override
    public Boolean visitBeginWork(BeginWork beginWork, Integer context) {
        builder.append("BEGIN;");
        return true;
    }

    @Override
    public Boolean visitCommitWork(CommitWork commitWork, Integer context) {
        builder.append("COMMIT;");
        return null;
    }

    private Boolean isForeignTable(CreateTable node) {
        if (CollectionUtils.isEmpty(node.getProperties())) {
            return false;
        }
        Optional<Property> foreignTablePropertyOpt = node.getProperties().stream().filter(property ->
            StringUtils.equalsIgnoreCase(HologresPropertyKey.FOREIGN.getValue(), property.getName())
                && BooleanUtils.toBoolean(property.getValue())).findAny();
        return foreignTablePropertyOpt.isPresent();
    }

    private String buildServerAndOptions(CreateTable node) {
        StringBuilder sb = new StringBuilder();
        Optional<Property> serverOpt = node.getProperties().stream().filter(property ->
            StringUtils.equalsIgnoreCase(HologresPropertyKey.SERVER_NAME.getValue(), property.getName())).findAny();
        serverOpt.ifPresent(server -> sb.append("\nSERVER ").append(server.getValue()).append("\n"));

        List<String> options = new ArrayList<>();
        Optional<Property> projectNameOpt = node.getProperties().stream().filter(property ->
            StringUtils.equalsIgnoreCase("project_name", property.getName())).findAny();
        projectNameOpt.ifPresent(projectName -> options.add("project_name " + StripUtils.addStrip(projectName.getValue())));
        Optional<Property> tableNameOpt = node.getProperties().stream().filter(property ->
            StringUtils.equalsIgnoreCase("table_name", property.getName())).findAny();
        tableNameOpt.ifPresent(tableName -> options.add("table_name " + StripUtils.addStrip(tableName.getValue())));
        if (CollectionUtils.isNotEmpty(options)) {
            sb.append("OPTIONS(").append(StringUtils.join(options, ", ")).append(")");
        }
        return sb.toString();
    }
}
