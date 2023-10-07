/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.hologres.parser.visitor;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
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
import com.aliyun.fastmodel.core.tree.expr.literal.BaseLiteral;
import com.aliyun.fastmodel.core.tree.statement.CompositeStatement;
import com.aliyun.fastmodel.core.tree.statement.table.AddCols;
import com.aliyun.fastmodel.core.tree.statement.table.AddConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.AddPartitionCol;
import com.aliyun.fastmodel.core.tree.statement.table.ChangeCol;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.DropCol;
import com.aliyun.fastmodel.core.tree.statement.table.DropPartitionCol;
import com.aliyun.fastmodel.core.tree.statement.table.DropTable;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableComment;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableProperties;
import com.aliyun.fastmodel.core.tree.statement.table.UnSetTableProperties;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.PrimaryConstraint;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.api.util.StringJoinUtil;
import com.aliyun.fastmodel.transform.hologres.client.converter.HologresPropertyConverter;
import com.aliyun.fastmodel.transform.hologres.context.HologresTransformContext;
import com.aliyun.fastmodel.transform.hologres.dialect.HologresVersion;
import com.aliyun.fastmodel.transform.hologres.parser.tree.BeginWork;
import com.aliyun.fastmodel.transform.hologres.parser.tree.CommitWork;
import com.aliyun.fastmodel.transform.hologres.parser.util.BuilderUtil;
import com.aliyun.fastmodel.transform.hologres.parser.util.HologresReservedWordUtil;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;

import static java.util.stream.Collectors.joining;

/**
 * 遍历node节点操作处理
 *
 * @author panguanjing
 * @date 2021/4/15
 */
public class HologresAstVisitor extends FastModelVisitor implements HologresVisitor<Boolean, Integer> {

    private final HologresTransformContext context;

    private final HologresVersion hologresVersion;

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
        if (!node.isPartitionEmpty()) {
            builder.append(" PARTITION BY LIST(").append(
                node.getPartitionedBy().getColumnDefinitions().stream().map(x -> formatExpression(x.getColName()))
                    .collect(joining(","))).append(")");
        }
        builder.append(";\n");
        List<Property> properties = node.getProperties();
        if (node.isPropertyEmpty()) {
            builder.append(callSetProperty(tableCode, "orientation", context.getOrientation()));
            builder.append("\n");
            builder.append(callSetProperty(tableCode, "time_to_live_in_seconds", String.valueOf(context.getTimeToLiveInSeconds())));
        } else {
            String propertiesValue = buildSetProperties(node.getQualifiedName(), properties);
            builder.append(propertiesValue);
        }
        if (node.getComment() != null) {
            builder.append("\n");
            builder.append(commentTable(tableCode, node.getCommentValue()));
        }
        if (!columnEmpty) {
            for (ColumnDefinition columnDefinition : columnDefines) {
                if (columnDefinition.getComment() == null) {
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
            primaryConstraint.getColNames().stream().map(
                c -> formatColName(c, 0)
            ).collect(joining(",")));
        builder.append(")");
        return true;
    }

    private String callSetProperty(String code, String key, String value) {
        HologresPropertyConverter instance = HologresPropertyConverter.getInstance();
        if (!instance.isValidProperty(key)) {
            return StringUtils.EMPTY;
        }
        BaseClientProperty baseClientProperty = instance.create(key, value);
        //https://help.aliyun.com/document_detail/160754.html?spm=a2c4g.467951.0.0.12c01598oEHfQG
        List<String> list = baseClientProperty.toColumnList();
        if (!list.isEmpty()) {
            if (hologresVersion == HologresVersion.V2) {
                //按照2.0的方式
                String[] strings = list.stream().map(c -> StripUtils.addDoubleStrip(c)).collect(Collectors.toList()).toArray(new String[0]);
                String[] searchList = list.toArray(new String[0]);
                value = StringUtils.replaceEach(value, searchList, strings);
            } else {
                //默认按照1.0的方式
                value = StripUtils.addDoubleStrip(value);
            }
        }
        //将code中的双引号去除
        String strip = StripUtils.removeDoubleStrip(code);
        //如果是列属性，那么
        String format = "CALL SET_TABLE_PROPERTY('%s', '%s', '%s');";
        return String.format(format, strip, key, value);
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
        return new StringBuilder()
            .append(formatColName(column.getColName(), max))
            .append(" ")
            .append(formatExpression(convert(dataType)));
    }

    @Override
    protected String formatColName(Identifier colName, Integer size) {
        String value = StringUtils.isNotBlank(colName.getOrigin()) ?
            StripUtils.strip(colName.getOrigin()) : colName.getValue();
        if (!colName.isDelimited()) {
            boolean reservedKeyWord = HologresReservedWordUtil.isReservedKeyWord(value);
            //如果node是关键字，那么进行转义处理
            if (reservedKeyWord) {
                value = StripUtils.addDoubleStrip(value);
            } else if (context.isCaseSensitive()) {
                //如果是不忽略大小写，那么统一加上双引号
                value = StripUtils.addDoubleStrip(value);
            }
        } else {
            String strip = StripUtils.strip(value);
            value = StripUtils.addDoubleStrip(strip);
        }
        return StringUtils.rightPad(value, size);
    }

    @Override
    protected BaseDataType convert(BaseDataType dataType) {
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
        BuilderUtil.addTransaction(builder, () ->{
            return commentTable(getCode(setTableComment.getQualifiedName()), setTableComment.getComment().getComment());
        });
        return true;
    }

    @Override
    public Boolean visitSetTableProperties(SetTableProperties setTableProperties, Integer context) {
        List<Property> propertyList = setTableProperties.getProperties().stream().filter(
            p -> HologresPropertyConverter.getInstance().isValidProperty(p.getName())
        ).collect(Collectors.toList());
        if (propertyList.isEmpty()) {
            return false;
        }
        BuilderUtil.addTransaction(builder, () -> {
            List<Property> properties = propertyList;
            return buildSetProperties(setTableProperties.getQualifiedName(), properties);
        });
        return true;
    }

    private String buildSetProperties(QualifiedName qualifiedName, List<Property> properties) {
        StringBuilder stringBuilder = new StringBuilder();
        Iterator<Property> iterator = properties.iterator();
        if (iterator.hasNext()) {
            Property p = iterator.next();
            String value = callSetProperty(getCode(qualifiedName), p.getName(), p.getValue());
            stringBuilder.append(value);
            while (iterator.hasNext()) {
                p = iterator.next();
                String str = callSetProperty(getCode(qualifiedName), p.getName(), p.getValue());
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
    public Boolean visitChangeCol(ChangeCol changeCol, Integer context) {
        Identifier oldColName = changeCol.getOldColName();
        Identifier newColName = changeCol.getNewColName();
        String code = getCode(changeCol.getQualifiedName());
        List<String> changeValue = Lists.newArrayList();
        if (Objects.equals(oldColName, newColName)) {
            BaseLiteral defaultValue = changeCol.getDefaultValue();
            if (defaultValue != null) {
                String builder = "ALTER TABLE " + code
                    + " ALTER COLUMN " + formatColName(oldColName, 0)
                    + " SET DEFAULT " + formatExpression(defaultValue)
                    + ";";
                changeValue.add(builder);
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
        //hologres的2.x版本不支持3段式的创建，1.x支持，为了兼容统一采用2段式的创建
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
}
