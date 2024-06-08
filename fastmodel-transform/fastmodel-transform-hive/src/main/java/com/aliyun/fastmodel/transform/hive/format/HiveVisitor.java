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

package com.aliyun.fastmodel.transform.hive.format;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.common.utils.StripUtils;
import com.aliyun.fastmodel.core.formatter.ExpressionFormatter;
import com.aliyun.fastmodel.core.formatter.FastModelVisitor;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.element.MultiComment;
import com.aliyun.fastmodel.core.tree.statement.insert.Insert;
import com.aliyun.fastmodel.core.tree.statement.table.AddConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.AddPartitionCol;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.DropCol;
import com.aliyun.fastmodel.core.tree.statement.table.DropConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.DropPartitionCol;
import com.aliyun.fastmodel.core.tree.statement.table.RenameCol;
import com.aliyun.fastmodel.core.tree.statement.table.SetColComment;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableAliasedName;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableComment;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableProperties;
import com.aliyun.fastmodel.core.tree.statement.table.UnSetTableProperties;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.ColumnGroupConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.DimConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.LevelConstraint;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.aliyun.fastmodel.transform.api.util.StringJoinUtil;
import com.aliyun.fastmodel.transform.hive.context.HiveTransformContext;
import com.aliyun.fastmodel.transform.hive.parser.util.HiveReservedWordUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

import static com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums.DATETIME;
import static java.util.stream.Collectors.joining;

/**
 * hive 表达式
 *
 * @author panguanjing
 * @date 2021/2/1
 */
public class HiveVisitor extends FastModelVisitor {

    private final HiveTransformContext context;

    private final boolean enableConstraint;

    public HiveVisitor(HiveTransformContext context) {
        this.context = context;
        enableConstraint = context.isEnableConstraint();
    }

    @Override
    public Boolean visitCreateTable(CreateTable node, Integer indent) {
        boolean columnNotEmpty = node.getColumnDefines() != null && !node.getColumnDefines().isEmpty();
        boolean executable = true;
        //hive不支持没有列的表
        if (!columnNotEmpty) {
            executable = false;
        }
        boolean external = HiveHelper.isExternal(node);
        if (external) {
            builder.append("CREATE EXTERNAL TABLE ");
        } else {
            builder.append("CREATE TABLE ");
        }
        if (node.isNotExists()) {
            builder.append("IF NOT EXISTS ");
        }
        String tableName = getCode(node.getQualifiedName());
        builder.append(tableName);

        int newIndent = indent + 1;
        String elementIndent = indentString(newIndent);
        if (columnNotEmpty) {
            builder.append(newLine("("));
            String columnList = formatColumnList(node.getColumnDefines(), elementIndent);
            builder.append(columnList);
            //因为Hive中EMR中不支持constraint的屏蔽处理
            if (context.isEnableConstraint() && node.getConstraintStatements() != null && !node
                .getConstraintStatements().isEmpty()) {
                builder.append(",\n");
                Iterator<BaseConstraint> iterator = node.getConstraintStatements().iterator();
                while (iterator.hasNext()) {
                    process(iterator.next(), newIndent);
                    if (iterator.hasNext()) {
                        builder.append(",\n");
                    }
                }
            }
            builder.append(newLine(")"));
        } else {
            if (!node.isCommentElementEmpty()) {
                builder.append(newLine(COMMENT + "("));
                builder.append(formatCommentElement(node.getColumnCommentElements(), elementIndent));
                builder.append(newLine(COMMENT + ")"));
            }
        }
        if (node.getComment() != null) {
            builder.append(formatComment(node.getComment(), isEndNewLine(builder.toString())));
        }
        if (!node.isPartitionEmpty()) {
            builder.append(
                formatPartitions(
                    node.getPartitionedBy().getColumnDefinitions(),
                    isEndNewLine(builder.toString()),
                    elementIndent)
            );
        }
        //append row format

        String rowFormat = HiveHelper.appendRowFormat(node, elementIndent);
        if (StringUtils.isNotBlank(rowFormat)) {
            appendLineIfNecessary();
            builder.append(rowFormat);
        }
        //append stored format
        String storedFormat = HiveHelper.appendStoredFormat(node);
        if (StringUtils.isNotBlank(storedFormat)) {
            appendLineIfNecessary();
            builder.append(storedFormat);
        }

        //append location
        String location = HiveHelper.appendLocation(node);
        if (StringUtils.isNotBlank(location)) {
            appendLineIfNecessary();
            builder.append(location);
        }
        if (!node.isPropertyEmpty()) {
            String s = formatTblProperties(node.getProperties(), isEndNewLine(builder.toString()));
            builder.append(s);
        }
        removeNewLine(builder);
        return executable;
    }

    private void appendLineIfNecessary() {
        if (!isEndNewLine(builder.toString())) {
            builder.append(StringUtils.LF);
        }
    }

    private String formatTblProperties(List<Property> properties, boolean isEndNewLine) {
        List<Property> propertyList = properties.stream().filter(property -> {
            String name = property.getName();
            HivePropertyKey byValue = HivePropertyKey.getByValue(name);
            //一种是系统自定义的属性，并且能够打印，一种是自定义的默认打印
            return byValue == null || byValue.isSupportPrint();
        }).collect(Collectors.toList());

        if (CollectionUtils.isEmpty(propertyList)) {
            return StringUtils.EMPTY;
        }

        if (!context.isPrintProperty()) {
            return StringUtils.EMPTY;
        }

        StringBuilder sb = new StringBuilder();
        if (!isEndNewLine) {
            sb.append(StringUtils.LF);
        }
        sb.append("TBLPROPERTIES (");
        String collect = properties.stream().map(x ->
            formatStringLiteral(x.getName()) + "=" + formatStringLiteral(x.getValue())).collect(joining(","));
        sb.append(collect);
        sb.append(")");
        return sb.toString();
    }

    @Override
    public Boolean visitSetTableComment(SetTableComment setTableComment, Integer context) {
        builder.append("ALTER TABLE ").append(getCode(setTableComment.getQualifiedName()));
        builder.append(" SET TBLPROPERTIES").append("(");
        builder.append(String.format("'%s'='%s'", "comment", setTableComment.getComment().getComment()));
        builder.append(")");
        return true;
    }

    @Override
    protected String formatCommentElement(List<MultiComment> commentElements, String elementIndent) {
        return commentElements.stream().map(
            element -> {
                HiveVisitor visitor = new HiveVisitor(this.context);
                visitor.process(element.getNode(), 0);
                String result = visitor.getBuilder().toString();
                return COMMENT + elementIndent + result;
            }).collect(Collectors.joining(",\n"));
    }

    @Override
    protected String formatColName(Identifier colName, Integer size) {
        String value = colName.getValue();
        if (!colName.isDelimited()) {
            boolean reservedKeyWord = HiveReservedWordUtil.isReservedKeyWord(value);
            //如果node是关键字，那么进行转义处理
            if (reservedKeyWord) {
                value = StripUtils.addPrefix(value);
            } else {
                value = formatExpression(colName);
            }
        } else {
            value = formatExpression(colName);
        }
        return StringUtils.rightPad(value, size);
    }

    /**
     * hive 没有 level constraint定义
     *
     * @param levelConstraint
     * @param indent
     * @return
     */
    @Override
    public Boolean visitLevelConstraint(LevelConstraint levelConstraint, Integer indent) {
        super.visitLevelConstraint(levelConstraint, indent);
        return false;
    }

    /**
     * hive don't have
     *
     * @param columnGroupConstraint
     * @param indent
     * @return
     */
    @Override
    public Boolean visitColumnGroupConstraint(ColumnGroupConstraint columnGroupConstraint, Integer indent) {
        super.visitColumnGroupConstraint(columnGroupConstraint, indent);
        return false;
    }

    @Override
    public Boolean visitDimConstraint(DimConstraint dimConstraint, Integer indent) {
        //如果是维度表的话，那么是用foreign key的方式进行处理
        builder.append(indentString(indent));
        if (dimConstraint.getName() != null) {
            builder.append("CONSTRAINT ").append(formatExpression(dimConstraint.getName()));
        }
        List<Identifier> colNames = dimConstraint.getColNames();
        if (colNames != null) {
            builder.append(" FOREIGN KEY (");
            String collect = colNames.stream().map(ExpressionFormatter::formatExpression).collect(joining(","));
            builder.append(collect).append(")");
        }
        builder.append(" REFERENCES ").append(dimConstraint.getReferenceTable());
        if (dimConstraint.getReferenceColNames() != null) {
            builder.append(" (").append(
                dimConstraint.getReferenceColNames().stream().map(ExpressionFormatter::formatExpression)
                    .collect(joining(","))).append(")");
        }
        return true;
    }

    @Override
    public Boolean visitSetTableProperties(SetTableProperties setTableProperties, Integer context) {
        builder.append("ALTER TABLE ").append(getCode(setTableProperties.getQualifiedName()));
        builder.append(" SET TBLPROPERTIES").append('(');
        builder.append(formatProperty(setTableProperties.getProperties()));
        builder.append(')');
        return true;
    }

    /**
     * 如果是varchar或者char，统一转换为string处理。
     *
     * @param typeName
     * @return
     */
    private IDataTypeName convert(IDataTypeName typeName) {
        if (DATETIME.equals(typeName)) {
            return DataTypeEnums.TIMESTAMP;
        }
        return typeName;
    }

    @Override
    protected String formatColumnDefinition(ColumnDefinition column, Integer max) {
        BaseDataType dataType = column.getDataType();
        IDataTypeName typeName = dataType.getTypeName();
        IDataTypeName convert1 = convert(typeName);
        BaseDataType convert = DataTypeUtil.convert(dataType, convert1);
        StringBuilder sb = new StringBuilder()
            .append(formatColName(column.getColName(), max))
            .append(" ").append(formatExpression(convert));
        if (column.getComment() != null) {
            sb.append(formatComment(column.getComment()));
        }
        return sb.toString();
    }

    @Override
    public Boolean visitUnSetTableProperties(UnSetTableProperties unSetTableProperties, Integer context) {
        builder.append("ALTER TABLE ").append(getCode(unSetTableProperties.getQualifiedName()));
        builder.append(" UNSET TBLPROPERTIES IF EXISTS").append('(');
        builder.append(
            unSetTableProperties.getPropertyKeys().stream().map(x -> formatStringLiteral(x)).collect(joining(",")));
        builder.append(')');
        return true;
    }

    @Override
    public Boolean visitInsert(Insert insert, Integer context) {
        Boolean overwrite = insert.getOverwrite();
        if (BooleanUtils.isNotTrue(overwrite)) {
            builder.append("INSERT INTO TABLE ").append(getCode(insert.getQualifiedName()));
        } else {
            builder.append("INSERT OVERWRITE TABLE ").append(getCode(insert.getQualifiedName()));
        }
        appendPartition(builder, insert.getPartitionSpecList(), ",");
        if (BooleanUtils.isNotTrue(overwrite)) {
            builder.append(" (").append(
                insert.getColumns()
                    .stream()
                    .map(identifier -> formatColName(identifier, 0))
                    .collect(joining(","))
            ).append(")");
            builder.append(" \n ");
        }
        process(insert.getQuery(), context);
        return true;
    }

    @Override
    protected String getCode(QualifiedName qualifiedName) {
        QualifiedName join = StringJoinUtil.join(context.getDatabase(), context.getSchema(), qualifiedName.getSuffix());
        return formatName(join);
    }

    @Override
    public String formatName(QualifiedName name) {
        return name.getOriginalParts().stream()
            .map(e -> {
                return formatColName(e, 0);
            })
            .collect(joining("."));
    }

    @Override
    public Boolean visitAddConstraint(AddConstraint addConstraint, Integer context) {
        if (enableConstraint) {
            return super.visitAddConstraint(addConstraint, context);
        }
        return false;
    }

    @Override
    public Boolean visitDropConstraint(DropConstraint dropConstraint, Integer context) {
        if (enableConstraint) {
            return super.visitDropConstraint(dropConstraint, context);
        }
        return false;
    }

    /**
     * 不支持修改列语句
     *
     * @param renameCol
     * @param context
     * @return
     */
    @Override
    public Boolean visitRenameCol(RenameCol renameCol, Integer context) {
        super.visitRenameCol(renameCol, context);
        return false;
    }

    /**
     * 不支持修改列备注语句
     *
     * @param setColComment
     * @param context
     * @return
     */
    @Override
    public Boolean visitSetColComment(SetColComment setColComment, Integer context) {
        super.visitSetColComment(setColComment, context);
        return false;
    }

    /**
     * 不支持drop 分区列
     *
     * @param dropPartitionCol
     * @param context
     * @return
     */
    @Override
    public Boolean visitDropPartitionCol(DropPartitionCol dropPartitionCol, Integer context) {
        super.visitDropPartitionCol(dropPartitionCol, context);
        return false;
    }

    /**
     * 不支持增加分区列
     *
     * @param addPartitionCol
     * @param context
     * @return
     */
    @Override
    public Boolean visitAddPartitionCol(AddPartitionCol addPartitionCol, Integer context) {
        super.visitAddPartitionCol(addPartitionCol, context);
        return false;
    }

    @Override
    public Boolean visitSetTableAliasedName(SetTableAliasedName setTableAliasedName, Integer context) {
        super.visitSetTableAliasedName(setTableAliasedName, context);
        return false;

    }

    /**
     * 不支持删除列
     *
     * @param dropCol
     * @param context
     * @return
     */
    @Override
    public Boolean visitDropCol(DropCol dropCol, Integer context) {
        super.visitDropCol(dropCol, context);
        return false;
    }

    @Override
    protected String formatExpression(BaseExpression baseExpression) {
        return new HiveExpressionVisitor().process(baseExpression);
    }

}
