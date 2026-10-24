/*
 * Copyright 2021 Alibaba Group Holding Ltd.
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

package com.aliyun.fastmodel.transform.mc.format;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.common.utils.StripUtils;
import com.aliyun.fastmodel.core.formatter.FastModelVisitor;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.datatype.GenericDataType;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.constants.ConstraintType;
import com.aliyun.fastmodel.core.tree.statement.element.MultiComment;
import com.aliyun.fastmodel.core.tree.statement.insert.Insert;
import com.aliyun.fastmodel.core.tree.statement.script.RefRelation;
import com.aliyun.fastmodel.core.tree.statement.table.AddCols;
import com.aliyun.fastmodel.core.tree.statement.table.AddConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.AddPartitionCol;
import com.aliyun.fastmodel.core.tree.statement.table.ChangeCol;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.DropCol;
import com.aliyun.fastmodel.core.tree.statement.table.DropConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.DropPartitionCol;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.core.tree.statement.table.SetColumnOrder;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableAliasedName;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableProperties;
import com.aliyun.fastmodel.core.tree.statement.table.UnSetTableProperties;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.PrimaryConstraint;
import com.aliyun.fastmodel.transform.api.datatype.DataTypeConverter;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.AutoPartitionedBy;
import com.aliyun.fastmodel.transform.api.util.PropertyKeyUtil;
import com.aliyun.fastmodel.transform.api.util.StringJoinUtil;
import com.aliyun.fastmodel.transform.mc.context.MaxComputeContext;
import com.aliyun.fastmodel.transform.mc.parser.tree.constraint.ClusteredByConstraint;
import com.aliyun.fastmodel.transform.mc.parser.util.MaxComputeReservedWordUtil;
import com.aliyun.fastmodel.transform.mc.util.MaxComputeV1DataTypeUtil;
import com.google.common.base.Strings;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

import static java.util.stream.Collectors.joining;

/**
 * MaxCompute Visitor处理内容
 *
 * @author panguanjing
 * @date 2021/2/7
 */
public class MaxComputeVisitor
    extends FastModelVisitor {

    private final MaxComputeContext transformContext;

    public MaxComputeVisitor(MaxComputeContext transformContext) {
        this.transformContext = transformContext;
    }

    @Override
    public Boolean visitCreateTable(CreateTable node, Integer indent) {
        boolean columnNotEmpty = !node.isColumnEmpty();
        //maxcompute不支持没有列的表
        boolean executable = true;
        if (!columnNotEmpty) {
            executable = false;
        }
        builder.append(formatCreateTable(node));
        if (node.isNotExists()) {
            builder.append("IF NOT EXISTS ");
        }
        builder.append(getCode(node.getQualifiedName()));
        String elementIndent = indentString(indent + 1);
        if (columnNotEmpty) {
            builder.append(newLine("("));
            builder.append(formatColumnList(node, node.getColumnDefines(), elementIndent));
            appendConstraint(node, indent);
            builder.append(newLine(")"));
        } else {
            if (!node.isCommentElementEmpty()) {
                builder.append(newLine(COMMENT + "("));
                builder.append(formatCommentElement(node.getColumnCommentElements(), elementIndent));
                builder.append(newLine(COMMENT + ")"));
            }
        }
        if (node.getCommentValue() != null) {
            builder.append(formatComment(node.getComment(), isEndNewLine(builder.toString())));
        }
        if (!node.isPartitionEmpty()) {
            String f = formatPartitions(node.getPartitionedBy(),
                isEndNewLine(builder.toString()), elementIndent);
            builder.append(f);
        }
        ClusteredByConstraint clusteredByConstraint = getClusteredByConstraint(node.getConstraintStatements());
        if (clusteredByConstraint != null) {
            String f = formatClusteredBy(clusteredByConstraint, isEndNewLine(builder.toString()), elementIndent);
            builder.append(f);
        }
        // 外表 clause
        String formatExternalClause = formatExternalClause(node, elementIndent);
        builder.append(formatExternalClause);

        ///优先从fml的模型对象中，获取lifecycle对象
        if (!node.isPropertyEmpty()) {
            List<Property> properties = node.getProperties();
            appendTblProperties(node);
            Optional<Property> first = properties.stream().filter(x -> x.getName().equalsIgnoreCase(MaxComputePropertyKey.LIFE_CYCLE.getValue()))
                .findFirst();
            first.ifPresent(property -> builder.append(formatLifeCycle(property.getValue(), isEndNewLine(builder.toString()))));
        }
        return executable;
    }

    private String formatCreateTable(CreateTable node) {
        String defaultTable = "CREATE TABLE ";
        if (CollectionUtils.isEmpty(node.getProperties())) {
            return defaultTable;
        }
        Optional<Property> externalOpt = node.getProperties().stream()
            .filter(property -> StringUtils.equalsIgnoreCase(MaxComputePropertyKey.EXTERNAL.getValue(), property.getName()))
            .findAny();
        if (externalOpt.isPresent() && Boolean.parseBoolean(externalOpt.get().getValue())) {
            return "CREATE EXTERNAL TABLE ";
        }
        return defaultTable;
    }

    private String formatExternalClause(CreateTable node, String elementIndent) {
        StringBuilder builder = new StringBuilder();
        if (CollectionUtils.isEmpty(node.getProperties())) {
            return builder.toString();
        }
        String prefix = MaxComputePropertyKey.SERDE_PROPS.getValue() + ".";
        List<Property> serdeProps = node.getProperties().stream()
            .filter(property -> property.getName().toLowerCase().startsWith(prefix))
            .collect(Collectors.toList());
        // row format serde
        Optional<Property> rowFormatSerdeOpt = node.getProperties().stream()
            .filter(property -> StringUtils.equalsIgnoreCase(MaxComputePropertyKey.ROW_FORMAT_SERDE.getValue(), property.getName()))
            .findAny();
        if (rowFormatSerdeOpt.isPresent()) {
            builder.append("ROW FORMAT SERDE ").append(StripUtils.addStrip(rowFormatSerdeOpt.get().getValue())).append("\n");
            // with serdeproperties
            if (CollectionUtils.isNotEmpty(serdeProps)) {
                builder.append("WITH SERDEPROPERTIES (\n");
                String props = serdeProps.stream().map(property -> elementIndent
                    + StripUtils.addStrip(property.getName().substring(prefix.length()))
                    + " = "
                    + StripUtils.addStrip(property.getValue())).collect(joining(",\n"));
                builder.append(props).append("\n)\n");
            }
        }
        // stored by
        Optional<Property> storedByOpt = node.getProperties().stream()
            .filter(property -> StringUtils.equalsIgnoreCase(MaxComputePropertyKey.STORED_BY.getValue(), property.getName()))
            .findAny();
        if (storedByOpt.isPresent()) {
            builder.append("STORED BY ").append(StripUtils.addStrip(storedByOpt.get().getValue())).append("\n");
            // with serdeproperties
            if (CollectionUtils.isNotEmpty(serdeProps)) {
                builder.append("WITH SERDEPROPERTIES (\n");
                String props = serdeProps.stream().map(property -> elementIndent
                    + StripUtils.addStrip(property.getName().substring(prefix.length()))
                    + " = "
                    + StripUtils.addStrip(property.getValue())).collect(joining(",\n"));
                builder.append(props).append("\n)\n");
            }
        }
        // stored as
        Optional<Property> storedAsOpt = node.getProperties().stream()
            .filter(property -> StringUtils.equalsIgnoreCase(MaxComputePropertyKey.STORED_AS.getValue(), property.getName()))
            .findAny();
        Optional<Property> inputFormatOpt = node.getProperties().stream()
            .filter(property -> StringUtils.equalsIgnoreCase(MaxComputePropertyKey.INPUT_FORMAT.getValue(), property.getName()))
            .findAny();
        Optional<Property> outputFormatOpt = node.getProperties().stream()
            .filter(property -> StringUtils.equalsIgnoreCase(MaxComputePropertyKey.OUTPUT_FORMAT.getValue(), property.getName()))
            .findAny();
        if (storedAsOpt.isPresent()) {
            builder.append("STORED AS ").append(storedAsOpt.get().getValue()).append("\n");
        } else if (inputFormatOpt.isPresent() && outputFormatOpt.isPresent()) {
            builder.append("STORED AS INPUTFORMAT ").append(StripUtils.addStrip(inputFormatOpt.get().getValue()))
                .append(" OUTPUTFORMAT ").append(StripUtils.addStrip(outputFormatOpt.get().getValue())).append("\n");
        }
        // location
        Optional<Property> locationOpt = node.getProperties().stream()
            .filter(property -> StringUtils.equalsIgnoreCase(MaxComputePropertyKey.LOCATION.getValue(), property.getName()))
            .findAny();
        locationOpt.ifPresent(property -> builder.append("LOCATION ").append(StripUtils.addStrip(property.getValue())).append("\n"));
        // using
        Optional<Property> usingOpt = node.getProperties().stream()
            .filter(property -> StringUtils.equalsIgnoreCase(MaxComputePropertyKey.USING.getValue(), property.getName()))
            .findAny();
        usingOpt.ifPresent(property -> builder.append("USING ").append(StripUtils.addStrip(property.getValue())).append("\n"));
        return builder.toString();
    }

    protected String formatColumnList(CreateTable node, List<ColumnDefinition> list, String elementIndent) {
        OptionalInt max = list.stream().map(ColumnDefinition::getColName).mapToInt(identifier -> formatExpression(identifier).length()).max();
        return list.stream().map(element -> elementIndent + formatColumnDefinition(node, element, max.getAsInt())).collect(
            joining(",\n"));
    }

    private String formatColumnDefinition(CreateTable node, ColumnDefinition column, int max) {
        StringBuilder sb = getNameAndDataType(column, max);
        boolean isNotNull = column.getNotNull() != null && column.getNotNull();
        //如果是acid表，那么输出
        boolean isPrimary = column.getPrimary() != null && column.getPrimary();
        if (isPrimary) {
            //如果这里设置了主键，那么要强制加上NOT NULl
            sb.append(" NOT NULL PRIMARY KEY");
        }
        if (!isPrimary && isNotNull) {
            sb.append(" NOT NULL");
        }
        if (column.getComment() != null) {
            sb.append(formatComment(column.getComment()));
        }
        return sb.toString();
    }

    private StringBuilder getNameAndDataType(ColumnDefinition column, int max) {
        return new StringBuilder().append(formatColName(column.getColName(), max)).append(" ")
            .append(formatDataType(column.getDataType()));
    }

    @Override
    protected String formatDataType(BaseDataType dataType) {
        if (BooleanUtils.isTrue(transformContext.isUseV1DataType())) {
            return new V1DataTypeVisitor().process(convert(dataType));
        }
        return super.formatDataType(dataType);
    }

    private static class V1DataTypeVisitor extends MaxComputeExpressionVisitor {
        @Override
        public String visitGenericDataType(GenericDataType dataType, Void context) {
            IDataTypeName converted = MaxComputeV1DataTypeUtil.convertIfNecessary(dataType.getTypeName());
            return converted == null ? super.visitGenericDataType(dataType, context) : converted.getValue();
        }
    }

    @Override
    protected String formatColName(Identifier colName, Integer size) {
        String value = colName.getValue();
        if (!colName.isDelimited()) {
            boolean reservedKeyWord = MaxComputeReservedWordUtil.isReservedKeyWord(value);
            //如果node是关键字，那么进行转义处理
            if (reservedKeyWord) {
                value = StripUtils.addPrefix(value);
            } else {
                value = formatExpression(colName);
            }
        } else {
            value = formatExpression(colName);
        }
        if (!transformContext.isPrettyFormat()) {
            return value;
        }
        return StringUtils.rightPad(value, size);
    }

    @Override
    protected String indentString(Integer indent) {
        if (indent == null) {
            return StringUtils.EMPTY;
        }
        if (transformContext.isPrettyFormat()) {
            return Strings.repeat(INDENT, indent);
        }
        return StringUtils.EMPTY;
    }

    /**
     * mc只支持主键索引
     *
     * @param node
     * @param indent
     */
    private void appendConstraint(CreateTable node, Integer indent) {
        if (node.isConstraintEmpty()) {
            return;
        }
        List<BaseConstraint> primaryConstraints = node.getConstraintStatements().stream().filter(
                c -> c.getConstraintType() == ConstraintType.PRIMARY_KEY)
            .collect(Collectors.toList());
        if (CollectionUtils.isEmpty(primaryConstraints)) {
            return;
        }
        // 主键约束只会有一个
        PrimaryConstraint primaryConstraint = (PrimaryConstraint) primaryConstraints.get(0);
        builder.append(",\n");
        process(primaryConstraint, indent + 1);
    }

    /**
     * append tbl properties
     *
     * @param node
     */
    private void appendTblProperties(CreateTable node) {
        if (node.isPropertyEmpty()) {
            return;
        }
        List<Property> properties = node.getProperties();
        List<Property> collect = properties.stream().filter(p -> {
            MaxComputePropertyKey definedKey = getPropertyKey(p);
            boolean definedAndSupportPrint = definedKey != null && definedKey.isSupportPrint();
            boolean notFilterUnDefined = definedKey == null && !transformContext.isFilterProperties();
            return definedAndSupportPrint || notFilterUnDefined;
        }).collect(Collectors.toList());
        if (CollectionUtils.isEmpty(collect)) {
            return;
        }
        if (!isEndNewLine(builder.toString())) {
            builder.append(StringUtils.LF);
        }
        builder.append("TBLPROPERTIES (");
        String result = collect.stream().map(property -> formatStringLiteral(property.getName()) + "=" + formatStringLiteral(property.getValue()))
            .collect(joining(","));
        builder.append(result);
        builder.append(")");
    }

    private MaxComputePropertyKey getPropertyKey(Property p) {
        MaxComputePropertyKey definedKey = MaxComputePropertyKey.getByValue(p.getName());
        if (definedKey != null) {
            return definedKey;
        }
        String prefix = MaxComputePropertyKey.SERDE_PROPS.getValue() + ".";
        if (p.getName().toLowerCase().startsWith(prefix)) {
            return MaxComputePropertyKey.SERDE_PROPS;
        }
        return null;
    }

    private String formatLifeCycle(String lifeCycle, boolean isNewLine) {
        if (isNewLine) {
            return "LIFECYCLE " + lifeCycle;
        } else {
            return "\nLIFECYCLE " + lifeCycle;
        }
    }

    @Override
    protected String formatCommentElement(List<MultiComment> commentElements, String elementIndent) {
        return commentElements.stream().map(
            element -> {
                MaxComputeVisitor visitor = new MaxComputeVisitor(this.transformContext);
                visitor.process(element.getNode(), 0);
                String result = visitor.getBuilder().toString();
                return COMMENT + elementIndent + result;
            }).collect(Collectors.joining(",\n"));
    }

    @Override
    protected String getCode(QualifiedName qualifiedName) {
        QualifiedName tableName = StringJoinUtil.join(this.transformContext.getMcProject(), this.transformContext.getMcSchema(),
            qualifiedName.getSuffix());
        return formatName(tableName);
    }

    /**
     * 转换
     *
     * @param dataType
     * @return
     */
    @Override
    protected BaseDataType convert(BaseDataType dataType) {
        DataTypeConverter dataTypeTransformer = this.transformContext.getDataTypeTransformer();
        if (dataTypeTransformer != null) {
            return dataTypeTransformer.convert(dataType);
        }
        return dataType;
    }

    @Override
    protected String formatColumnDefinition(ColumnDefinition column, Integer max) {
        StringBuilder sb = getNameAndDataType(column, max);
        if (column.getComment() != null) {
            sb.append(formatComment(column.getComment()));
        }
        return sb.toString();
    }

    @Override
    public Boolean visitChangeCol(ChangeCol renameCol, Integer context) {
        builder.append("ALTER TABLE ").append(getCode(renameCol.getQualifiedName()));
        ColumnDefinition columnDefinition = renameCol.getColumnDefinition();
        builder.append(" CHANGE COLUMN ").append(formatExpression(renameCol.getOldColName())).append(" ").append(
            formatColumnDefinition(columnDefinition, 0)
        );
        return true;
    }

    @Override
    public Boolean visitSetColumnOrder(SetColumnOrder setColumnOrder, Integer context) {
        // MaxCompute 支持 CHANGE COLUMN ... AFTER <col> 调整列顺序（需 odps.schema.evolution.enable=true
        // 且账号为 Owner/Admin），但没有 FIRST 关键字、无法把列移动到首位（见 MaxCompute 文档
        // user-guide/column-operation）。仍调用 super 生成 DDL 文本（DialectNode 的 node 字段代表"这条
        // 语句长什么样"，供审计/非执行语句留痕，与 visitDropPartitionCol 等一致），再按引擎能力声明可
        // 执行性：AFTER 定位可执行、正常下发；first=true 移到首位不可执行，由 DialectNode#isExecutable
        // 过滤——既不下发引擎、发布前 fastsql 校验也不会撞 FIRST。
        super.visitSetColumnOrder(setColumnOrder, context);
        return setColumnOrder.getBeforeColName() != null;
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
                insert.getColumns().stream().map(identifier -> formatColName(identifier, 0)).collect(joining(","))
            ).append(")");
            builder.append(" \n ");
        }
        process(insert.getQuery(), context);
        return true;
    }

    @Override
    public Boolean visitAddCols(AddCols addCols, Integer context) {
        return super.visitAddCols(addCols, context);
    }

    /**
     * MaxCompute 不支持删除列，增加约束和删除约束, 增加分区列和删除分区列
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

    @Override
    public Boolean visitAddPartitionCol(AddPartitionCol addPartitionCol, Integer context) {
        super.visitAddPartitionCol(addPartitionCol, context);
        return false;
    }

    @Override
    public Boolean visitDropCol(DropCol dropCol, Integer context) {
        super.visitDropCol(dropCol, context);
        return true;
    }

    @Override
    public Boolean visitAddConstraint(AddConstraint addConstraint, Integer context) {
        super.visitAddConstraint(addConstraint, context);
        return false;
    }

    @Override
    public Boolean visitDropConstraint(DropConstraint dropConstraint, Integer context) {
        super.visitDropConstraint(dropConstraint, context);
        return false;
    }

    @Override
    public Boolean visitUnSetTableProperties(UnSetTableProperties unSetTableProperties, Integer context) {
        List<String> propertyKeys = unSetTableProperties.getPropertyKeys();
        Optional<String> lifeCycle = propertyKeys.stream().filter(k -> {
            return StringUtils.equalsIgnoreCase(k, MaxComputePropertyKey.LIFE_CYCLE.getValue());
        }).findFirst();
        if (!lifeCycle.isPresent()) {
            super.visitUnSetTableProperties(unSetTableProperties, context);
            return false;
        }
        builder.append("ALTER TABLE ").append(getCode(unSetTableProperties.getQualifiedName()));
        builder.append(" DISABLE LIFECYCLE");
        return true;
    }

    @Override
    public Boolean visitSetTableAliasedName(SetTableAliasedName setTableAliasedName, Integer context) {
        super.visitSetTableAliasedName(setTableAliasedName, context);
        return false;
    }

    @Override
    public Boolean visitRefEntityStatement(RefRelation refEntityStatement, Integer context) {
        super.visitRefEntityStatement(refEntityStatement, context);
        return false;
    }

    @Override
    public Boolean visitSetTableProperties(SetTableProperties setTableProperties, Integer context) {
        List<Property> properties = setTableProperties.getProperties();
        for (Property p : properties) {
            if (StringUtils.equalsIgnoreCase(p.getName(), MaxComputePropertyKey.LIFE_CYCLE.getValue())) {
                QualifiedName qualifiedName = setTableProperties.getQualifiedName();
                builder.append("ALTER TABLE ").append(getCode(qualifiedName)).append(" ENABLE LIFECYCLE");
                builder.append(";\n");
                builder.append("ALTER TABLE ").append(getCode(qualifiedName)).append(" SET LIFECYCLE ").append(p.getValue());
                return true;
            }
        }
        super.visitSetTableProperties(setTableProperties, context);
        return false;
    }

    @Override
    protected String formatExpression(BaseExpression baseExpression) {
        return new MaxComputeExpressionVisitor().process(baseExpression);
    }

    @Override
    protected String formatPartitions(PartitionedBy partitionedBy, boolean isNewLine, String elementIndent) {
        if (!(partitionedBy instanceof AutoPartitionedBy)) {
            return super.formatPartitions(partitionedBy, isNewLine, elementIndent);
        }
        AutoPartitionedBy auto = (AutoPartitionedBy)partitionedBy;
        StringBuilder sb = new StringBuilder();
        if (!isNewLine) {
            sb.append("\n");
        }
        sb.append("AUTO PARTITIONED BY\n(\n");
        sb.append(elementIndent)
            .append(formatExpression(auto.getExpression()))
            .append(" AS ")
            .append(auto.getAlias().getValue());
        sb.append("\n").append(")\n");
        return sb.toString();
    }

    private ClusteredByConstraint getClusteredByConstraint(List<BaseConstraint> constraints) {
        if (CollectionUtils.isEmpty(constraints)) {
            return null;
        }
        Optional<BaseConstraint> any = constraints.stream()
            .filter(constraint -> constraint instanceof ClusteredByConstraint)
            .findAny();
        return (ClusteredByConstraint) any.orElse(null);
    }

    private String formatClusteredBy(ClusteredByConstraint clusteredByConstraint, boolean isNewLine, String elementIndent) {
        StringBuilder stringBuilder = new StringBuilder();
        if (!isNewLine) {
            stringBuilder.append("\n");
        }
        if (clusteredByConstraint.getClusterType() != null
            && ClusteredByConstraint.ClusterType.RANGE == clusteredByConstraint.getClusterType()) {
            stringBuilder.append("RANGE CLUSTERED BY ");
        } else {
            stringBuilder.append("CLUSTERED BY ");
        }
        if (CollectionUtils.isNotEmpty(clusteredByConstraint.getColumns())) {
            stringBuilder.append("(");
            String columns = clusteredByConstraint.getColumns().stream().map(Identifier::getValue).collect(joining(","));
            stringBuilder.append(columns);
            stringBuilder.append(")\n");
        }
        if (CollectionUtils.isNotEmpty(clusteredByConstraint.getSortColumns())) {
            stringBuilder.append("SORTED BY (");
            String sort = clusteredByConstraint.getSortColumns().stream()
                .map(sortItem -> StripUtils.strip(sortItem.getSortKey().toString()) + " " + sortItem.getOrdering().getCode())
                .collect(joining(","));
            stringBuilder.append(sort);
            stringBuilder.append(") ");
        }
        if (clusteredByConstraint.getBucketNum() != null) {
            stringBuilder.append("INTO ").append(clusteredByConstraint.getBucketNum()).append(" BUCKETS\n");
        }
        return stringBuilder.toString();
    }
}
