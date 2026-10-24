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

package com.aliyun.fastmodel.transform.starrocks.format;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.core.formatter.FastModelVisitor;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.datatype.GenericDataType;
import com.aliyun.fastmodel.core.tree.datatype.RowDataType;
import com.aliyun.fastmodel.core.tree.datatype.TypeParameter;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.atom.FunctionCall;
import com.aliyun.fastmodel.core.tree.expr.literal.BooleanLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.ListStringLiteral;
import com.aliyun.fastmodel.core.tree.statement.BaseOperatorStatement;
import com.aliyun.fastmodel.core.tree.statement.CompositeStatement;
import com.aliyun.fastmodel.core.tree.statement.table.AddCols;
import com.aliyun.fastmodel.core.tree.statement.table.ChangeCol;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.DropCol;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.core.tree.statement.table.RenameCol;
import com.aliyun.fastmodel.core.tree.statement.table.RenameTable;
import com.aliyun.fastmodel.core.tree.statement.table.SetColumnOrder;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableProperties;
import com.aliyun.fastmodel.core.tree.statement.table.UnSetTableProperties;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.PrimaryConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.UniqueConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.index.TableIndex;
import com.aliyun.fastmodel.core.tree.util.PropertyUtil;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.AggregateKeyConstraint;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.DuplicateKeyConstraint;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.desc.DistributeNonKeyConstraint;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.desc.NonKeyConstraint;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.desc.OrderByNonKeyConstraint;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.desc.RollupItem;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.desc.RollupNonKeyConstraint;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.ExpressionPartitionBy;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.ListPartitionedBy;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.RangePartitionedBy;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.desc.MultiItemListPartition;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.desc.MultiRangePartition;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.desc.PartitionDesc;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.desc.SingleItemListPartition;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.desc.SingleRangePartition;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.keyvalue.ArrayPartitionKey;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.keyvalue.LessThanPartitionKey;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.keyvalue.ListPartitionValue;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.keyvalue.PartitionValue;
import com.aliyun.fastmodel.transform.api.format.PropertyKey;
import com.aliyun.fastmodel.transform.starrocks.context.StarRocksContext;
import com.aliyun.fastmodel.transform.starrocks.parser.visitor.StarRocksAstVisitor;
import com.google.common.base.Objects;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

import static com.aliyun.fastmodel.transform.api.extension.client.property.ExtensionPropertyKey.COLUMN_AGG_DESC;
import static com.aliyun.fastmodel.transform.api.extension.client.property.ExtensionPropertyKey.COLUMN_AUTO_INCREMENT;
import static com.aliyun.fastmodel.transform.api.extension.client.property.ExtensionPropertyKey.COLUMN_KEY;
import static com.aliyun.fastmodel.transform.api.extension.client.property.ExtensionPropertyKey.TABLE_ENGINE;
import static com.aliyun.fastmodel.transform.api.extension.client.property.ExtensionPropertyKey.TABLE_INDEX_COMMENT;
import static com.aliyun.fastmodel.transform.api.extension.client.property.ExtensionPropertyKey.TABLE_INDEX_TYPE;
import static com.aliyun.fastmodel.transform.api.extension.client.property.ExtensionPropertyKey.TABLE_PARTITION_RAW;

/**
 * StarRocksVisitor
 *
 * @author panguanjing
 * @date 2023/9/5
 */
public class StarRocksOutVisitor extends FastModelVisitor implements StarRocksAstVisitor<Boolean, Integer> {

    public static final String MAXVALUE = "MAXVALUE";
    private final StarRocksContext starRocksContext;
    private final CreateTable alterTarget;
    private final Set<Identifier> keyColumns = new LinkedHashSet<>();
    private boolean alterClauseOnly;

    public StarRocksOutVisitor(StarRocksContext context) {
        this(context, null);
    }

    public StarRocksOutVisitor(StarRocksContext context, CreateTable alterTarget) {
        this.starRocksContext = context;
        this.alterTarget = alterTarget;
        if (alterTarget != null && alterTarget.getConstraintStatements() != null) {
            for (BaseConstraint constraint : alterTarget.getConstraintStatements()) {
                if (constraint instanceof PrimaryConstraint || constraint instanceof UniqueConstraint
                    || constraint instanceof DuplicateKeyConstraint || constraint instanceof AggregateKeyConstraint) {
                    constraint.getChildren().stream().filter(Identifier.class::isInstance)
                        .map(Identifier.class::cast).forEach(keyColumns::add);
                }
            }
        }
    }

    /**
     * Only compatible column changes can share a StarRocks schema-change job.
     */
    public static boolean isSchemaChange(BaseStatement statement) {
        if (statement instanceof SetColumnOrder) {
            SetColumnOrder order = (SetColumnOrder)statement;
            return Objects.equal(order.getOldColName(), order.getNewColName())
                && (order.getBeforeColName() != null || BooleanUtils.isTrue(order.getFirst()));
        }
        if (statement instanceof ChangeCol) {
            ChangeCol change = (ChangeCol)statement;
            return Objects.equal(change.getOldColName(), change.getColumnDefinition().getColName());
        }
        return statement instanceof AddCols || statement instanceof DropCol;
    }

    @Override
    public Boolean visitCompositeStatement(CompositeStatement statement, Integer context) {
        List<BaseStatement> statements = statement.getStatements();
        if (statements.isEmpty() || !statements.stream().allMatch(StarRocksOutVisitor::isSchemaChange)) {
            return super.visitCompositeStatement(statement, context);
        }
        QualifiedName table = ((BaseOperatorStatement)statements.get(0)).getQualifiedName();
        if (statements.stream().anyMatch(item -> !Objects.equal(table,
            ((BaseOperatorStatement)item).getQualifiedName()))) {
            return super.visitCompositeStatement(statement, context);
        }
        // Separate ALTER statements race with the preceding asynchronous schema change.
        appendAlterTable(table);
        boolean previous = alterClauseOnly;
        alterClauseOnly = true;
        try {
            boolean executable = true;
            for (int i = 0; i < statements.size(); i++) {
                if (i > 0) {
                    builder.append(",");
                }
                executable &= Boolean.TRUE.equals(process(statements.get(i), context));
            }
            return executable;
        } finally {
            alterClauseOnly = previous;
        }
    }

    /**
     * UNKNOWN_TYPE preserves metadata for inspection but cannot be executed as a column type.
     */
    private boolean containsUnknownType(BaseDataType dataType) {
        if (dataType == null) {
            return false;
        }
        if ("UNKNOWN_TYPE".equalsIgnoreCase(dataType.getTypeName().getValue())) {
            return true;
        }
        if (dataType instanceof GenericDataType) {
            GenericDataType generic = (GenericDataType)dataType;
            return CollectionUtils.isNotEmpty(generic.getArguments()) && generic.getArguments().stream()
                .filter(TypeParameter.class::isInstance).map(TypeParameter.class::cast)
                .anyMatch(parameter -> containsUnknownType(parameter.getType()));
        }
        if (dataType instanceof RowDataType) {
            RowDataType row = (RowDataType)dataType;
            return CollectionUtils.isNotEmpty(row.getFields()) && row.getFields().stream()
                .anyMatch(field -> containsUnknownType(field.getDataType()));
        }
        return false;
    }

    private void appendAlterTable(QualifiedName table) {
        if (!alterClauseOnly) {
            builder.append("ALTER TABLE ").append(getCode(table));
        }
    }

    @Override
    public Boolean visitCreateTable(CreateTable node, Integer indent) {
        // StarRocks 要求分区列必须出现在列定义中,补回仅声明在 PARTITION BY 里的分区列
        List<ColumnDefinition> columnDefines = mergePartitionColumnsIntoColumns(node);
        boolean columnEmpty = CollectionUtils.isEmpty(columnDefines);
        //maxcompute不支持没有列的表
        boolean executable = !columnEmpty && columnDefines.stream()
            .noneMatch(column -> containsUnknownType(column.getDataType()));
        builder.append("CREATE TABLE ");
        if (node.isNotExists()) {
            builder.append("IF NOT EXISTS ");
        }
        String tableName = getCode(node.getQualifiedName());
        builder.append(tableName);
        if (!columnEmpty) {
            builder.append("\n(\n");
            String elementIndent = indentString(indent + 1);
            String columnList = formatColumnList(columnDefines, elementIndent);
            builder.append(columnList);

            if (!node.isIndexEmpty()) {
                for (TableIndex tableIndex : node.getTableIndexList()) {
                    builder.append(",\n");
                    process(tableIndex, indent + 1);
                }
            }
            builder.append("\n").append(")");
        }

        //format engine
        if (node.getProperties() != null) {
            Optional<Property> first = node.getProperties().stream().filter(
                p -> StringUtils.equalsIgnoreCase(p.getName(), TABLE_ENGINE.getValue())).findFirst();
            first.ifPresent(property -> builder.append("\nENGINE=").append(property.getValue()));
        }

        //key constraint
        if (!node.isConstraintEmpty()) {
            List<BaseConstraint> keyConstraint = node.getConstraintStatements().stream().filter(
                c -> !(c instanceof NonKeyConstraint)).collect(Collectors.toList());
            appendConstraint(keyConstraint, indent);
        }

        if (node.getComment() != null) {
            builder.append("\n");
            builder.append(formatComment(node.getComment(), true));
        }
        //format partitioned by
        if (!node.isPartitionEmpty()) {
            builder.append("\n");
            process(node.getPartitionedBy(), indent);
        } else {
            String propertyValue = PropertyUtil.getPropertyValue(node.getProperties(), TABLE_PARTITION_RAW.getValue());
            if (StringUtils.isNotBlank(propertyValue)) {
                builder.append("\n");
                builder.append(propertyValue);
            }
        }

        //non key constraint
        if (!node.isConstraintEmpty()) {
            List<BaseConstraint> nonKeyConstraint = node.getConstraintStatements().stream().filter(
                c -> c instanceof NonKeyConstraint).collect(Collectors.toList());
            appendConstraint(nonKeyConstraint, indent);
        }

        if (!node.isPropertyEmpty()) {
            String prop = formatProperty(node.getProperties());
            if (StringUtils.isNotBlank(prop)) {
                builder.append("\n");
                builder.append("PROPERTIES (");
                builder.append(prop);
                builder.append(")");
            }
        }
        builder.append(";");
        return executable;
    }

    /**
     * StarRocks 要求分区列必须出现在列定义中(列类型取自列定义)。当上游 AST 把分区列只放进
     * PARTITION BY(ODPS/MaxCompute 语义)而未并入普通列时,这里按列名去重补回,保证生成的
     * 建表 DDL 合法;仅引用列名(无数据类型)的分区列说明本应已在普通列中,不臆造无类型列。
     *
     * @param node 建表语法树
     * @return 合并分区列后的列定义列表
     */
    private List<ColumnDefinition> mergePartitionColumnsIntoColumns(CreateTable node) {
        List<ColumnDefinition> columns = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(node.getColumnDefines())) {
            columns.addAll(node.getColumnDefines());
        }
        PartitionedBy partitionedBy = node.getPartitionedBy();
        if (partitionedBy == null || CollectionUtils.isEmpty(partitionedBy.getColumnDefinitions())) {
            return columns;
        }
        List<ColumnDefinition> partitionColumns = partitionedBy.getColumnDefinitions();
        Set<String> existing = columns.stream()
            .map(column -> formatExpression(column.getColName()))
            .collect(Collectors.toCollection(LinkedHashSet::new));
        for (ColumnDefinition partitionColumn : partitionColumns) {
            if (partitionColumn.getDataType() == null) {
                continue;
            }
            if (existing.add(formatExpression(partitionColumn.getColName()))) {
                columns.add(partitionColumn);
            }
        }
        return columns;
    }

    @Override
    protected String formatProperty(List<Property> properties) {
        List<Property> collect = properties.stream().filter(
            p -> {
                PropertyKey propertyKey = StarRocksPropertyKey.getByValue(p.getName());
                return propertyKey == null || propertyKey.isSupportPrint();
            }
        ).collect(Collectors.toList());
        return super.formatProperty(collect);
    }

    @Override
    protected String formatColumnDefinition(ColumnDefinition column, Integer max) {
        BaseDataType dataType = column.getDataType();
        String col = formatColName(column.getColName(), max);
        StringBuilder sb = new StringBuilder().append(col);
        if (dataType != null) {
            sb.append(" ").append(formatDataType(dataType));
        }
        List<Property> columnProperties = column.getColumnProperties();
        //charset
        String charSet = PropertyUtil.getPropertyValue(columnProperties, StarRocksPropertyKey.COLUMN_CHAR_SET.getValue());
        if (StringUtils.isNotBlank(charSet)) {
            sb.append(" ").append("CHARSET ").append(charSet);
        }
        //key
        String key = PropertyUtil.getPropertyValue(columnProperties, COLUMN_KEY.getValue());
        if (StringUtils.isNotBlank(key) || keyColumns.contains(column.getColName())) {
            sb.append(" ").append("KEY");
        }
        //aggDesc
        String propertyValue = PropertyUtil.getPropertyValue(columnProperties, COLUMN_AGG_DESC.getValue());
        if (StringUtils.isNotBlank(propertyValue)) {
            sb.append(" ").append(propertyValue);
        }
        boolean isNotNull = column.getNotNull() != null && column.getNotNull();
        if (isNotNull) {
            sb.append(" NOT NULL");
        } else if (BooleanUtils.isFalse(column.getNotNull())) {
            sb.append(" NULL");
        }
        if (column.getDefaultValue() != null) {
            String expressionValue = formatExpression(column.getDefaultValue());
            if (column.getDefaultValue() instanceof FunctionCall) {
                sb.append(" DEFAULT (").append(expressionValue).append(")");
            } else {
                sb.append(" DEFAULT ").append(expressionValue);
            }
        }
        //auto increment
        String autoIncrement = PropertyUtil.getPropertyValue(columnProperties, COLUMN_AUTO_INCREMENT.getValue());
        if (StringUtils.equalsIgnoreCase(BooleanLiteral.TRUE, autoIncrement)) {
            sb.append(" ").append("AUTO_INCREMENT");
        }
        sb.append(formatComment(column.getComment(), isEndNewLine(sb.toString())));
        return sb.toString();
    }

    private void appendConstraint(List<BaseConstraint> constraints, Integer indent) {
        for (BaseConstraint baseConstraint : constraints) {
            builder.append("\n");
            process(baseConstraint, indent);
        }
    }

    @Override
    public Boolean visitAggregateConstraint(AggregateKeyConstraint aggregateConstraint, Integer context) {
        builder.append("AGGREGATE KEY (");
        List<Identifier> colNames = aggregateConstraint.getColumns();
        builder.append(colNames.stream().map(this::formatExpression).collect(Collectors.joining(",")));
        builder.append(")");
        return true;
    }

    @Override
    public Boolean visitDuplicateConstraint(DuplicateKeyConstraint duplicateConstraint, Integer context) {
        builder.append("DUPLICATE KEY (");
        List<Identifier> colNames = duplicateConstraint.getColumns();
        builder.append(colNames.stream().map(this::formatExpression).collect(Collectors.joining(",")));
        builder.append(")");
        return true;
    }

    @Override
    public Boolean visitPrimaryConstraint(PrimaryConstraint primaryConstraint, Integer ident) {
        builder.append("PRIMARY KEY (");
        List<Identifier> colNames = primaryConstraint.getColNames();
        builder.append(colNames.stream().map(this::formatExpression).collect(Collectors.joining(",")));
        builder.append(")");
        return true;
    }

    @Override
    public Boolean visitRollupConstraint(RollupNonKeyConstraint rollupConstraint, Integer context) {
        builder.append("ROLLUP (");
        if (CollectionUtils.isNotEmpty(rollupConstraint.getRollupItemList())) {
            String s = rollupConstraint.getRollupItemList().stream().map(
                this::formatRollupItem
            ).collect(Collectors.joining(","));
            builder.append(s);
        }
        builder.append(")");
        return true;
    }

    private String formatRollupItem(RollupItem item) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(formatExpression(item.getRollupName()));
        stringBuilder.append(" (");
        String columnList = getCollect(item.getColumnList());
        stringBuilder.append(columnList);
        stringBuilder.append(")");
        if (CollectionUtils.isNotEmpty(item.getDuplicateList())) {
            stringBuilder.append(" DUPLICATE KEY (");
            String duplicate = getCollect(item.getDuplicateList());
            stringBuilder.append(duplicate);
            stringBuilder.append(")");
        }
        if (item.getFromRollup() != null) {
            stringBuilder.append(" FROM ");
            stringBuilder.append(formatExpression(item.getFromRollup()));
        }

        if (CollectionUtils.isNotEmpty(item.getProperties())) {
            stringBuilder.append(" PROPERTIES (");
            String p = formatProperty(item.getProperties());
            stringBuilder.append(p);
            stringBuilder.append(")");
        }
        return stringBuilder.toString();
    }

    private String getCollect(List<Identifier> item) {
        return item.stream().map(this::formatExpression).collect(Collectors.joining(","));
    }

    @Override
    protected String formatStringLiteral(String s) {
        return new StarRocksExpressionVisitor(starRocksContext).formatStringLiteral(s);
    }

    @Override
    public Boolean visitUniqueConstraint(UniqueConstraint uniqueConstraint, Integer indent) {
        builder.append("UNIQUE KEY (");
        builder.append(uniqueConstraint.getColumnNames().stream().map(this::formatExpression).collect(Collectors.joining(",")));
        builder.append(")");
        return true;
    }

    @Override
    protected String formatExpression(BaseExpression baseExpression) {
        return new StarRocksExpressionVisitor(starRocksContext).process(baseExpression);
    }

    @Override
    public Boolean visitChangeCol(ChangeCol renameCol, Integer context) {
        //starRocks不支持修改列名
        Identifier oldColName = renameCol.getOldColName();
        ColumnDefinition columnDefinition = renameCol.getColumnDefinition();
        Identifier targetColName = columnDefinition.getColName();
        if (!Objects.equal(oldColName, targetColName)) {
            return false;
        }
        if (alterTarget != null) {
            columnDefinition = alterTarget.getColumnDefines().stream()
                .filter(column -> Objects.equal(column.getColName(), targetColName))
                .findFirst().orElse(columnDefinition);
        }
        appendAlterTable(renameCol.getQualifiedName());
        builder.append(" MODIFY COLUMN");
        builder.append(" ").append(formatColumnDefinition(columnDefinition, 0));
        return !containsUnknownType(columnDefinition.getDataType());
    }

    @Override
    public Boolean visitSetColumnOrder(SetColumnOrder setColumnOrder, Integer context) {
        if (!Objects.equal(setColumnOrder.getOldColName(), setColumnOrder.getNewColName())
            || (setColumnOrder.getBeforeColName() == null && !BooleanUtils.isTrue(setColumnOrder.getFirst()))) {
            return super.visitSetColumnOrder(setColumnOrder, context);
        }
        // StarRocks 调序使用 MODIFY COLUMN，必须保留列定义，避免同时改变可空性、默认值等属性。
        appendAlterTable(setColumnOrder.getQualifiedName());
        builder.append(" MODIFY COLUMN ").append(formatColumnDefinition(setColumnOrder.getColumnDefinition(), 0));
        if (setColumnOrder.getBeforeColName() != null) {
            builder.append(" AFTER ").append(formatExpression(setColumnOrder.getBeforeColName()));
        } else {
            builder.append(" FIRST");
        }
        return !containsUnknownType(setColumnOrder.getColumnDefinition().getDataType());
    }

    @Override
    public Boolean visitDropCol(DropCol dropCol, Integer context) {
        appendAlterTable(dropCol.getQualifiedName());
        builder.append(" DROP COLUMN ").append(formatExpression(dropCol.getColumnName()));
        return true;
    }

    @Override
    public Boolean visitRenameTable(RenameTable renameTable, Integer context) {
        builder.append("ALTER TABLE ");
        builder.append(getCode(renameTable.getQualifiedName()));
        builder.append(" RENAME ").append(getCode(renameTable.getTarget()));
        return true;
    }

    /**
     * starRocks不支持修改列名
     *
     * @param renameCol
     * @param context
     * @return
     */
    @Override
    public Boolean visitRenameCol(RenameCol renameCol, Integer context) {
        return !super.visitRenameCol(renameCol, context);
    }

    @Override
    public Boolean visitSetTableProperties(SetTableProperties setTableProperties, Integer context) {
        builder.append("ALTER TABLE ");
        builder.append(formatName(setTableProperties.getQualifiedName()));
        builder.append(" SET (");
        builder.append(formatProperty(setTableProperties.getProperties()));
        builder.append(")");
        return true;
    }

    @Override
    public Boolean visitUnSetTableProperties(UnSetTableProperties unSetTableProperties, Integer context) {
        builder.append("ALTER TABLE ");
        builder.append(formatName(unSetTableProperties.getQualifiedName()));
        builder.append(" SET (");
        List<String> propertyKeys = unSetTableProperties.getPropertyKeys();
        String a = propertyKeys.stream().map(
            k -> formatStringLiteral(k) + "=" + "\"\""
        ).collect(Collectors.joining(","));
        builder.append(a);
        builder.append(")");
        return true;
    }

    @Override
    public Boolean visitAddCols(AddCols addCols, Integer context) {
        appendAlterTable(addCols.getQualifiedName());
        builder.append(" ADD COLUMN\n").append('(').append("\n");
        String columnList = formatColumnList(addCols.getColumnDefineList(), indentString(context + 1));
        builder.append(columnList);
        builder.append("\n");
        builder.append(')');
        return addCols.getColumnDefineList().stream()
            .noneMatch(column -> containsUnknownType(column.getDataType()));
    }

    @Override
    public Boolean visitTableIndex(TableIndex tableIndex, Integer ident) {
        builder.append(indentString(ident));
        builder.append("INDEX ").append(formatExpression(tableIndex.getIndexName()));
        appendTableIndex(tableIndex.getIndexColumnNames());
        List<Property> properties = tableIndex.getProperties();
        if (CollectionUtils.isEmpty(properties)) {
            return true;
        }
        Optional<Property> first = properties.stream().filter(p -> {
            return StringUtils.equalsIgnoreCase(p.getName(), TABLE_INDEX_TYPE.getValue());
        }).findFirst();
        first.ifPresent(property -> builder.append(" USING ").append(property.getValue()));

        Optional<Property> comment = properties.stream().filter(p -> {
            return StringUtils.equalsIgnoreCase(p.getName(), TABLE_INDEX_COMMENT.getValue());
        }).findFirst();
        comment.ifPresent(property -> builder.append(" COMMENT ").append(formatStringLiteral(property.getValue())));
        return true;
    }

    @Override
    public Boolean visitPartitionedBy(PartitionedBy partitionedBy, Integer context) {
        if (partitionedBy instanceof RangePartitionedBy) {
            return visitRangePartitionedBy((RangePartitionedBy)partitionedBy, context);
        }
        if (partitionedBy instanceof ListPartitionedBy) {
            return visitListPartitionedBy((ListPartitionedBy)partitionedBy, context);
        }
        if (partitionedBy instanceof ExpressionPartitionBy) {
            return visitExpressionPartitionedBy((ExpressionPartitionBy)partitionedBy, context);
        }
        ExpressionPartitionBy expressionPartitionBy = new ExpressionPartitionBy(
            partitionedBy.getColumnDefinitions(), null, null
        );
        return visitExpressionPartitionedBy(expressionPartitionBy, context);
    }

    @Override
    public Boolean visitRangePartitionedBy(RangePartitionedBy starRocksPartitionedBy, Integer indent) {
        builder.append("PARTITION BY RANGE (");
        List<ColumnDefinition> partitioned = starRocksPartitionedBy.getColumnDefinitions();
        String collect = partitioned.stream().map(
            x -> this.formatExpression(x.getColName())
        ).collect(Collectors.joining(","));
        builder.append(collect).append(")");
        List<PartitionDesc> rangePartitions = starRocksPartitionedBy.getRangePartitions();
        if (CollectionUtils.isNotEmpty(rangePartitions)) {
            builder.append("\n");
        }
        Iterator<PartitionDesc> iterator = rangePartitions.iterator();
        builder.append("(");
        if (iterator.hasNext()) {
            builder.append("\n");
            process(iterator.next(), indent + 1);
            while (iterator.hasNext()) {
                builder.append(",\n");
                process(iterator.next(), indent + 1);
            }
        }
        if (CollectionUtils.isNotEmpty(rangePartitions)) {
            builder.append("\n");
        }
        builder.append(")");
        return true;
    }

    @Override
    public Boolean visitListPartitionedBy(ListPartitionedBy listPartitionedBy, Integer indent) {
        builder.append("PARTITION BY LIST (");
        List<ColumnDefinition> partitioned = listPartitionedBy.getColumnDefinitions();
        String collect = partitioned.stream().map(
            x -> formatExpression(x.getColName())
        ).collect(Collectors.joining(","));
        builder.append(collect).append(")");
        if (listPartitionedBy.getListPartitions() != null) {
            builder.append("\n");
            List<PartitionDesc> rangePartitions = listPartitionedBy.getListPartitions();
            appendPartitionDesc(indent, rangePartitions);
        }
        return true;
    }

    private void appendPartitionDesc(Integer indent, List<PartitionDesc> rangePartitions) {
        if (rangePartitions == null) {
            return;
        }
        Iterator<PartitionDesc> iterator = rangePartitions.iterator();
        if (iterator.hasNext()) {
            builder.append("(\n");
            process(iterator.next(), indent + 1);
            while (iterator.hasNext()) {
                builder.append(",\n");
                process(iterator.next(), indent + 1);
            }
            builder.append("\n)");
        }
    }

    @Override
    public Boolean visitExpressionPartitionedBy(ExpressionPartitionBy expressionPartitionedBy, Integer indent) {
        builder.append("PARTITION BY ");
        if (expressionPartitionedBy.getFunctionCall() == null) {
            builder.append("(");
            List<ColumnDefinition> partitioned = expressionPartitionedBy.getColumnDefinitions();
            String collect = partitioned.stream().map(
                x -> formatExpression(x.getColName())
            ).collect(Collectors.joining(","));
            builder.append(collect).append(")");
        } else {
            String function = formatExpression(expressionPartitionedBy.getFunctionCall());
            builder.append(function);
        }
        if (expressionPartitionedBy.getRangePartitions() != null) {
            appendPartitionDesc(indent, expressionPartitionedBy.getRangePartitions());
        }
        return true;
    }

    @Override
    public Boolean visitSingleItemListPartition(SingleItemListPartition singleItemListPartition, Integer context) {
        builder.append("PARTITION ");
        if (singleItemListPartition.isIfNotExists()) {
            builder.append("IF NOT EXISTS ");
        }
        builder.append(formatExpression(singleItemListPartition.getName()));
        builder.append(" VALUES IN (");
        String collect = getListString(singleItemListPartition.getListStringLiteral());
        builder.append(collect);
        builder.append(")");
        appendProperty(singleItemListPartition.getPropertyList());
        return true;
    }

    private void appendProperty(List<Property> propertyList) {
        if (propertyList == null || propertyList.isEmpty()) {
            return;
        }
        builder.append(" ");
        builder.append("(");
        builder.append(formatProperty(propertyList));
        builder.append(")");
    }

    private String getListString(ListStringLiteral listStringLiteral) {
        return listStringLiteral.getStringLiteralList().stream().map(
            this::formatExpression
        ).collect(Collectors.joining(","));
    }

    private String getListString(ListPartitionValue listStringLiteral) {
        return listStringLiteral.getPartitionValueList().stream().map(
            this::formatPartitionValue
        ).collect(Collectors.joining(","));
    }

    private String formatPartitionValue(PartitionValue partitionValue) {
        if (partitionValue.isMaxValue()) {
            return MAXVALUE;
        }
        return formatExpression(partitionValue.getStringLiteral());
    }

    private String formatListPartitionValue(ListPartitionValue listPartitionValue) {
        return listPartitionValue.getPartitionValueList().stream().map(
            p -> {
                return formatPartitionValue(p);
            }
        ).collect(Collectors.joining(","));
    }

    @Override
    public Boolean visitMultiItemListPartition(MultiItemListPartition multiItemListPartition, Integer context) {
        builder.append("PARTITION ");
        if (multiItemListPartition.isIfNotExists()) {
            builder.append("IF NOT EXISTS ");
        }
        builder.append(formatExpression(multiItemListPartition.getName()));
        builder.append(" VALUES IN (");
        String collect = multiItemListPartition.getListStringLiterals().stream().map(
            x -> "(" + getListString(x) + ")"
        ).collect(Collectors.joining(","));
        builder.append(collect);
        builder.append(")");
        appendProperty(multiItemListPartition.getPropertyList());
        return true;
    }

    @Override
    public Boolean visitOrderByConstraint(OrderByNonKeyConstraint orderByConstraint, Integer context) {
        builder.append("ORDER BY (");
        List<Identifier> colNames = orderByConstraint.getColumns();
        builder.append(colNames.stream().map(this::formatExpression).collect(Collectors.joining(",")));
        builder.append(")");
        return true;
    }

    @Override
    public Boolean visitDistributeKeyConstraint(DistributeNonKeyConstraint distributeKeyConstraint, Integer context) {
        builder.append("DISTRIBUTED BY ");
        boolean random = BooleanUtils.isTrue(distributeKeyConstraint.getRandom());
        if (random) {
            builder.append("RANDOM");
        } else {
            builder.append("HASH");
        }
        if (CollectionUtils.isNotEmpty(distributeKeyConstraint.getColumns())) {
            List<Identifier> colNames = distributeKeyConstraint.getColumns();
            builder.append("(");
            builder.append(colNames.stream().map(this::formatExpression).collect(Collectors.joining(",")));
            builder.append(")");
        }
        if (distributeKeyConstraint.getBucket() != null) {
            builder.append(" BUCKETS ");
            builder.append(distributeKeyConstraint.getBucket());
        } else if (BooleanUtils.isTrue(distributeKeyConstraint.getAuto())) {
            builder.append(" BUCKETS AUTO");
        }
        return true;
    }

    @Override
    public Boolean visitSingleRangePartition(SingleRangePartition singleRangePartition, Integer context) {
        String s = indentString(context);
        builder.append(s).append("PARTITION ");
        if (singleRangePartition.isIfNotExists()) {
            builder.append("IF NOT EXISTS ");
        }
        builder.append(formatExpression(singleRangePartition.getName()));
        builder.append(" VALUES ");
        process(singleRangePartition.getPartitionKey());

        if (singleRangePartition.getPropertyList() != null) {
            String property = formatProperty(singleRangePartition.getPropertyList());
            builder.append(property);
        }
        return true;
    }

    @Override
    public Boolean visitLessThanPartitionKey(LessThanPartitionKey lessThanPartitionKey, Integer context) {
        builder.append("LESS THAN ");
        if (lessThanPartitionKey.isMaxValue()) {
            builder.append(MAXVALUE);
        } else {
            String collect = lessThanPartitionKey.getPartitionValues().getPartitionValueList().stream()
                .map(this::formatPartitionValue)
                .collect(Collectors.joining(","));
            builder.append("(");
            builder.append(collect);
            builder.append(")");
        }
        return true;
    }

    @Override
    public Boolean visitArrayPartitionKey(ArrayPartitionKey arrayPartitionKey, Integer context) {
        builder.append("[");
        String value = arrayPartitionKey.getPartitionValues().stream().map(
            p -> "(" + getListString(p) + ")"
        ).collect(Collectors.joining(","));
        builder.append(value);
        builder.append(")");
        return true;
    }

    @Override
    public Boolean visitMultiRangePartition(MultiRangePartition multiRangePartition, Integer context) {
        String s = indentString(context);
        builder.append(s).append("START");
        builder.append("(").append(formatListPartitionValue(multiRangePartition.getStart()));
        builder.append(")");
        builder.append(" ");
        builder.append("END");
        builder.append("(");
        builder.append(formatListPartitionValue(multiRangePartition.getEnd()));
        builder.append(")");
        builder.append(" EVERY (");
        if (multiRangePartition.getIntervalLiteral() != null) {
            builder.append(formatExpression(multiRangePartition.getIntervalLiteral()));
        } else if (multiRangePartition.getLongLiteral() != null) {
            builder.append(formatExpression(multiRangePartition.getLongLiteral()));
        }
        builder.append(")");
        return true;
    }

    @Override
    public Boolean visitListPartitionValue(ListPartitionValue listPartitionValue, Integer context) {
        builder.append(formatListPartitionValue(listPartitionValue));
        return true;
    }
}
