package com.aliyun.fastmodel.transform.starrocks.format;

import com.aliyun.fastmodel.core.formatter.FastModelVisitor;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.atom.FunctionCall;
import com.aliyun.fastmodel.core.tree.expr.literal.BooleanLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.ListStringLiteral;
import com.aliyun.fastmodel.core.tree.statement.table.AddCols;
import com.aliyun.fastmodel.core.tree.statement.table.ChangeCol;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.DropCol;
import com.aliyun.fastmodel.core.tree.statement.table.RenameCol;
import com.aliyun.fastmodel.core.tree.statement.table.RenameTable;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableProperties;
import com.aliyun.fastmodel.core.tree.statement.table.UnSetTableProperties;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.PrimaryConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.UniqueConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.index.TableIndex;
import com.aliyun.fastmodel.core.tree.util.PropertyUtil;
import com.aliyun.fastmodel.transform.starrocks.context.StarRocksContext;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.constraint.AggregateKeyConstraint;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.constraint.DuplicateKeyConstraint;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.constraint.desc.DistributeConstraint;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.constraint.desc.NonKeyConstraint;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.constraint.desc.OrderByConstraint;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.constraint.desc.RollupConstraint;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.constraint.desc.RollupItem;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.ArrayPartitionKey;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.ExpressionPartitionBy;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.LessThanPartitionKey;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.ListPartitionValue;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.ListPartitionedBy;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.MultiItemListPartition;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.MultiRangePartition;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.PartitionDesc;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.PartitionValue;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.RangePartitionedBy;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.SingleItemListPartition;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.SingleRangePartition;
import com.aliyun.fastmodel.transform.starrocks.parser.visitor.StarRocksAstVisitor;
import com.google.common.base.Objects;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * StarRocksVisitor
 *
 * @author panguanjing
 * @date 2023/9/5
 */
public class StarRocksOutVisitor extends FastModelVisitor implements StarRocksAstVisitor<Boolean, Integer> {

    public static final String MAXVALUE = "MAXVALUE";
    private final StarRocksContext starRocksContext;

    public StarRocksOutVisitor(StarRocksContext context) {
        this.starRocksContext = context;
    }

    @Override
    public Boolean visitCreateTable(CreateTable node, Integer indent) {
        boolean columnEmpty = node.isColumnEmpty();
        //maxcompute不支持没有列的表
        boolean executable = !columnEmpty;
        builder.append("CREATE TABLE ");
        if (node.isNotExists()) {
            builder.append("IF NOT EXISTS ");
        }
        String tableName = getCode(node.getQualifiedName());
        builder.append(tableName);
        if (!columnEmpty) {
            builder.append("\n(\n");
            String elementIndent = indentString(indent + 1);
            String columnList = formatColumnList(node.getColumnDefines(), elementIndent);
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
                p -> StringUtils.equalsIgnoreCase(p.getName(), StarRocksProperty.TABLE_ENGINE.getValue())).findFirst();
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
            String propertyValue = PropertyUtil.getPropertyValue(node.getProperties(), StarRocksProperty.TABLE_PARTITION_RAW.getValue());
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

    @Override
    protected String formatProperty(List<Property> properties) {
        List<Property> collect = properties.stream().filter(
            p -> {
                StarRocksProperty byValue = StarRocksProperty.getByValue(p.getName());
                return byValue == null || byValue.isSupportPrint();
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
        String charSet = PropertyUtil.getPropertyValue(columnProperties, StarRocksProperty.COLUMN_CHAR_SET.getValue());
        if (StringUtils.isNotBlank(charSet)) {
            sb.append(" ").append("CHARSET ").append(charSet);
        }
        //key
        String key = PropertyUtil.getPropertyValue(columnProperties, StarRocksProperty.COLUMN_KEY.getValue());
        if (StringUtils.isNotBlank(key)) {
            sb.append(" ").append("KEY");
        }
        //aggDesc
        String propertyValue = PropertyUtil.getPropertyValue(columnProperties, StarRocksProperty.COLUMN_AGG_DESC.getValue());
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
        String autoIncrement = PropertyUtil.getPropertyValue(columnProperties, StarRocksProperty.COLUMN_AUTO_INCREMENT.getValue());
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
    public Boolean visitRollupConstraint(RollupConstraint rollupConstraint, Integer context) {
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
        builder.append("ALTER TABLE ").append(getCode(renameCol.getQualifiedName()));
        builder.append(" MODIFY COLUMN");
        builder.append(" ").append(formatColumnDefinition(columnDefinition, 0));
        return true;
    }

    @Override
    public Boolean visitDropCol(DropCol dropCol, Integer context) {
        return super.visitDropCol(dropCol, context);
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
        builder.append("ALTER TABLE ").append(getCode(addCols.getQualifiedName()));
        builder.append(" ADD COLUMN\n").append('(').append("\n");
        String columnList = formatColumnList(addCols.getColumnDefineList(), indentString(context + 1));
        builder.append(columnList);
        builder.append("\n");
        builder.append(')');
        return true;
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
            StarRocksProperty tableIndexType = StarRocksProperty.TABLE_INDEX_TYPE;
            return StringUtils.equalsIgnoreCase(p.getName(), tableIndexType.getValue());
        }).findFirst();
        first.ifPresent(property -> builder.append(" USING ").append(property.getValue()));

        Optional<Property> comment = properties.stream().filter(p -> {
            StarRocksProperty tableIndexType = StarRocksProperty.TABLE_INDEX_COMMENT;
            return StringUtils.equalsIgnoreCase(p.getName(), tableIndexType.getValue());
        }).findFirst();
        comment.ifPresent(property -> builder.append(" COMMENT ").append(formatStringLiteral(property.getValue())));
        return true;
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
        builder.append("\n");
        List<PartitionDesc> rangePartitions = listPartitionedBy.getListPartitions();
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
        return true;
    }

    @Override
    public Boolean visitExpressionPartitionedBy(ExpressionPartitionBy expressionPartitionedBy, Integer indent) {
        builder.append("PARTITION BY ");
        List<ColumnDefinition> partitioned = expressionPartitionedBy.getColumnDefinitions();
        String col = partitioned.stream().map(
                x -> formatExpression(x.getColName())
        ).collect(Collectors.joining(","));
        if (expressionPartitionedBy.getFunctionCall() == null) {
            // 列表达式
            builder.append("(").append(col).append(")");
        } else {
            // 时间函数表达式
            FunctionCall functionCall = expressionPartitionedBy.getFunctionCall();
            String functionName = functionCall.getFuncName().getFirst();
            builder.append(functionName).append("(");
            if (TimeFunctionType.DATE_TRUNC.getValue().equalsIgnoreCase(functionName)) {
                builder.append(expressionPartitionedBy.getTimeUnitArg().getOrigin()).append(", ");
                builder.append(col).append(")");
            } else if (TimeFunctionType.TIME_SLICE.getValue().equalsIgnoreCase(functionName)) {
                builder.append(col).append(", ");
                builder.append(formatExpression(expressionPartitionedBy.getIntervalLiteralArg())).append(")");
            }
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
    public Boolean visitOrderByConstraint(OrderByConstraint orderByConstraint, Integer context) {
        builder.append("ORDER BY (");
        List<Identifier> colNames = orderByConstraint.getColumns();
        builder.append(colNames.stream().map(this::formatExpression).collect(Collectors.joining(",")));
        builder.append(")");
        return true;
    }

    @Override
    public Boolean visitDistributeKeyConstraint(DistributeConstraint distributeKeyConstraint, Integer context) {
        builder.append("DISTRIBUTED BY ");
        boolean random = BooleanUtils.isTrue(distributeKeyConstraint.getRandom());
        if (random) {
            builder.append("RANDOM");
        } else {
            builder.append("HASH");
        }
        if (CollectionUtils.isNotEmpty(distributeKeyConstraint.getColumns())) {
            List<Identifier> colNames = distributeKeyConstraint.getColumns();
            builder.append(" (");
            builder.append(colNames.stream().map(this::formatExpression).collect(Collectors.joining(",")));
            builder.append(")");
        }
        if (distributeKeyConstraint.getBucket() != null) {
            builder.append(" BUCKETS ");
            builder.append(distributeKeyConstraint.getBucket());
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
        builder.append("(").append(formatExpression(multiRangePartition.getStart()));
        builder.append(")");
        builder.append(" ");
        builder.append("END");
        builder.append("(");
        builder.append(formatExpression(multiRangePartition.getEnd()));
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
}
