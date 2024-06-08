package com.aliyun.fastmodel.transform.starrocks.format;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.core.formatter.ExpressionVisitor;
import com.aliyun.fastmodel.core.formatter.FastModelVisitor;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
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
import com.aliyun.fastmodel.transform.starrocks.parser.tree.AggregateConstraint;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.DuplicateConstraint;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.ArrayPartitionKey;
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
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

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

        //constraint
        if (!node.isConstraintEmpty()) {
            appendConstraint(node, indent);
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

        //distribute desc
        appendDistributeBy(node.getProperties());


        if (!node.isPropertyEmpty()) {
            String prop = formatProperty(node.getProperties());
            if (StringUtils.isNotBlank(prop)) {
                builder.append("\n");
                builder.append("PROPERTIES (");
                builder.append(prop);
                builder.append(")");
            }
        }
        return executable;
    }

    private void appendDistributeBy(List<Property> properties) {
        String propertyValue = PropertyUtil.getPropertyValue(properties, StarRocksProperty.TABLE_DISTRIBUTED_HASH.getValue());
        if (StringUtils.isBlank(propertyValue)) {
            return;
        }
        builder.append("\n");
        builder.append("DISTRIBUTED BY HASH");
        String formatPropertyValue = formatIdentifierListValue(propertyValue);
        builder.append("(").append(formatPropertyValue).append(")");

        String buckets = PropertyUtil.getPropertyValue(properties, StarRocksProperty.TABLE_DISTRIBUTED_BUCKETS.getValue());
        if (StringUtils.isNotBlank(buckets)) {
            builder.append(" BUCKETS ");
            builder.append(buckets);
        }
    }

    private String formatIdentifierListValue(String propertyValue) {
        return Lists.newArrayList(propertyValue.split(",")).stream()
            .map(Identifier::new).map(this::formatExpression).collect(Collectors.joining(","));
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
            sb.append(" DEFAULT ").append(formatExpression(column.getDefaultValue()));
        }
        sb.append(formatComment(column.getComment(), isEndNewLine(sb.toString())));
        return sb.toString();
    }

    private void appendConstraint(CreateTable node, Integer indent) {
        List<BaseConstraint> constraintStatements = node.getConstraintStatements();
        if (CollectionUtils.isEmpty(constraintStatements)) {
            return;
        }
        for (BaseConstraint baseConstraint : node.getConstraintStatements()) {
            builder.append("\n");
            process(baseConstraint, indent);
        }
    }

    @Override
    public Boolean visitAggregateConstraint(AggregateConstraint aggregateConstraint, Integer context) {
        builder.append("AGGREGATE KEY (");
        List<Identifier> colNames = aggregateConstraint.getColumns();
        builder.append(colNames.stream().map(this::formatExpression).collect(Collectors.joining(",")));
        builder.append(")");
        return true;
    }

    @Override
    public Boolean visitDuplicateConstraint(DuplicateConstraint duplicateConstraint, Integer context) {
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
        List<PartitionDesc> rangePartitions = listPartitionedBy.getRangePartitions();
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
