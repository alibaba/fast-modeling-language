package com.aliyun.fastmodel.transform.flink.format;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.core.formatter.FastModelVisitor;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.PrimaryConstraint;
import com.aliyun.fastmodel.core.tree.util.PropertyUtil;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.WaterMarkConstraint;
import com.aliyun.fastmodel.transform.flink.context.FlinkTransformContext;
import com.aliyun.fastmodel.transform.flink.parser.visitor.FlinkAstVisitor;
import com.aliyun.fastmodel.transform.flink.parser.visitor.FlinkExpressionAstVisitor;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

import static com.aliyun.fastmodel.transform.api.extension.client.property.ExtensionPropertyKey.TABLE_PARTITION_RAW;

/**
 * @author 子梁
 * @date 2024/5/22
 */
public class FlinkOutVisitor extends FastModelVisitor implements FlinkAstVisitor<Boolean, Integer> {

    private final FlinkTransformContext context;

    public FlinkOutVisitor(FlinkTransformContext context) {
        this.context = context;
    }

    @Override
    public Boolean visitCreateTable(CreateTable node, Integer indent) {
        builder.append("CREATE TABLE ");
        if (node.isNotExists()) {
            builder.append("IF NOT EXISTS ");
        }
        String tableName = getCode(node.getQualifiedName());
        builder.append(tableName);
        boolean columnEmpty = node.isColumnEmpty();
        if (!columnEmpty) {
            builder.append("\n(\n");
            String elementIndent = indentString(indent + 1);
            String columnList = formatColumnList(node.getColumnDefines(), elementIndent);
            builder.append(columnList);

            // constraint
            List<BaseConstraint> constraints = node.getConstraintStatements();
            if (CollectionUtils.isNotEmpty(constraints)) {
                // watermark definition
                Optional<BaseConstraint> waterMarkConstraintOpt = constraints.stream()
                    .filter(constraint -> constraint instanceof WaterMarkConstraint).findAny();
                waterMarkConstraintOpt.ifPresent(constraint -> builder.append(",\n").append(elementIndent).append(formatWaterMarkConstraint((WaterMarkConstraint) constraint, indent)));

                // primary key
                Optional<BaseConstraint> primaryConstraint = constraints.stream()
                    .filter(constraint -> constraint instanceof PrimaryConstraint).findAny();
                primaryConstraint.ifPresent(constraint -> {
                    builder.append(",\n").append(elementIndent);
                    process(constraint, indent);
                });
            }
            builder.append("\n").append(")");
        }

        if (node.getComment() != null) {
            builder.append("\n");
            builder.append(formatComment(node.getComment(), true));
        }

        // partitioned by
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

        if (!node.isPropertyEmpty()) {
            String prop = formatProperty(node.getProperties());
            if (StringUtils.isNotBlank(prop)) {
                builder.append("\n");
                builder.append("WITH (");
                builder.append(prop);
                builder.append(")");
            }
        }
        builder.append(";");

        //不支持没有列的表
        return !columnEmpty;
    }

    @Override
    public String formatColumnDefinition(ColumnDefinition column, Integer max) {
        List<Property> columnProperties = column.getColumnProperties();
        if (CollectionUtils.isEmpty(columnProperties)) {
            return formatPhysicalColumnDefinition(column, max);
        }

        Map<String, String> propertyMap = PropertyUtil.toMap(columnProperties);
        if (propertyMap.containsKey(FlinkColumnPropertyKey.METADATA.getValue())
            && BooleanUtils.toBoolean(propertyMap.get(FlinkColumnPropertyKey.METADATA.getValue()))) {
            return formatMetadataColumnDefinition(column, max);
        }

        if (propertyMap.containsKey(FlinkColumnPropertyKey.COMPUTED.getValue())
            && BooleanUtils.toBoolean(propertyMap.get(FlinkColumnPropertyKey.COMPUTED.getValue()))) {
            return formatComputedColumnDefinition(column, max);
        }

        return formatPhysicalColumnDefinition(column, max);
    }

    @Override
    public String formatExpression(BaseExpression baseExpression) {
        return new FlinkExpressionAstVisitor(context).process(baseExpression);
    }

    @Override
    public Boolean visitPartitionedBy(PartitionedBy partitionedBy, Integer context) {
        builder.append("PARTITIONED BY (");

        List<ColumnDefinition> partitioned = partitionedBy.getColumnDefinitions();
        String collect = partitioned.stream().map(
            x -> formatExpression(x.getColName())
        ).collect(Collectors.joining(", "));
        builder.append(collect);

        builder.append(")");
        return true;
    }

    private String formatPhysicalColumnDefinition(ColumnDefinition column, Integer max) {
        String col = formatColName(column.getColName(), max);
        StringBuilder sb = new StringBuilder().append(col);

        BaseDataType dataType = column.getDataType();
        if (dataType != null) {
//            sb.append(" ").append(formatDataType(dataType));
            sb.append(" ").append(dataType.getOrigin());
        }

        boolean isPrimary = column.getPrimary() != null && column.getPrimary();
        if (isPrimary) {
            sb.append(" PRIMARY KEY");
        }
        boolean isNotNull = column.getNotNull() != null && column.getNotNull();
        if (!isPrimary && isNotNull) {
            sb.append(" NOT NULL");
        } else if (BooleanUtils.isFalse(column.getNotNull())) {
            sb.append(" NULL");
        }

        sb.append(formatComment(column.getComment(), isEndNewLine(sb.toString())));
        return sb.toString();
    }

    private String formatMetadataColumnDefinition(ColumnDefinition column, Integer max) {
        String col = formatColName(column.getColName(), max);
        StringBuilder sb = new StringBuilder().append(col);

        BaseDataType dataType = column.getDataType();
        if (dataType != null) {
            sb.append(" ").append(formatDataType(dataType));
        }
        sb.append(" METADATA");

        Map<String, String> propertyMap = PropertyUtil.toMap(column.getColumnProperties());
        if (propertyMap.containsKey(FlinkColumnPropertyKey.METADATA_KEY.getValue())) {
            String metaDataKey = propertyMap.get(FlinkColumnPropertyKey.METADATA_KEY.getValue());
            sb.append(" FROM ").append(metaDataKey);
        }

        if (propertyMap.containsKey(FlinkColumnPropertyKey.VIRTUAL.getValue())) {
            sb.append(" VIRTUAL");
        }

        return sb.toString();
    }

    private String formatComputedColumnDefinition(ColumnDefinition column, Integer max) {
        String col = formatColName(column.getColName(), max);
        StringBuilder sb = new StringBuilder().append(col).append(" AS");

        List<Property> columnProperties = column.getColumnProperties();
        Optional<Property> expressionProperty = columnProperties.stream()
            .filter(property -> StringUtils.equalsIgnoreCase(FlinkColumnPropertyKey.COMPUTED_COLUMN_EXPRESSION.getValue(), property.getName()))
            .findAny();
        expressionProperty.ifPresent(p -> sb.append(" ").append(p.getValueLiteral().getOrigin()));

        sb.append(formatComment(column.getComment(), isEndNewLine(sb.toString())));
        return sb.toString();
    }

    private String formatWaterMarkConstraint(WaterMarkConstraint constraint, Integer indent) {
        StringBuilder sb = new StringBuilder().append("WATERMARK FOR ");

        String col = formatColName(constraint.getColumn(), indent);
        sb.append(col);

        sb.append(" AS ");

        StringLiteral expression = constraint.getExpression();
        sb.append(expression.getOrigin());

        return sb.toString();
    }

}
