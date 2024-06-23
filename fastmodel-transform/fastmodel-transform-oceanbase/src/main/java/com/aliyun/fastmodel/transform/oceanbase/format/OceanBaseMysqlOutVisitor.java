package com.aliyun.fastmodel.transform.oceanbase.format;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.core.formatter.FastModelVisitor;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.atom.FunctionCall;
import com.aliyun.fastmodel.core.tree.expr.literal.BooleanLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.LongLiteral;
import com.aliyun.fastmodel.core.tree.statement.table.AddCols;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableProperties;
import com.aliyun.fastmodel.core.tree.statement.table.UnSetTableProperties;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.index.IndexColumnName;
import com.aliyun.fastmodel.core.tree.statement.table.index.IndexExpr;
import com.aliyun.fastmodel.core.tree.statement.table.index.IndexSortKey;
import com.aliyun.fastmodel.core.tree.statement.table.index.TableIndex;
import com.aliyun.fastmodel.core.tree.util.IdentifierUtil;
import com.aliyun.fastmodel.core.tree.util.PropertyUtil;
import com.aliyun.fastmodel.transform.api.extension.client.property.ExtensionPropertyKey;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.CheckExpressionConstraint;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.ForeignKeyConstraint;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.UniqueKeyExprConstraint;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.desc.SingleRangePartition;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.keyvalue.LessThanPartitionKey;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.keyvalue.ListPartitionValue;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.keyvalue.PartitionValue;
import com.aliyun.fastmodel.transform.api.format.PropertyValueType;
import com.aliyun.fastmodel.transform.oceanbase.context.OceanBaseContext;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.OceanBaseHashPartitionBy;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.OceanBaseKeyPartitionBy;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.OceanBaseListPartitionBy;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.OceanBaseRangePartitionBy;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.SubHashPartition;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.SubKeyPartition;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.SubListPartition;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.SubRangePartition;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.element.HashPartitionElement;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.element.ListPartitionElement;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.element.RangePartitionElement;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.element.SubHashPartitionElement;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.element.SubListPartitionElement;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.element.SubPartitionElement;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.element.SubPartitionList;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.element.SubRangePartitionElement;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.template.SubHashTemplatePartition;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.template.SubKeyTemplatePartition;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.template.SubListTemplatePartition;
import com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.template.SubRangeTemplatePartition;
import com.aliyun.fastmodel.transform.oceanbase.parser.visitor.OceanBaseMysqlAstVisitor;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

import static com.aliyun.fastmodel.transform.api.extension.client.property.ExtensionPropertyKey.COLUMN_AUTO_INCREMENT;
import static com.aliyun.fastmodel.transform.api.extension.client.property.ExtensionPropertyKey.TABLE_PARTITION_RAW;
import static java.util.stream.Collectors.joining;

/**
 * out visitor
 *
 * @author panguanjing
 * @date 2024/2/2
 */
public class OceanBaseMysqlOutVisitor extends FastModelVisitor implements OceanBaseMysqlAstVisitor<Boolean, Integer> {
    private final OceanBaseContext context;

    public OceanBaseMysqlOutVisitor(OceanBaseContext context) {
        this.context = context;
    }

    @Override
    public Boolean visitCreateTable(CreateTable node, Integer indent) {
        builder.append("CREATE");
        if (node.getCreateOrReplace() != null && node.getCreateOrReplace()) {
            builder.append(" OR REPLACE");
        }
        builder.append(" TABLE ");
        if (node.isNotExists()) {
            builder.append("IF NOT EXISTS ");
        }
        String tableName = getCode(node.getQualifiedName());
        builder.append(tableName);
        int newIndent = indent + 1;
        if (!node.isColumnEmpty()) {
            builder.append(" \n(\n");
            String elementIndent = indentString(newIndent);
            builder.append(formatColumnList(node.getColumnDefines(), elementIndent));
            if (!node.isConstraintEmpty()) {
                Iterator<BaseConstraint> iterator = node.getConstraintStatements().stream()
                    .iterator();
                while (iterator.hasNext()) {
                    builder.append(",\n");
                    process(iterator.next(), newIndent);
                }
            }
            if (!node.isIndexEmpty()) {
                Iterator<TableIndex> iterator = node.getTableIndexList().iterator();
                while (iterator.hasNext()) {
                    builder.append(",\n");
                    process(iterator.next(), newIndent);
                }
            }
            builder.append(newLine(")"));
        }
        if (node.getComment() != null) {
            builder.append(formatComment(node.getComment(), isEndNewLine(builder.toString())));
        }
        if (node.getPartitionedBy() != null) {
            if (!isEndNewLine(builder.toString())) {
                builder.append("\n");
            }
            PartitionedBy partitionedBy = node.getPartitionedBy();
            process(partitionedBy);
        } else {
            String propertyValue = PropertyUtil.getPropertyValue(node.getProperties(), TABLE_PARTITION_RAW.getValue());
            if (StringUtils.isNotBlank(propertyValue)) {
                if (!isEndNewLine(builder.toString())) {
                    builder.append("\n");
                }
                builder.append(propertyValue);
            }
        }
        List<Property> properties = node.getProperties();
        builder.append(formatProperty(properties));
        removeNewLine(builder);
        return true;
    }

    @Override
    protected String formatColumnDefinition(ColumnDefinition column, Integer max) {
        BaseDataType dataType = column.getDataType();
        String col = formatColName(column.getColName(), max);
        StringBuilder sb = new StringBuilder().append(col);
        if (dataType != null) {
            sb.append(" ").append(formatDataType(dataType));
        }
        boolean isPrimary = column.getPrimary() != null && column.getPrimary();
        if (isPrimary) {
            sb.append(" PRIMARY KEY");
        }
        List<Property> columnProperties = column.getColumnProperties();
        boolean isNotNull = column.getNotNull() != null && column.getNotNull();
        if (isNotNull) {
            sb.append(" NOT NULL");
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

    /**
     * table_option
     * 配置信息
     *
     * @param properties
     * @return
     */
    @Override
    protected String formatProperty(List<Property> properties) {
        if (properties == null) {
            return StringUtils.EMPTY;
        }
        StringBuilder stringBuilder = new StringBuilder();
        for (Property property : properties) {
            String name = property.getName();
            ExtensionPropertyKey byValue = ExtensionPropertyKey.getByValue(name);
            if (byValue != null && !byValue.isSupportPrint()) {
                continue;
            }
            if (StringUtils.equalsIgnoreCase(name, OceanBasePropertyKey.COMMENT.getValue())) {
                continue;
            }
            if (StringUtils.equalsIgnoreCase(name, OceanBasePropertyKey.SORT_KEY.getValue())) {
                stringBuilder.append(OceanBasePropertyKey.SORT_KEY.getValue()).append("(");
                stringBuilder.append(property.getValue());
                stringBuilder.append(")");
                stringBuilder.append(StringUtils.LF);
                continue;
            }
            OceanBasePropertyKey oceanBasePropertyKey = OceanBasePropertyKey.getByValue(name);
            if (oceanBasePropertyKey == null) {
                stringBuilder.append(name);
                stringBuilder.append("=");
                stringBuilder.append(formatStringLiteral(property.getValue()));
                stringBuilder.append(StringUtils.LF);
                continue;
            }
            if (StringUtils.equalsIgnoreCase(name, OceanBasePropertyKey.CHARSET_KEY.getValue())) {
                stringBuilder.append("DEFAULT");
                stringBuilder.append(" CHARSET ").append(property.getValue());
                stringBuilder.append(StringUtils.LF);
                continue;
            }
            if (StringUtils.equalsIgnoreCase(name, OceanBasePropertyKey.COLLATE.getValue())) {
                stringBuilder.append("COLLATE ");
                stringBuilder.append(property.getValue());
                stringBuilder.append(StringUtils.LF);
                continue;
            }
            PropertyValueType valueType = oceanBasePropertyKey.getValueType();
            if (valueType == PropertyValueType.IDENTIFIER || valueType == PropertyValueType.NUMBER_LITERAL) {
                stringBuilder.append(name);
                stringBuilder.append(" ");
                stringBuilder.append(property.getValue());
                stringBuilder.append(StringUtils.LF);
                continue;
            }
            if (valueType == PropertyValueType.STRING_LITERAL) {
                stringBuilder.append(name);
                stringBuilder.append(" ");
                stringBuilder.append(formatStringLiteral(property.getValue()));
                stringBuilder.append(StringUtils.LF);
            }
        }
        return stringBuilder.toString();
    }

    @Override
    public Boolean visitOceanBaseHashPartitionBy(OceanBaseHashPartitionBy oceanBaseHashPartitionBy, Integer context) {
        // PARTITION BY HASH LeftParen expr RightParen subpartition_option? (PARTITIONS INTNUM)? opt_hash_partition_list?
        builder.append("PARTITION BY HASH (");
        String expr = formatExpression(oceanBaseHashPartitionBy.getExpression());
        builder.append(expr);
        builder.append(")");
        if (oceanBaseHashPartitionBy.getSubPartition() != null) {
            builder.append(" ");
            process(oceanBaseHashPartitionBy.getSubPartition());
        }
        appendPartitionCount(oceanBaseHashPartitionBy.getPartitionCount());
        appendHashPartitionElements(oceanBaseHashPartitionBy.getHashPartitionElements());
        return true;
    }

    private void appendHashPartitionElements(List<HashPartitionElement> hashPartitionElements) {
        if (!CollectionUtils.isNotEmpty(hashPartitionElements)) {
            return;
        }
        builder.append("\n(");
        Iterator<HashPartitionElement> iterator = hashPartitionElements.iterator();
        HashPartitionElement next = iterator.next();
        process(next);
        while (iterator.hasNext()) {
            builder.append(",\n");
            next = iterator.next();
            process(next);
        }
        builder.append(")");
    }

    private void appendPartitionCount(LongLiteral partitionCount) {
        if (partitionCount != null) {
            builder.append(" PARTITIONS ");
            builder.append(partitionCount.getValue());
        }
    }

    @Override
    public Boolean visitOceanBaseRangePartitionBy(OceanBaseRangePartitionBy oceanBaseRangePartitionBy, Integer context) {
        /**
         *  PARTITION BY RANGE LeftParen expr RightParen subpartition_option? (PARTITIONS INTNUM)? opt_range_partition_list
         *  | PARTITION BY RANGE COLUMNS LeftParen column_name_list RightParen subpartition_option? (PARTITIONS INTNUM)? opt_range_partition_list
         */
        builder.append("PARTITION BY RANGE ");
        if (oceanBaseRangePartitionBy.getBaseExpression() != null) {
            builder.append("(");
            String expr = formatExpression(oceanBaseRangePartitionBy.getBaseExpression());
            builder.append(expr);
            builder.append(")");
        }
        if (oceanBaseRangePartitionBy.getColumnDefinitions() != null) {
            appendColumnList(oceanBaseRangePartitionBy.getColumnDefinitions().stream().map(x -> x.getColName()).collect(Collectors.toList()));
        }
        if (oceanBaseRangePartitionBy.getSubPartition() != null) {
            builder.append(" ");
            process(oceanBaseRangePartitionBy.getSubPartition());
        }
        appendPartitionCount(oceanBaseRangePartitionBy.getPartitionCount());
        if (CollectionUtils.isNotEmpty(oceanBaseRangePartitionBy.getSingleRangePartitionList())) {
            int index = 1;
            builder.append("\n(");
            List<RangePartitionElement> singleRangePartitionList = oceanBaseRangePartitionBy.getSingleRangePartitionList();
            Iterator<RangePartitionElement> iterator = singleRangePartitionList.iterator();
            RangePartitionElement next = iterator.next();
            process(next, index);
            while (iterator.hasNext()) {
                builder.append(",\n");
                next = iterator.next();
                process(next, index);
            }
            builder.append(")");
        }
        return true;
    }

    private void appendColumnList(List<Identifier> columnList) {
        builder.append("COLUMNS (");
        String columns = columnList.stream().map(
            c -> formatExpression(c)
        ).collect(Collectors.joining(","));
        builder.append(columns);
        builder.append(")");
    }

    @Override
    public Boolean visitRangePartitionElement(RangePartitionElement rangePartitionElement, Integer context) {
        //PARTITION relation_factor VALUES LESS THAN range_partition_expr (ID INTNUM)? partition_attributes_option? subpartition_list?
        builder.append("PARTITION");
        SingleRangePartition singleRangePartition = rangePartitionElement.getSingleRangePartition();
        builder.append(" ").append(formatExpression(singleRangePartition.getName()));
        builder.append(" VALUES LESS THAN ");
        process(rangePartitionElement.getSingleRangePartition().getPartitionKey());
        if (rangePartitionElement.getIdCount() != null) {
            builder.append(" ID ").append(rangePartitionElement.getIdCount().getValue());
        }
        if (rangePartitionElement.getSingleRangePartition().getPropertyList() != null) {
            List<Property> properties = rangePartitionElement.getSingleRangePartition().getPropertyList();
            String attribute = properties.stream().map(p -> {
                return p.getName() + "=" + p.getValue();
            }).collect(Collectors.joining(","));
            builder.append(attribute);
        }
        if (rangePartitionElement.getSubPartitionList() != null) {
            process(rangePartitionElement.getSubPartitionList());
        }
        return true;
    }

    @Override
    public Boolean visitLessThanPartitionKey(LessThanPartitionKey lessThanPartitionKey, Integer context) {
        if (lessThanPartitionKey.isMaxValue()) {
            builder.append("MAXVALUE");
        } else {
            ListPartitionValue partitionValues = lessThanPartitionKey.getPartitionValues();
            builder.append("(");
            List<PartitionValue> partitionValueList = partitionValues.getPartitionValueList();
            String collect = partitionValueList.stream().map(p -> {
                if (p.isMaxValue()) {
                    return "MAXVALUE";
                }
                return formatExpression(p.getStringLiteral());
            }).collect(Collectors.joining(","));
            builder.append(collect);
            builder.append(")");
        }
        return true;
    }

    @Override
    public Boolean visitOceanBaseListPartitionBy(OceanBaseListPartitionBy oceanBaseListPartitionBy, Integer context) {
        //PARTITION BY BISON_LIST LeftParen expr RightParen subpartition_option? (PARTITIONS INTNUM)? opt_list_partition_list
        //    | PARTITION BY BISON_LIST COLUMNS LeftParen column_name_list RightParen subpartition_option? (PARTITIONS INTNUM)?
        //    opt_list_partition_list
        builder.append("PARTITION BY LIST ");
        BaseExpression baseExpression = oceanBaseListPartitionBy.getBaseExpression();
        if (baseExpression != null) {
            builder.append("(");
            String e = formatExpression(baseExpression);
            builder.append(e);
            builder.append(")");
        } else {
            builder.append("COLUMNS(");
            String columnNameList = oceanBaseListPartitionBy.getColumnDefinitions().stream().map(c -> {
                return formatColName(c.getColName(), 0);
            }).collect(Collectors.joining(","));
            builder.append(columnNameList);
            builder.append(")");
        }
        if (oceanBaseListPartitionBy.getSubPartition() != null) {
            builder.append(" ");
            process(oceanBaseListPartitionBy.getSubPartition());
        }
        appendPartitionCount(oceanBaseListPartitionBy.getPartitionCount());
        int index = 1;
        if (oceanBaseListPartitionBy.getPartitionElementList() != null) {
            builder.append("\n(");
            Iterator<ListPartitionElement> iterator = oceanBaseListPartitionBy.getPartitionElementList().iterator();
            ListPartitionElement next = iterator.next();
            process(next, index);
            while (iterator.hasNext()) {
                builder.append(",\n");
                next = iterator.next();
                process(next, index);
            }
            builder.append(")");
        }
        return true;
    }

    @Override
    public Boolean visitOceanBaseKeyPartitionBy(OceanBaseKeyPartitionBy oceanBaseKeyPartitionBy, Integer context) {
        //PARTITION BY KEY LeftParen column_name_list? RightParen subpartition_option? (PARTITIONS INTNUM)? opt_hash_partition_list?
        builder.append("PARTITION BY KEY (");
        if (oceanBaseKeyPartitionBy.getColumnDefinitions() != null) {
            String columnList = oceanBaseKeyPartitionBy.getColumnDefinitions().stream()
                .map(c -> formatExpression(c.getColName())).collect(Collectors.joining(","));
            builder.append(columnList);
        }
        builder.append(")");
        if (oceanBaseKeyPartitionBy.getSubPartition() != null) {
            process(oceanBaseKeyPartitionBy.getSubPartition());
        }
        if (oceanBaseKeyPartitionBy.getPartitionCount() != null) {
            appendPartitionCount(oceanBaseKeyPartitionBy.getPartitionCount());
        }
        appendHashPartitionElements(oceanBaseKeyPartitionBy.getHashPartitionElements());
        return true;
    }

    @Override
    public Boolean visitListPartitionElement(ListPartitionElement listPartitionElement, Integer context) {
        // PARTITION relation_factor VALUES IN list_partition_expr (ID INTNUM)? partition_attributes_option? subpartition_list?
        builder.append("PARTITION ");
        QualifiedName qualifiedName = listPartitionElement.getQualifiedName();
        builder.append(formatName(qualifiedName));
        builder.append(" VALUES IN ");
        if (BooleanUtils.isTrue(listPartitionElement.getDefaultExpr())) {
            builder.append("(DEFAULT)");
        } else {
            List<BaseExpression> expressionList = listPartitionElement.getExpressionList();
            String listExpr = expressionList.stream().map(this::formatExpression).collect(Collectors.joining(","));
            builder.append("(");
            builder.append(listExpr);
            builder.append(")");
        }
        if (listPartitionElement.getNum() != null) {
            builder.append(" ID ");
            builder.append(listPartitionElement.getNum().getValue());
        }
        if (listPartitionElement.getProperty() != null) {
            Property property = listPartitionElement.getProperty();
            builder.append(property.getName() + "=" + property.getValue());
        }
        if (listPartitionElement.getSubPartitionList() != null) {
            process(listPartitionElement.getSubPartitionList());
        }
        return true;
    }

    @Override
    public Boolean visitHashPartitionElement(HashPartitionElement hashPartitionElement, Integer context) {
        //PARTITION relation_factor (ID INTNUM)? partition_attributes_option? subpartition_list?
        builder.append("PARTITION ");
        QualifiedName qualifiedName = hashPartitionElement.getQualifiedName();
        builder.append(formatName(qualifiedName));
        if (hashPartitionElement.getNum() != null) {
            builder.append(" ID ");
            builder.append(hashPartitionElement.getNum().getValue());
        }
        if (hashPartitionElement.getProperty() != null) {
            Property property = hashPartitionElement.getProperty();
            builder.append(property.getName() + "=" + property.getValue());
        }
        if (hashPartitionElement.getSubPartitionList() != null) {
            process(hashPartitionElement.getSubPartitionList(), 1);
        }
        return true;
    }

    @Override
    public Boolean visitSubPartitionList(SubPartitionList subPartitionList, Integer context) {
        builder.append("(\n");
        Iterator<SubPartitionElement> iterator = subPartitionList.getSubPartitionElementList().iterator();
        process(iterator.next(), context);
        while (iterator.hasNext()) {
            builder.append(",\n");
            process(iterator.next(), context);
        }
        builder.append("\n)");
        return true;
    }

    @Override
    public Boolean visitHashSubPartitionElement(SubHashPartitionElement hashSubPartitionElement, Integer context) {
        // SUBPARTITION relation_factor partition_attributes_option?
        String indent = indentString(context);
        builder.append(indent);
        builder.append("SUBPARTITION ");
        builder.append(formatName(hashSubPartitionElement.getQualifiedName()));
        if (hashSubPartitionElement.getProperty() != null) {
            Property property = hashSubPartitionElement.getProperty();
            builder.append(property.getName() + "=" + property.getValue());
        }
        return true;
    }

    @Override
    public Boolean visitListSubPartitionElement(SubListPartitionElement listSubPartitionElement, Integer context) {
        // SUBPARTITION relation_factor VALUES IN list_partition_expr partition_attributes_option?
        String indent = indentString(context);
        builder.append(indent);
        builder.append("SUBPARTITION ");
        builder.append(formatName(listSubPartitionElement.getQualifiedName()));
        builder.append(" VALUES IN ");
        if (listSubPartitionElement.getExpressionList() != null) {
            List<BaseExpression> expressionList = listSubPartitionElement.getExpressionList();
            String listExpr = expressionList.stream().map(this::formatExpression).collect(Collectors.joining(","));
            builder.append("(");
            builder.append(listExpr);
            builder.append(")");
        }
        if (listSubPartitionElement.getProperty() != null) {
            Property property = listSubPartitionElement.getProperty();
            builder.append(property.getName() + "=" + property.getValue());
        }
        return true;
    }

    @Override
    public Boolean visitRangeSubPartitionElement(SubRangePartitionElement rangeSubPartitionElement, Integer context) {
        //SUBPARTITION relation_factor VALUES LESS THAN range_partition_expr partition_attributes_option?
        String indent = indentString(context);
        builder.append(indent);
        builder.append("SUBPARTITION ");
        SingleRangePartition singleRangePartition = rangeSubPartitionElement.getSingleRangePartition();
        builder.append(" ").append(formatExpression(singleRangePartition.getName()));
        builder.append(" VALUES LESS THAN ");
        process(rangeSubPartitionElement.getSingleRangePartition().getPartitionKey());
        if (rangeSubPartitionElement.getSingleRangePartition().getPropertyList() != null) {
            List<Property> properties = singleRangePartition.getPropertyList();
            String attribute = properties.stream().map(p -> {
                return p.getName() + "=" + p.getValue();
            }).collect(Collectors.joining(","));
            builder.append(attribute);
        }
        return true;
    }

    @Override
    public Boolean visitSubRangeTemplatePartition(SubRangeTemplatePartition subRangeTemplatePartition, Integer context) {
        // SUBPARTITION BY RANGE LeftParen expr RightParen SUBPARTITION TEMPLATE opt_range_subpartition_list
        //    | SUBPARTITION BY RANGE COLUMNS LeftParen column_name_list RightParen SUBPARTITION TEMPLATE opt_range_subpartition_list
        builder.append("\nSUBPARTITION BY RANGE ");
        if (subRangeTemplatePartition.getColumnList() != null) {
            appendColumnList(subRangeTemplatePartition.getColumnList());
        } else {
            builder.append("(");
            String expr = formatExpression(subRangeTemplatePartition.getExpression());
            builder.append(expr);
            builder.append(")");
        }
        builder.append("\nSUBPARTITION TEMPLATE");
        if (subRangeTemplatePartition.getSubPartitionList() != null) {
            builder.append(" ");
            process(subRangeTemplatePartition.getSubPartitionList());
        }
        return true;
    }

    @Override
    public Boolean visitSubHashTemplatePartition(SubHashTemplatePartition subHashTemplatePartition, Integer context) {
        //SUBPARTITION BY HASH LeftParen expr RightParen SUBPARTITION TEMPLATE opt_hash_subpartition_list
        builder.append("\nSUBPARTITION BY HASH ");
        if (subHashTemplatePartition.getExpression() != null) {
            builder.append("(");
            String expr = formatExpression(subHashTemplatePartition.getExpression());
            builder.append(expr);
            builder.append(")");
        }
        builder.append("\nSUBPARTITION TEMPLATE");
        if (subHashTemplatePartition.getSubPartitionList() != null) {
            builder.append(" ");
            process(subHashTemplatePartition.getSubPartitionList());
        }
        return true;
    }

    @Override
    public Boolean visitSubListTemplatePartition(SubListTemplatePartition subListTemplatePartition, Integer context) {
        //SUBPARTITION BY BISON_LIST LeftParen expr RightParen SUBPARTITION TEMPLATE opt_list_subpartition_list
        //    | SUBPARTITION BY BISON_LIST COLUMNS LeftParen column_name_list RightParen SUBPARTITION TEMPLATE opt_list_subpartition_list
        builder.append("\nSUBPARTITION BY LIST ");
        if (subListTemplatePartition.getColumnList() != null) {
            appendColumnList(subListTemplatePartition.getColumnList());
        } else {
            appendExpression(subListTemplatePartition.getExpression());
        }
        builder.append("\nSUBPARTITION TEMPLATE");
        if (subListTemplatePartition.getSubPartitionList() != null) {
            builder.append(" ");
            process(subListTemplatePartition.getSubPartitionList());
        }
        return true;
    }

    private void appendExpression(BaseExpression expression) {
        builder.append("(");
        String expr = formatExpression(expression);
        builder.append(expr);
        builder.append(")");
    }

    @Override
    public Boolean visitSubKeyTemplatePartition(SubKeyTemplatePartition subKeyTemplatePartition, Integer context) {
        //SUBPARTITION BY KEY LeftParen column_name_list RightParen SUBPARTITION TEMPLATE opt_hash_subpartition_list
        builder.append("\nSUBPARTITION BY KEY ");
        appendColumnList(subKeyTemplatePartition.getColumnList());
        builder.append("\nSUBPARTITION TEMPLATE");
        if (subKeyTemplatePartition.getSubPartitionList() != null) {
            builder.append(" ");
            process(subKeyTemplatePartition.getSubPartitionList());
        }
        return true;
    }

    @Override
    public Boolean visitSubHashPartition(SubHashPartition subHashPartition, Integer context) {
        //SUBPARTITION BY HASH LeftParen expr RightParen (SUBPARTITIONS INTNUM)?
        builder.append("SUBPARTITION BY HASH ");
        appendExpression(subHashPartition.getExpression());
        if (subHashPartition.getSubpartitionCount() != null) {
            builder.append(" SUBPARTITIONS ");
            builder.append(subHashPartition.getSubpartitionCount().getValue());
        }
        return true;
    }

    @Override
    public Boolean visitSubRangePartition(SubRangePartition subRangePartition, Integer context) {
        //SUBPARTITION BY (RANGE|BISON_LIST) COLUMNS LeftParen column_name_list RightParen
        //SUBPARTITION BY (RANGE|BISON_LIST) LeftParen expr RightParen
        builder.append("SUBPARTITION BY RANGE ");
        if (subRangePartition.getColumnList() != null) {
            appendColumnList(subRangePartition.getColumnList());
        } else {
            appendExpression(subRangePartition.getExpression());
        }
        return true;
    }

    @Override
    public Boolean visitSubListPartition(SubListPartition subListPartition, Integer context) {
        //SUBPARTITION BY (RANGE|BISON_LIST) COLUMNS LeftParen column_name_list RightParen
        //SUBPARTITION BY (RANGE|BISON_LIST) LeftParen expr RightParen
        builder.append("SUBPARTITION BY LIST ");
        if (subListPartition.getColumnList() != null) {
            appendColumnList(subListPartition.getColumnList());
        } else {
            appendExpression(subListPartition.getExpression());
        }
        return true;
    }

    @Override
    public Boolean visitSubKeyPartition(SubKeyPartition subKeyPartition, Integer context) {
        //SUBPARTITION BY KEY LeftParen column_name_list RightParen (SUBPARTITIONS INTNUM)?
        builder.append("SUBPARTITION BY KEY (");
        List<Identifier> columnList = subKeyPartition.getColumnList();
        String e = columnList.stream().map(c -> c.getValue()).collect(Collectors.joining(","));
        builder.append(e);
        builder.append(")");
        if (subKeyPartition.getSubpartitionCount() != null) {
            builder.append(" SUBPARTITIONS ");
            builder.append(subKeyPartition.getSubpartitionCount().getValue());
        }
        return true;
    }

    @Override
    public Boolean visitCheckExpressionConstraint(CheckExpressionConstraint checkExpressionConstraint, Integer context) {
        //(CONSTRAINT opt_constraint_name)? CHECK LeftParen expr RightParen check_state?
        Identifier name = checkExpressionConstraint.getName();
        if (!IdentifierUtil.isSysIdentifier(name)) {
            builder.append(indentString(context)).append(CONSTRAINT).append(formatExpression(name));
            builder.append(" CHECK");
        } else {
            builder.append(indentString(context)).append("CHECK");
        }
        appendExpression(checkExpressionConstraint.getExpression());
        if (checkExpressionConstraint.getEnforced() != null) {
            if (checkExpressionConstraint.getEnforced()) {
                builder.append(" ENFORCED");
            } else {
                builder.append(" NOT ENFORCED");
            }
        }
        return true;
    }

    @Override
    public Boolean visitForeignKeyConstraint(ForeignKeyConstraint foreignKeyConstraint, Integer context) {
        // (CONSTRAINT opt_constraint_name)? FOREIGN KEY index_name? LeftParen column_name_list RightParen references_clause
        // REFERENCES relation_factor LeftParen column_name_list RightParen (MATCH match_action)? (opt_reference_option_list reference_option)?
        Identifier name = foreignKeyConstraint.getName();
        if (!IdentifierUtil.isSysIdentifier(name)) {
            builder.append(indentString(context)).append(CONSTRAINT).append(formatExpression(name));
            builder.append(" FOREIGN KEY");
        } else {
            builder.append(indentString(context)).append("FOREIGN KEY");
        }
        if (foreignKeyConstraint.getIndexName() != null) {
            builder.append(" ").append(formatExpression(foreignKeyConstraint.getIndexName()));
        }
        builder.append("(");
        List<Identifier> colNames = foreignKeyConstraint.getColNames();
        String collect = colNames.stream().map(c -> formatExpression(c)).collect(joining(","));
        builder.append(collect).append(")");
        //reference
        builder.append(" REFERENCES ");
        builder.append(formatName(foreignKeyConstraint.getReferenceTable()));

        String referenceColumns = foreignKeyConstraint.getReferenceColNames().stream().map(c -> formatExpression(c)).collect(joining(","));
        builder.append("(").append(referenceColumns).append(")");

        if (foreignKeyConstraint.getMatchAction() != null) {
            builder.append(" MATCH ").append(foreignKeyConstraint.getMatchAction().name());
        }

        if (foreignKeyConstraint.getReferenceAction() != null) {
            //ON (DELETE|UPDATE) reference_action
            builder.append(" ON ");
            builder.append(foreignKeyConstraint.getReferenceOperator().name());
            builder.append(" ").append(foreignKeyConstraint.getReferenceAction().getValue());
        }
        return true;
    }

    @Override
    public Boolean visitPartitionedBy(PartitionedBy partitionedBy, Integer context) {
        if (partitionedBy instanceof OceanBaseHashPartitionBy) {
            OceanBaseHashPartitionBy oceanBaseHashPartitionBy = (OceanBaseHashPartitionBy)partitionedBy;
            return visitOceanBaseHashPartitionBy(oceanBaseHashPartitionBy, context);
        }
        if (partitionedBy instanceof OceanBaseListPartitionBy) {
            OceanBaseListPartitionBy oceanBaseListPartitionBy = (OceanBaseListPartitionBy)partitionedBy;
            return visitOceanBaseListPartitionBy(oceanBaseListPartitionBy, context);
        }
        if (partitionedBy instanceof OceanBaseKeyPartitionBy) {
            OceanBaseKeyPartitionBy oceanBaseKeyPartitionBy = (OceanBaseKeyPartitionBy)partitionedBy;
            return visitOceanBaseKeyPartitionBy(oceanBaseKeyPartitionBy, context);
        }
        if (partitionedBy instanceof OceanBaseRangePartitionBy) {
            OceanBaseRangePartitionBy oceanBaseRangePartitionBy = (OceanBaseRangePartitionBy)partitionedBy;
            return visitOceanBaseRangePartitionBy(oceanBaseRangePartitionBy, context);
        }
        return super.visitPartitionedBy(partitionedBy, context);

    }

    @Override
    public Boolean visitUniqueKeyExprConstraint(UniqueKeyExprConstraint uniqueKeyExprConstraint, Integer context) {
        // UNIQUE key_or_index? index_name? index_using_algorithm? LeftParen sort_column_list RightParen opt_index_options? (partition_option |
        // auto_partition_option)?
        Identifier name = uniqueKeyExprConstraint.getName();
        if (!IdentifierUtil.isSysIdentifier(name)) {
            builder.append(indentString(context)).append(CONSTRAINT).append(formatExpression(name));
            builder.append(" UNIQUE KEY(");
        } else {
            builder.append(indentString(context)).append("UNIQUE KEY(");
        }
        Iterator<IndexSortKey> iterator = uniqueKeyExprConstraint.getIndexSortKeys().iterator();
        process(iterator.next(), context);
        while (iterator.hasNext()) {
            builder.append(",");
            builder.append(iterator.next());
        }
        builder.append(")");
        return true;
    }

    @Override
    public Boolean visitIndexColumnName(IndexColumnName indexColumnName, Integer context) {
        //column_name (LeftParen INTNUM RightParen)? (ASC | DESC)? (ID INTNUM)?
        builder.append(formatExpression(indexColumnName.getColumnName()));
        if (indexColumnName.getColumnLength() != null) {
            builder.append("(");
            builder.append(indexColumnName.getColumnLength().getValue());
            builder.append(")");
        }
        if (indexColumnName.getSortType() != null) {
            builder.append(" ");
            builder.append(indexColumnName.getSortType().name());
        }
        return true;
    }

    @Override
    public Boolean visitIndexExpr(IndexExpr indexExpr, Integer context) {
        // LeftParen expr RightParen (ASC | DESC)? (ID INTNUM)?
        builder.append("(");
        String expr = formatExpression(indexExpr.getExpression());
        builder.append(expr);
        builder.append(")");
        if (indexExpr.getSortType() != null) {
            builder.append(" ").append(indexExpr.getSortType().name());
        }
        return true;
    }

    @Override
    public Boolean visitAddCols(AddCols addCols, Integer context) {
        builder.append("ALTER TABLE ").append(getCode(addCols.getQualifiedName()));
        List<ColumnDefinition> columnDefineList = addCols.getColumnDefineList();
        String collect = columnDefineList.stream().map(
            c -> "\nADD COLUMN " + formatColumnDefinition(c, context + 1)
        ).collect(joining(","));
        builder.append(collect);
        return true;
    }

    @Override
    public Boolean visitSetTableProperties(SetTableProperties setTableProperties, Integer context) {
        builder.append("ALTER TABLE ").append(getCode(setTableProperties.getQualifiedName()));
        builder.append(" ");
        builder.append(formatProperty(setTableProperties.getProperties()));
        return true;
    }

    @Override
    public Boolean visitUnSetTableProperties(UnSetTableProperties unSetTableProperties, Integer context) {
        builder.append("ALTER TABLE ").append(getCode(unSetTableProperties.getQualifiedName()));
        builder.append(" ");
        String collect = unSetTableProperties.getPropertyKeys().stream().map(x -> x + "=''").collect(joining(","));
        builder.append(collect);
        return true;
    }

    @Override
    protected String formatExpression(BaseExpression baseExpression) {
        return new OceanBaseMysqlExpressionVisitor(context).process(baseExpression);
    }
}
