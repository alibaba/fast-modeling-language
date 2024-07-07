package com.aliyun.fastmodel.transform.adbmysql.format;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.core.formatter.ExpressionFormatter;
import com.aliyun.fastmodel.core.formatter.FastModelVisitor;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.datatype.GenericDataType;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import com.aliyun.fastmodel.core.tree.datatype.NumericParameter;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.constants.ConstraintType;
import com.aliyun.fastmodel.core.tree.statement.element.MultiComment;
import com.aliyun.fastmodel.core.tree.statement.script.RefObject;
import com.aliyun.fastmodel.core.tree.statement.script.RefRelation;
import com.aliyun.fastmodel.core.tree.statement.table.AddCols;
import com.aliyun.fastmodel.core.tree.statement.table.AddConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.AddPartitionCol;
import com.aliyun.fastmodel.core.tree.statement.table.ChangeCol;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.DropConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.DropPartitionCol;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableProperties;
import com.aliyun.fastmodel.core.tree.statement.table.UnSetTableProperties;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.PrimaryConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.index.TableIndex;
import com.aliyun.fastmodel.core.tree.util.PropertyUtil;
import com.aliyun.fastmodel.transform.adbmysql.context.AdbMysqlTransformContext;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.desc.DistributeConstraint;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.desc.NonKeyConstraint;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.ExpressionPartitionBy;
import com.aliyun.fastmodel.transform.api.extension.visitor.ExtensionAstVisitor;
import com.aliyun.fastmodel.transform.api.format.DefaultExpressionVisitor;
import com.aliyun.fastmodel.transform.api.format.PropertyValueType;
import com.aliyun.fastmodel.transform.api.util.StringJoinUtil;
import com.google.common.collect.ImmutableList;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

import static com.aliyun.fastmodel.transform.api.extension.client.property.ExtensionPropertyKey.TABLE_PARTITION_RAW;
import static java.util.stream.Collectors.joining;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2023/2/11
 */
public class AdbMysqlOutVisitor extends FastModelVisitor implements ExtensionAstVisitor<Boolean, Integer> {

    private final AdbMysqlTransformContext mysqlTransformContext;

    public AdbMysqlOutVisitor(AdbMysqlTransformContext context) {
        this.mysqlTransformContext = context;
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
        String tableName = getCode(node.getQualifiedName());
        builder.append(tableName);
        if (!columnEmpty) {
            builder.append("\n(\n");
            String elementIndent = indentString(indent + 1);
            String columnList = formatColumnList(node.getColumnDefines(), elementIndent);
            builder.append(columnList);
            if (!node.isConstraintEmpty()) {
                appendConstraint(node, indent);
            }
            if (!node.isIndexEmpty()) {
                Iterator<TableIndex> iterator = node.getTableIndexList().iterator();
                while (iterator.hasNext()) {
                    builder.append(",\n");
                    process(iterator.next(), indent + 1);
                }
            }
            builder.append("\n").append(")");
        } else {
            if (!node.isCommentElementEmpty()) {
                builder.append(newLine("/*("));
                String elementIndent = indentString(indent + 1);
                builder.append(formatCommentElement(node.getColumnCommentElements(), elementIndent));
                builder.append(newLine(")*/"));
            }
        }
        if (!node.isConstraintEmpty()) {
            List<BaseConstraint> nonKeyConstraint = node.getConstraintStatements().stream().filter(
                c -> c instanceof NonKeyConstraint).collect(Collectors.toList());
            appendConstraint(nonKeyConstraint, indent);
        }
        //分区信息内容
        if (!node.isPartitionEmpty()) {
            if (node.getPartitionedBy() instanceof ExpressionPartitionBy) {
                ExpressionPartitionBy expressionPartitionBy = (ExpressionPartitionBy)node.getPartitionedBy();
                process(expressionPartitionBy);
            } else {
                String list = formatPartitions(node.getPartitionedBy().getColumnDefinitions());
                if (!isEndNewLine(builder.toString())) {
                    builder.append(StringUtils.LF);
                }
                builder.append(list);
            }
        } else {
            String propertyValue = PropertyUtil.getPropertyValue(node.getProperties(), TABLE_PARTITION_RAW.getValue());
            if (StringUtils.isNotBlank(propertyValue)) {
                if (!isEndNewLine(builder.toString())) {
                    builder.append("\n");
                }
                builder.append(propertyValue);
            }
        }

        appendLifecycle(node);
        //storage policy
        appendProperty(node);
        //append comment
        if (node.getComment() != null) {
            if (!isEndNewLine(builder.toString())) {
                builder.append(StringUtils.LF);
            }
            builder.append(formatComment(node.getComment(), true));
        }
        return executable;
    }

    private void appendProperty(CreateTable node) {
        if (node.isPropertyEmpty()) {
            return;
        }
        List<Property> properties = node.getProperties();
        List<Property> collect = properties.stream().filter(
            p -> {
                AdbMysqlPropertyKey propertyKey = AdbMysqlPropertyKey.getByValue(p.getName());
                return propertyKey != null && propertyKey.isSupportPrint();
            }
        ).collect(Collectors.toList());
        if (collect.isEmpty()) {
            return;
        }
        builder.append(StringUtils.LF);
        String val = collect.stream().map(p -> {
            AdbMysqlPropertyKey propertyKey = AdbMysqlPropertyKey.getByValue(p.getName());
            if (propertyKey.getValueType() == PropertyValueType.NUMBER_LITERAL) {
                return p.getName() + "=" + p.getValue();
            } else {
                return p.getName() + "=" + formatStringLiteral(p.getValue());
            }
        }).collect(joining(StringUtils.LF));
        builder.append(val);
    }

    @Override
    public Boolean visitExpressionPartitionedBy(ExpressionPartitionBy expressionPartitionedBy, Integer context) {
        if (!isEndNewLine(builder.toString())) {
            builder.append(StringUtils.LF);
        }
        builder.append("PARTITION BY VALUE(");
        String expression = formatExpression(expressionPartitionedBy.getFunctionCall());
        builder.append(expression);
        builder.append(")");
        return true;
    }

    @Override
    public Boolean visitPartitionedBy(PartitionedBy partitionedBy, Integer context) {
        if (!isEndNewLine(builder.toString())) {
            builder.append(StringUtils.LF);
        }
        builder.append("PARTITION BY HASH(");
        String columnList = partitionedBy.getColumnDefinitions().stream().map(c -> c.getColName().getValue()).collect(joining(","));
        builder.append(columnList);
        builder.append(")");
        return true;
    }

    private void appendConstraint(List<BaseConstraint> constraints, Integer indent) {
        for (BaseConstraint baseConstraint : constraints) {
            builder.append("\n");
            process(baseConstraint, indent);
        }
    }

    private String getPropertyValue(CreateTable node, AdbMysqlPropertyKey adbMysqlPropertyKey) {
        if (node.isPropertyEmpty()) {
            return null;
        }
        List<Property> properties = node.getProperties();
        Optional<Property> first = properties.stream().filter(p -> StringUtils.equalsIgnoreCase(p.getName(), adbMysqlPropertyKey.getValue()))
            .findFirst();
        return first.map(Property::getValue).orElse(null);

    }

    private void appendLifecycle(CreateTable node) {
        String propertyValue = getPropertyValue(node, AdbMysqlPropertyKey.LIFE_CYCLE);
        if (propertyValue == null) {
            return;
        }
        builder.append(" LIFECYCLE ").append(propertyValue);
    }

    protected String formatPartitions(List<ColumnDefinition> partitionCol) {
        StringBuilder stringBuilder = new StringBuilder("PARTITION BY VALUE(");
        String collect = partitionCol.stream().map(c -> formatExpression(c.getColName())).collect(joining(","));
        stringBuilder.append(collect);
        stringBuilder.append(")");
        return stringBuilder.toString();
    }

    @Override
    protected String formatCommentElement(List<MultiComment> commentElements, String elementIndent) {
        return commentElements.stream().map(
            element -> {
                AdbMysqlOutVisitor visitor = new AdbMysqlOutVisitor(this.mysqlTransformContext);
                visitor.process(element.getNode(), 0);
                String result = visitor.getBuilder().toString();
                return elementIndent + result;
            }).collect(Collectors.joining(",\n"));
    }

    private void appendConstraint(CreateTable node, Integer indent) {
        for (BaseConstraint next : node.getConstraintStatements()) {
            if (!(next instanceof NonKeyConstraint)) {
                builder.append(",\n");
                process(next, indent + 1);
            }
        }
    }

    @Override
    public Boolean visitPrimaryConstraint(PrimaryConstraint primaryConstraint, Integer indent) {
        builder.append(indentString(indent)).append("PRIMARY KEY(");
        builder.append(
            primaryConstraint.getColNames().stream().map(ExpressionFormatter::formatExpression).collect(joining(",")));
        builder.append(")");
        return true;
    }

    @Override
    public Boolean visitAddCols(AddCols addCols, Integer context) {
        builder.append("ALTER TABLE ").append(getCode(addCols.getQualifiedName()));
        builder.append(" ADD\n").append('(').append("\n");
        String elementIndent = indentString(context + 1);
        String columnList = formatColumnList(addCols.getColumnDefineList(), elementIndent);
        builder.append(columnList);
        builder.append("\n").append(')');
        return true;
    }

    @Override
    public Boolean visitDropConstraint(DropConstraint dropConstraint, Integer context) {
        if (dropConstraint.getConstraintType() == null) {
            super.visitDropConstraint(dropConstraint, context);
            return false;
        }
        ConstraintType constraintType = dropConstraint.getConstraintType();
        if (constraintType == ConstraintType.PRIMARY_KEY) {
            builder.append("ALTER TABLE ").append(getCode(dropConstraint.getQualifiedName()));
            builder.append(" DROP PRIMARY KEY");
        } else {
            super.visitDropConstraint(dropConstraint, context);
            return false;
        }
        return true;
    }

    @Override
    public Boolean visitChangeCol(ChangeCol renameCol, Integer context) {
        builder.append("ALTER TABLE ").append(getCode(renameCol.getQualifiedName()));
        builder.append(" CHANGE COLUMN ").append(ExpressionFormatter.formatExpression(renameCol.getOldColName()));
        builder.append(" ").append(formatColumnDefinition(renameCol.getColumnDefinition(), 0));
        return true;
    }

    @Override
    public Boolean visitAddConstraint(AddConstraint addConstraint, Integer context) {
        BaseConstraint constraintStatement = addConstraint.getConstraintStatement();
        if (constraintStatement instanceof PrimaryConstraint) {
            PrimaryConstraint constraint = (PrimaryConstraint)constraintStatement;
            List<Identifier> colNames = constraint.getColNames();
            builder.append("ALTER TABLE ").append(getCode(addConstraint.getQualifiedName()));
            builder.append(" ADD CONSTRAINT ").append(formatExpression(constraint.getName()));
            builder.append(" PRIMARY KEY ");
            builder.append("(");
            builder.append(colNames.stream().map(Identifier::getValue).collect(joining(",")));
            builder.append(")");
        } else {
            super.visitAddConstraint(addConstraint, context);
            return false;
        }
        return true;
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
    public Boolean visitSetTableProperties(SetTableProperties setTableProperties, Integer context) {
        super.visitSetTableProperties(setTableProperties, context);
        return false;
    }

    @Override
    protected String getCode(QualifiedName qualifiedName) {
        QualifiedName tableName = StringJoinUtil.join(this.mysqlTransformContext.getDatabase(),
            this.mysqlTransformContext.getSchema(), qualifiedName.getSuffix());
        return formatName(tableName);
    }

    @Override
    protected String formatColumnDefinition(ColumnDefinition column, Integer max) {
        BaseDataType dataType = column.getDataType();
        StringBuilder sb = new StringBuilder();
        sb.append(formatColName(column.getColName(), max));
        sb.append(" ").append(formatExpression(convert(dataType)));
        Boolean notNull = column.getNotNull();
        if (BooleanUtils.isTrue(notNull)) {
            sb.append(" NOT NULL");
        } else if (BooleanUtils.isFalse(notNull)) {
            sb.append(" NULL");
        }
        if (column.getDefaultValue() != null) {
            sb.append(" DEFAULT ").append(formatExpression(column.getDefaultValue()));
        }
        boolean isPrimary = column.getPrimary() != null && column.getPrimary();
        if (isPrimary) {
            sb.append(" PRIMARY KEY");
        }
        sb.append(formatComment(column.getComment()));
        return sb.toString();
    }

    @Override
    protected BaseDataType convert(BaseDataType dataType) {
        if (mysqlTransformContext.getDataTypeTransformer() != null) {
            return mysqlTransformContext.getDataTypeTransformer().convert(dataType);
        }
        IDataTypeName typeName = dataType.getTypeName();
        if (StringUtils.equalsIgnoreCase(typeName.getValue(), DataTypeEnums.STRING.getValue())) {
            return new GenericDataType(new Identifier(DataTypeEnums.VARCHAR.name()),
                ImmutableList.of(new NumericParameter("128")));
        }
        return dataType;
    }

    @Override
    public Boolean visitRefEntityStatement(RefRelation refEntityStatement, Integer context) {
        //转为dim constraint
        RefObject left = refEntityStatement.getLeft();
        RefObject right = refEntityStatement.getRight();
        List<Identifier> columnList = left.getAttrNameList();
        List<Identifier> rightColumnList = right.getAttrNameList();
        if (CollectionUtils.isEmpty(columnList) || CollectionUtils.isEmpty(rightColumnList)) {
            return false;
        }
        //ALTER TABLE `a` ADD CONSTRAINT `name` FOREIGN KEY (`a`) REFERENCES `b` (`a`);
        builder.append("ALTER TABLE ").append(getCode(left.getMainName()));
        builder.append(" ADD CONSTRAINT ").append(formatName(refEntityStatement.getQualifiedName()));
        String collect = columnList.stream().map(this::formatExpression).collect(joining(","));
        builder.append(" FOREIGN KEY (").append(collect).append(")");
        builder.append(" REFERENCES ").append(formatName(right.getMainName()));
        String rightReference = rightColumnList.stream().map(this::formatExpression).collect(joining(","));
        builder.append(" (").append(rightReference).append(")");
        return true;
    }

    @Override
    public Boolean visitDistributeKeyConstraint(DistributeConstraint distributeKeyConstraint, Integer context) {
        if (!isEndNewLine(builder.toString())) {
            builder.append(StringUtils.LF);
        }
        if (BooleanUtils.isTrue(distributeKeyConstraint.getRandom())) {
            builder.append("DISTRIBUTE BY BROADCAST");
        } else {
            String value = distributeKeyConstraint.getColumns().stream()
                .map(this::formatExpression)
                .collect(joining(","));
            builder.append("DISTRIBUTE BY HASH(").append(value).append(")");
        }
        return true;
    }

    @Override
    protected String formatExpression(BaseExpression baseExpression) {
        return new DefaultExpressionVisitor().process(baseExpression);
    }
}
