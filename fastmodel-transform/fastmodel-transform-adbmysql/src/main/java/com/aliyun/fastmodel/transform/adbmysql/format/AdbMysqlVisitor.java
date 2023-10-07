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
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName.Dimension;
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
import com.aliyun.fastmodel.core.tree.statement.table.SetTableProperties;
import com.aliyun.fastmodel.core.tree.statement.table.UnSetTableProperties;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.PrimaryConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.UniqueConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.index.TableIndex;
import com.aliyun.fastmodel.transform.adbmysql.context.AdbMysqlTransformContext;
import com.aliyun.fastmodel.transform.api.format.DefaultExpressionVisitor;
import com.aliyun.fastmodel.transform.api.util.StringJoinUtil;
import com.google.common.collect.ImmutableList;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

import static java.util.stream.Collectors.joining;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2023/2/11
 */
public class AdbMysqlVisitor extends FastModelVisitor {

    private final AdbMysqlTransformContext mysqlTransformContext;

    public AdbMysqlVisitor(AdbMysqlTransformContext context) {
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
        String tableName = node.getIdentifier();
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
        //distribute by
        appendDistributeBy(node);
        //分区信息内容
        if (!node.isPartitionEmpty()) {
            String list = formatPartitions(node, node.getPartitionedBy().getColumnDefinitions());
            if (!isEndNewLine(builder.toString())) {
                builder.append(StringUtils.LF);
            }
            builder.append(list);
        }
        appendLifecycle(node);
        //storage policy
        appendStoragePolicy(node);
        //block size
        appendBlockSize(node);

        //append comment
        builder.append(formatComment(node.getComment()));
        return executable;
    }

    private void appendBlockSize(CreateTable node) {
        String value = getPropertyValue(node, AdbMysqlPropertyKey.BLOCK_SIZE);
        if (value == null) {
            return;
        }
        if (!isEndNewLine(builder.toString())) {
            builder.append(StringUtils.LF);
        }
        builder.append("BLOCK_SIZE=").append(value);
    }

    private void appendStoragePolicy(CreateTable node) {
        String value = getPropertyValue(node, AdbMysqlPropertyKey.STORAGE_POLICY);
        if (value == null) {return;}
        builder.append(StringUtils.LF).append("STORAGE_POLICY=").append(formatStringLiteral(value));
        if (StringUtils.equalsIgnoreCase(value, "MIXED")) {
            String mix = getPropertyValue(node, AdbMysqlPropertyKey.HOT_PARTITION_COUNT);
            if (mix == null) {
                return;
            }
            builder.append(" ").append(mix);
        }
    }

    private String getPropertyValue(CreateTable node, AdbMysqlPropertyKey adbMysqlPropertyKey) {
        if (node.isPropertyEmpty()) {
            return null;
        }
        List<Property> properties = node.getProperties();
        Optional<Property> first = properties.stream().filter(p -> StringUtils.equalsIgnoreCase(p.getName(), adbMysqlPropertyKey.getValue()))
            .findFirst();
        if (!first.isPresent()) {
            return null;
        }

        return first.get().getValue();
    }

    private void appendDistributeBy(CreateTable node) {
        String value = getPropertyValue(node, AdbMysqlPropertyKey.DISTRIBUTED_BY);
        if (value == null) {
            return;
        }
        if (!isEndNewLine(builder.toString())) {
            builder.append(StringUtils.LF);
        }
        builder.append("DISTRIBUTED BY HASH(").append(value).append(")");
    }

    private void appendLifecycle(CreateTable node) {
        String propertyValue = getPropertyValue(node, AdbMysqlPropertyKey.LIFE_CYCLE);
        if (propertyValue == null) {
            return;
        }
        builder.append(" LIFECYCLE " + propertyValue);
    }

    protected String formatPartitions(CreateTable node, List<ColumnDefinition> partitionCol) {
        StringBuilder stringBuilder = new StringBuilder("PARTITION BY VALUE(");
        String propertyValue = getPropertyValue(node, AdbMysqlPropertyKey.PARTITION_DATE_FORMAT);
        if (propertyValue == null) {
            String collect = partitionCol.stream().map(c -> formatExpression(c.getColName())).collect(joining(","));
            stringBuilder.append(collect);
        } else {
            ColumnDefinition columnDefinition = partitionCol.get(0);
            stringBuilder.append("DATE_FORMAT(").append(formatExpression(columnDefinition.getColName())).append(",").append(formatStringLiteral(
                propertyValue
            )).append(")");
        }
        stringBuilder.append(")");
        return stringBuilder.toString();
    }

    @Override
    protected String formatCommentElement(List<MultiComment> commentElements, String elementIndent) {
        return commentElements.stream().map(
            element -> {
                AdbMysqlVisitor visitor = new AdbMysqlVisitor(this.mysqlTransformContext);
                visitor.process(element.getNode(), 0);
                String result = visitor.getBuilder().toString();
                return elementIndent + result;
            }).collect(Collectors.joining(",\n"));
    }

    private void appendConstraint(CreateTable node, Integer indent) {
        Iterator<BaseConstraint> iterator = node.getConstraintStatements().iterator();
        while (iterator.hasNext()) {
            BaseConstraint next = iterator.next();
            if (next instanceof PrimaryConstraint || next instanceof UniqueConstraint) {
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
        } else if (typeName.getDimension() == Dimension.MULTIPLE) {
            return new GenericDataType(new Identifier(DataTypeEnums.JSON.name()));
        } else if (StringUtils.equalsIgnoreCase(typeName.getValue(), DataTypeEnums.BOOLEAN.getValue())) {
            return new GenericDataType(new Identifier(DataTypeEnums.CHAR.name()),
                ImmutableList.of(new NumericParameter("1")));
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
        String collect = columnList.stream().map(identifier -> formatExpression(identifier)).collect(joining(","));
        builder.append(" FOREIGN KEY (").append(collect).append(")");
        builder.append(" REFERENCES ").append(formatName(right.getMainName()));
        String rightReference = rightColumnList.stream().map(identifier -> formatExpression(identifier)).collect(joining(","));
        builder.append(" (").append(rightReference).append(")");
        return true;
    }

    @Override
    protected String formatExpression(BaseExpression baseExpression) {
        return new DefaultExpressionVisitor().process(baseExpression);
    }
}
