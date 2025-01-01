package com.aliyun.fastmodel.transform.flink.parser.visitor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.common.utils.StripUtils;
import com.aliyun.fastmodel.core.exception.ParseException;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.NodeLocation;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeParameter;
import com.aliyun.fastmodel.core.tree.datatype.Field;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import com.aliyun.fastmodel.core.tree.datatype.NumericParameter;
import com.aliyun.fastmodel.core.tree.datatype.TypeParameter;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.literal.BooleanLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.PrimaryConstraint;
import com.aliyun.fastmodel.core.tree.util.IdentifierUtil;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.WaterMarkConstraint;
import com.aliyun.fastmodel.transform.flink.format.FlinkColumnPropertyKey;
import com.aliyun.fastmodel.transform.flink.parser.tree.datatype.FlinkDataTypeName;
import com.aliyun.fastmodel.transform.flink.parser.tree.datatype.FlinkGenericDataType;
import com.aliyun.fastmodel.transform.flink.parser.tree.datatype.FlinkRawDataType;
import com.aliyun.fastmodel.transform.flink.parser.tree.datatype.FlinkRowDataType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.calcite.sql.SqlAlienSystemTypeNameSpec;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlTypeNameSpec;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlTableColumn.SqlComputedColumn;
import org.apache.flink.sql.parser.ddl.SqlTableColumn.SqlMetadataColumn;
import org.apache.flink.sql.parser.ddl.SqlTableColumn.SqlRegularColumn;
import org.apache.flink.sql.parser.ddl.SqlTableOption;
import org.apache.flink.sql.parser.ddl.SqlWatermark;
import org.apache.flink.sql.parser.ddl.constraint.SqlTableConstraint;
import org.apache.flink.sql.parser.type.ExtendedSqlCollectionTypeNameSpec;
import org.apache.flink.sql.parser.type.ExtendedSqlRowTypeNameSpec;
import org.apache.flink.sql.parser.type.SqlMapTypeNameSpec;
import org.apache.flink.sql.parser.type.SqlRawTypeNameSpec;

/**
 * @author 子梁
 * @date 2024/5/23
 */
public class FlinkParserAstBuilder extends SqlBasicVisitor<Node> {

    public static final String STRING_TYPE = "STRING";
    public static final String STRING_TYPE_ALIAS = "VARCHAR";

    private ReverseContext flinkTransformContext;

    public FlinkParserAstBuilder(ReverseContext context) {
        this.flinkTransformContext = context;
    }

    @Override
    public Node visit(SqlCall call) {
        if (!(call instanceof SqlCreateTable)) {
            throw new ParseException("Not supported yet!");
        }

        SqlCreateTable sqlCreateTable = (SqlCreateTable) call;

        QualifiedName tableName = visitTableName(sqlCreateTable.getTableName());

        List<ColumnDefinition> columns = visitColumns(sqlCreateTable.getColumnList());

        PartitionedBy partition = visitPartitionBy(sqlCreateTable.getPartitionKeyList());

        List<BaseConstraint> constraints = visitConstraints(sqlCreateTable.getTableConstraints());

        List<Property> properties = visitProperties(sqlCreateTable.getPropertyList());

        Comment comment = null;
        if (sqlCreateTable.getComment().isPresent()) {
            comment = visitComment(sqlCreateTable.getComment().get());
        }

        WaterMarkConstraint waterMarkConstraint = null;
        if (sqlCreateTable.getWatermark().isPresent()) {
            waterMarkConstraint = visitWaterMark(sqlCreateTable.getWatermark().get());
            constraints.add(waterMarkConstraint);
        }

        return CreateTable.builder()
            .ifNotExist(sqlCreateTable.isIfNotExists())
            .tableName(tableName)
            .columns(columns)
            .partition(partition)
            .constraints(constraints)
            .properties(properties)
            .comment(comment)
            .build();
    }

    private QualifiedName visitTableName(SqlIdentifier tableName) {
        List<Identifier> identifiers = tableName.names.stream().map(Identifier::new).collect(Collectors.toList());
        return QualifiedName.of(identifiers);
    }

    private List<ColumnDefinition> visitColumns(SqlNodeList columnList) {
        if (CollectionUtils.isEmpty(columnList.getList())) {
            return Collections.emptyList();
        }
        return columnList.stream().map(column -> {
            if (column instanceof SqlRegularColumn) {
                return visitRegularColumn((SqlRegularColumn) column);
            } else if (column instanceof SqlMetadataColumn) {
                return visitMetadataColumn((SqlMetadataColumn) column);
            } else if (column instanceof SqlComputedColumn) {
                return visitComputedColumn((SqlComputedColumn) column);
            } else {
                throw new ParseException("Invalid column!");
            }
        }).collect(Collectors.toList());
    }

    private ColumnDefinition visitRegularColumn(SqlRegularColumn column) {
        Identifier columnName = new Identifier(column.getName().names.get(0));
        BaseDataType dataType = visitColumnType(column.getType().getTypeNameSpec());
        Comment comment = null;
        if (column.getComment().isPresent()) {
            comment = new Comment(StripUtils.strip(column.getComment().get().toString()));
        }

        return ColumnDefinition.builder()
            .colName(columnName)
            .dataType(dataType)
            .notNull(BooleanUtils.isFalse(column.getType().getNullable()))
            .comment(comment)
            .build();
    }

    private ColumnDefinition visitMetadataColumn(SqlMetadataColumn column) {
        Identifier columnName = new Identifier(column.getName().getSimple());
        BaseDataType dataType = visitColumnType(column.getType().getTypeNameSpec());

        List<Property> columnProperties = new ArrayList<>();
        columnProperties.add(new Property(FlinkColumnPropertyKey.METADATA.getValue(), new BooleanLiteral("true")));
        if (column.getMetadataAlias().isPresent()) {
            columnProperties.add(new Property(FlinkColumnPropertyKey.METADATA_KEY.getValue(), StripUtils.strip(column.getMetadataAlias().get())));
        }
        if (column.isVirtual()) {
            columnProperties.add(new Property(FlinkColumnPropertyKey.VIRTUAL.getValue(), new BooleanLiteral("true")));
        }

        return ColumnDefinition.builder()
            .colName(columnName)
            .dataType(dataType)
            .properties(columnProperties)
            .build();
    }

    private ColumnDefinition visitComputedColumn(SqlComputedColumn column) {
        Identifier columnName = new Identifier(column.getName().getSimple());

        Comment comment = null;
        if (column.getComment().isPresent()) {
            comment = new Comment(column.getComment().get().toString());
        }

        List<Property> columnProperties = new ArrayList<>();
        columnProperties.add(new Property(FlinkColumnPropertyKey.COMPUTED.getValue(), new BooleanLiteral("true")));
        if (column.getExpr() != null) {
            SqlBasicCall expr = (SqlBasicCall)column.getExpr();
            String expression = expr.getOperandList().stream()
                .map(SqlNode::toString)
                .collect(Collectors.joining(expr.getOperator().getName()));
            columnProperties.add(new Property(FlinkColumnPropertyKey.COMPUTED_COLUMN_EXPRESSION.getValue(),
                new StringLiteral(new NodeLocation(expr.getParserPosition().getLineNum(), expr.getParserPosition().getColumnNum()),
                    expression, expression)));
        }

        return ColumnDefinition.builder()
            .colName(columnName)
            .comment(comment)
            .properties(columnProperties)
            .build();
    }

    private BaseDataType visitColumnType(SqlTypeNameSpec typeNameSpec) {
        if (typeNameSpec instanceof ExtendedSqlRowTypeNameSpec) {
            ExtendedSqlRowTypeNameSpec extendedSqlRowTypeNameSpec = (ExtendedSqlRowTypeNameSpec) typeNameSpec;
            List<SqlIdentifier> columnNames = extendedSqlRowTypeNameSpec.getFieldNames();
            List<SqlDataTypeSpec> dataTypes = extendedSqlRowTypeNameSpec.getFieldTypes();
            List<Field> fields = new ArrayList<>();
            for (int i = 0; i < columnNames.size(); i++) {
                SqlIdentifier columnName = columnNames.get(i);
                SqlDataTypeSpec dataType = dataTypes.get(i);
                fields.add(new Field(new Identifier(columnName.getSimple()),
                    visitColumnType(dataType.getTypeNameSpec()), null));
            }
            return new FlinkRowDataType(fields);
        } else if (typeNameSpec instanceof ExtendedSqlCollectionTypeNameSpec) {
            ExtendedSqlCollectionTypeNameSpec extendedSqlCollectionTypeNameSpec = (ExtendedSqlCollectionTypeNameSpec) typeNameSpec;

            return new FlinkGenericDataType(
                new NodeLocation(extendedSqlCollectionTypeNameSpec.getParserPos().getLineNum(),
                    extendedSqlCollectionTypeNameSpec.getParserPos().getColumnNum()),
                extendedSqlCollectionTypeNameSpec.toString(),
                extendedSqlCollectionTypeNameSpec.getTypeName().getSimple(),
                ImmutableList.of(new TypeParameter(visitColumnType(extendedSqlCollectionTypeNameSpec.getElementTypeName()))));
        } else if (typeNameSpec instanceof SqlMapTypeNameSpec) {
            SqlMapTypeNameSpec sqlMapTypeNameSpec = (SqlMapTypeNameSpec) typeNameSpec;
            return new FlinkGenericDataType(
                new NodeLocation(sqlMapTypeNameSpec.getParserPos().getLineNum(),
                    sqlMapTypeNameSpec.getParserPos().getColumnNum()),
                sqlMapTypeNameSpec.toString(),
                sqlMapTypeNameSpec.getTypeName().getSimple(),
                ImmutableList.of(
                    new TypeParameter(visitColumnType(sqlMapTypeNameSpec.getKeyType().getTypeNameSpec())),
                    new TypeParameter(visitColumnType(sqlMapTypeNameSpec.getValType().getTypeNameSpec()))));
        } else if (typeNameSpec instanceof SqlRawTypeNameSpec) {
            SqlRawTypeNameSpec sqlRawTypeNameSpec = (SqlRawTypeNameSpec) typeNameSpec;
            // SqlRawTypeNameSpec没有提供访问内部变量的入口，这里先使用反射实现
            try {
                java.lang.reflect.Field classNameField = SqlRawTypeNameSpec.class.getDeclaredField("className");
                classNameField.setAccessible(true);
                SqlNode className = (SqlNode) classNameField.get(sqlRawTypeNameSpec);
                java.lang.reflect.Field serializerStringField = SqlRawTypeNameSpec.class.getDeclaredField("serializerString");
                serializerStringField.setAccessible(true);
                SqlNode serializerString = (SqlNode) serializerStringField.get(sqlRawTypeNameSpec);
                return new FlinkRawDataType(Lists.newArrayList(
                    QualifiedName.of(StripUtils.strip(className.toString())),
                    QualifiedName.of(StripUtils.strip(serializerString.toString()))));
            } catch (NoSuchFieldException | IllegalAccessException e) {
                throw new ParseException(e.getMessage());
            }

        } else {
            if (typeNameSpec instanceof SqlAlienSystemTypeNameSpec) {
                return new FlinkGenericDataType(new NodeLocation(typeNameSpec.getParserPos().getLineNum(),
                    typeNameSpec.getParserPos().getColumnNum()),
                    typeNameSpec.toString(), FlinkDataTypeName.STRING.getValue(), Collections.emptyList());
            }
            String dataTypeName = typeNameSpec.getTypeName().getSimple();
            IDataTypeName dataType = FlinkDataTypeName.getByValue(dataTypeName);

            List<DataTypeParameter> list = Lists.newArrayList();
            SqlBasicTypeNameSpec sqlBasicTypeNameSpec = (SqlBasicTypeNameSpec) typeNameSpec;
            if (sqlBasicTypeNameSpec.getPrecision() > -1) {
                list.add(new NumericParameter(String.valueOf(sqlBasicTypeNameSpec.getPrecision())));
            }
            if (sqlBasicTypeNameSpec.getScale() > -1) {
                list.add(new NumericParameter(String.valueOf(sqlBasicTypeNameSpec.getScale())));
            }
            return new FlinkGenericDataType(new NodeLocation(typeNameSpec.getParserPos().getLineNum(),
                typeNameSpec.getParserPos().getColumnNum()),
                typeNameSpec.toString(), dataType.getValue(), list);
        }
    }

    private PartitionedBy visitPartitionBy(SqlNodeList partitionKeyList) {
        List<ColumnDefinition> columnDefinitions = partitionKeyList.stream().map(partitionKey -> {
            SqlIdentifier sqlIdentifier = (SqlIdentifier) partitionKey;
            return ColumnDefinition.builder().colName(new Identifier(sqlIdentifier.getSimple())).build();
        }).collect(Collectors.toList());
        return new PartitionedBy(columnDefinitions);
    }

    private List<BaseConstraint> visitConstraints(List<SqlTableConstraint> tableConstraints) {
        List<BaseConstraint> constraints = new ArrayList<>();
        tableConstraints.forEach(tableConstraint -> {
            if (tableConstraint.isPrimaryKey()) {
                Identifier constraintName = tableConstraint.getConstraintName().isPresent()
                    ? new Identifier(tableConstraint.getConstraintName().get()) : IdentifierUtil.sysIdentifier();
                List<Identifier> columns = tableConstraint.getColumns().stream().map(column -> {
                    SqlIdentifier sqlIdentifier = (SqlIdentifier) column;
                    return new Identifier(sqlIdentifier.getSimple());
                }).collect(Collectors.toList());
                PrimaryConstraint primaryConstraint = new PrimaryConstraint(constraintName, columns);
                constraints.add(primaryConstraint);
            }
        });
        return constraints;
    }

    private List<Property> visitProperties(SqlNodeList partitionKeyList) {
        return partitionKeyList.stream().map(partitionKey -> {
            SqlTableOption sqlTableOption = (SqlTableOption) partitionKey;
            String key = StripUtils.strip(sqlTableOption.getKey().toString());
            String value = StripUtils.strip(sqlTableOption.getValue().toString());
            return new Property(key, value);
        }).collect(Collectors.toList());
    }

    private Comment visitComment(SqlCharStringLiteral comment) {
        return new Comment(comment.toString());
    }

    private WaterMarkConstraint visitWaterMark(SqlWatermark watermark) {
        SqlIdentifier eventTimeColumnName = watermark.getEventTimeColumnName();
        SqlBasicCall watermarkStrategy = (SqlBasicCall) watermark.getWatermarkStrategy();
        String expression = watermarkStrategy.getOperandList().stream()
            .map(SqlNode::toString)
            .collect(Collectors.joining(watermarkStrategy.getOperator().getName()));
        return new WaterMarkConstraint(IdentifierUtil.sysIdentifier(),
            new Identifier(eventTimeColumnName.getSimple()), new StringLiteral(expression));
    }

}
