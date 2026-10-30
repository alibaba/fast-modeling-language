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

package com.aliyun.fastmodel.transform.clickhouse.client.converter;

import java.math.BigDecimal;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.core.parser.LanguageParser;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.datatype.GenericDataType;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName.Dimension;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.literal.BooleanLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.DecimalLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.DoubleLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.NullLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.PrimaryConstraint;
import com.aliyun.fastmodel.core.tree.util.StringLiteralUtil;
import com.aliyun.fastmodel.transform.api.client.PropertyConverter;
import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.api.client.dto.table.Column;
import com.aliyun.fastmodel.transform.api.client.dto.table.Table;
import com.aliyun.fastmodel.transform.api.client.dto.table.TableConfig;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.api.datatype.simple.ISimpleDataTypeName;
import com.aliyun.fastmodel.transform.api.datatype.simple.SimpleDataTypeName;
import com.aliyun.fastmodel.transform.api.extension.client.converter.ExtensionClientConverter;
import com.aliyun.fastmodel.transform.clickhouse.context.ClickHouseContext;
import com.aliyun.fastmodel.transform.clickhouse.format.ClickHouseExpressionVisitor;
import com.aliyun.fastmodel.transform.clickhouse.format.ClickHouseOutVisitor;
import com.aliyun.fastmodel.transform.clickhouse.format.ClickHousePropertyKey;
import com.aliyun.fastmodel.transform.clickhouse.parser.ClickHouseLanguageParser;
import com.aliyun.fastmodel.transform.clickhouse.parser.tree.ClickHouseDataTypeName;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * clickHouse的client dto与FML模型的互转
 * <p>
 * 负责建表链路里的双向转换：Table DTO -> CreateTable AST(reverseTable) 与
 * CreateTable AST -> Table DTO(transformTable)。表级引擎子句通过 {@link ClickHousePropertyKey} 传递。
 *
 * @author panguanjing
 */
public class ClickHouseClientConverter extends ExtensionClientConverter<ClickHouseContext> {

    /**
     * 裸列名(可带反引号/双引号)，用于判断 PARTITION BY 是否只是列名列表
     */
    private static final Pattern COLUMN_NAME_PATTERN = Pattern.compile("^[A-Za-z_][A-Za-z0-9_]*$");

    private final ClickHouseLanguageParser clickHouseLanguageParser = new ClickHouseLanguageParser();

    private final ClickHousePropertyConverter clickHousePropertyConverter = new ClickHousePropertyConverter();

    @Override
    public PropertyConverter getPropertyConverter() {
        return clickHousePropertyConverter;
    }

    @Override
    public LanguageParser getLanguageParser() {
        return clickHouseLanguageParser;
    }

    @Override
    public IDataTypeName getDataTypeName(String dataTypeName) {
        return ClickHouseDataTypeName.getByValue(dataTypeName);
    }

    @Override
    public String getRaw(Node node) {
        ClickHouseOutVisitor clickHouseOutVisitor = new ClickHouseOutVisitor(ClickHouseContext.builder().build());
        node.accept(clickHouseOutVisitor, 0);
        return clickHouseOutVisitor.getBuilder().toString();
    }

    @Override
    public BaseDataType getDataType(Column column) {
        String dataTypeName = column.getDataType();
        if (StringUtils.isBlank(dataTypeName)) {
            return null;
        }
        ReverseContext context = ReverseContext.builder().build();
        //带参数的类型文本(如 Decimal(10,2)、Array(String))直接按ClickHouse类型解析
        if (dataTypeName.indexOf('(') > 0) {
            return (BaseDataType)clickHouseLanguageParser.parseDataType(dataTypeName, context);
        }
        ClickHouseDataTypeName dataType = ClickHouseDataTypeName.getByValue(dataTypeName);
        String typeValue = dataType.getValue();
        Dimension dimension = dataType.getDimension();
        if (isScaleOnlyDecimal(dataType)) {
            Integer scale = column.getScale() != null ? column.getScale() : column.getPrecision();
            validateScale(scale, typeValue);
            return (BaseDataType)clickHouseLanguageParser.parseDataType(
                String.format(ONE_DIMENSION, typeValue, scale), context);
        }
        if (dimension == Dimension.ONE && column.getLength() != null && column.getLength() > 0) {
            return (BaseDataType)clickHouseLanguageParser.parseDataType(String.format(ONE_DIMENSION, typeValue, column.getLength()), context);
        }
        if (dataType == ClickHouseDataTypeName.DateTime64 && column.getPrecision() != null) {
            return (BaseDataType)clickHouseLanguageParser.parseDataType(
                String.format(ONE_DIMENSION, typeValue, column.getPrecision()), context);
        }
        if (dimension == Dimension.TWO && column.getPrecision() != null) {
            if (column.getScale() == null) {
                throw new IllegalArgumentException("ClickHouse " + typeValue + " requires both precision and scale");
            }
            if (column.getPrecision() <= 0 || column.getScale() < 0 || column.getScale() > column.getPrecision()) {
                throw new IllegalArgumentException("Invalid ClickHouse " + typeValue + " precision/scale");
            }
            return (BaseDataType)clickHouseLanguageParser.parseDataType(
                String.format(TWO_DIMENSION, typeValue, column.getPrecision(), column.getScale()), context);
        }
        if (dimension == Dimension.TWO) {
            throw new IllegalArgumentException("ClickHouse " + typeValue + " requires precision and scale");
        }
        return (BaseDataType)clickHouseLanguageParser.parseDataType(typeValue, context);
    }

    private boolean isScaleOnlyDecimal(ClickHouseDataTypeName dataType) {
        return dataType == ClickHouseDataTypeName.Decimal32 || dataType == ClickHouseDataTypeName.Decimal64
            || dataType == ClickHouseDataTypeName.Decimal128 || dataType == ClickHouseDataTypeName.Decimal256;
    }

    private void validateScale(Integer scale, String typeValue) {
        if (scale == null || scale < 0) {
            throw new IllegalArgumentException("ClickHouse " + typeValue + " requires a non-negative scale");
        }
    }

    @Override
    protected Column getColumn(ColumnDefinition c, boolean partitionKey, Integer partitionKeyIndex) {
        Column column = super.getColumn(c, partitionKey, partitionKeyIndex);
        BaseDataType dataType = c.getDataType();
        if (!(dataType instanceof GenericDataType)) {
            return column;
        }
        GenericDataType genericDataType = (GenericDataType)dataType;
        IDataTypeName typeName = genericDataType.getTypeName();
        boolean hasArguments = CollectionUtils.isNotEmpty(genericDataType.getArguments());
        if (hasArguments && typeName != null && isScaleOnlyDecimal(ClickHouseDataTypeName.getByValue(typeName.getValue()))
            && genericDataType.getArguments().get(0) instanceof com.aliyun.fastmodel.core.tree.datatype.NumericParameter) {
            com.aliyun.fastmodel.core.tree.datatype.NumericParameter scale =
                (com.aliyun.fastmodel.core.tree.datatype.NumericParameter)genericDataType.getArguments().get(0);
            column.setScale(Integer.valueOf(scale.getValue()));
        }
        //ClickHouse的复合类型(Array、Map、Tuple、Enum等)需要保留完整的类型文本
        if (hasArguments && typeName != null && typeName.getDimension() == Dimension.ZERO) {
            column.setDataType(formatExpression(dataType));
        }
        return column;
    }

    @Override
    protected String formatExpression(BaseExpression baseExpression) {
        return new ClickHouseExpressionVisitor(ClickHouseContext.builder().build()).process(baseExpression);
    }

    /**
     * ClickHouse 的分区列直接生成 PARTITION BY (col...)，
     * 不使用 Extension 的 range/list 分区属性，避免带表属性时分区列被丢弃
     *
     * @param table 表
     * @param columns 列
     * @return 分区定义
     */
    @Override
    protected PartitionedBy toPartitionedBy(Table table, List<Column> columns) {
        if (CollectionUtils.isEmpty(columns)) {
            return null;
        }
        List<ColumnDefinition> partitionColumns = columns.stream()
            .filter(Column::isPartitionKey)
            .sorted(Comparator.comparing(Column::getPartitionKeyIndex, Comparator.nullsLast(Comparator.naturalOrder())))
            .map(c -> ColumnDefinition.builder()
                .colName(new Identifier(c.getName()))
                .dataType(getDataType(c))
                .notNull(BooleanUtils.isFalse(c.isNullable()))
                .build())
            .collect(Collectors.toList());
        if (CollectionUtils.isEmpty(partitionColumns)) {
            return null;
        }
        return new PartitionedBy(partitionColumns);
    }

    /**
     * 反向转换时把 PARTITION BY 里的裸列名回填为列的分区标记
     * <p>
     * 仅当 PARTITION BY 是裸列名列表(如 (region, ds))时回填；toYYYYMM(ts)、id % 5 等表达式分区
     * 只保留 partition_by 属性，不做列标记，避免把派生表达式当成直接分区列。
     *
     * @param node AST
     * @param context 上下文
     * @return 表结构
     */
    @Override
    public Table convertToTable(Node node, ClickHouseContext context) {
        Table table = super.convertToTable(node, context);
        if (table == null || CollectionUtils.isEmpty(table.getColumns())) {
            return table;
        }
        List<String> partitionColumnNames = toPartitionColumnNames(
            getClientPropertyValue(table.getProperties(), ClickHousePropertyKey.TABLE_PARTITION_BY));
        if (CollectionUtils.isEmpty(partitionColumnNames)) {
            return table;
        }
        for (int i = 0; i < partitionColumnNames.size(); i++) {
            Column column = findColumn(table.getColumns(), partitionColumnNames.get(i));
            if (column == null) {
                continue;
            }
            column.setPartitionKey(true);
            column.setPartitionKeyIndex(i);
        }
        return table;
    }

    /**
     * 解析 PARTITION BY 表达式中的裸列名
     *
     * @param partitionBy 分区表达式
     * @return 裸列名列表；表达式中含函数、运算或字面量时返回空
     */
    private List<String> toPartitionColumnNames(String partitionBy) {
        if (StringUtils.isBlank(partitionBy)) {
            return Lists.newArrayList();
        }
        String expression = StringUtils.trim(partitionBy);
        if (StringUtils.startsWith(expression, "(") && StringUtils.endsWith(expression, ")")) {
            expression = expression.substring(1, expression.length() - 1);
        }
        List<String> columnNames = Lists.newArrayList();
        for (String item : StringUtils.split(expression, ',')) {
            String columnName = StringUtils.trim(item);
            if (!isBareColumnName(columnName)) {
                return Lists.newArrayList();
            }
            columnNames.add(stripQuote(columnName));
        }
        return columnNames;
    }

    private boolean isBareColumnName(String columnName) {
        if (StringUtils.isBlank(columnName)) {
            return false;
        }
        String value = stripQuote(columnName);
        return COLUMN_NAME_PATTERN.matcher(value).matches();
    }

    private String stripQuote(String columnName) {
        if (columnName.length() > 1 && (columnName.startsWith("`") && columnName.endsWith("`")
            || columnName.startsWith("\"") && columnName.endsWith("\""))) {
            return columnName.substring(1, columnName.length() - 1);
        }
        return columnName;
    }

    private Column findColumn(List<Column> columns, String columnName) {
        return columns.stream()
            .filter(c -> StringUtils.equals(c.getName(), columnName))
            .findFirst()
            .orElseGet(() -> columns.stream()
                .filter(c -> StringUtils.equalsIgnoreCase(c.getName(), columnName))
                .findFirst()
                .orElse(null));
    }

    private String getClientPropertyValue(List<BaseClientProperty> properties, ClickHousePropertyKey propertyKey) {
        if (CollectionUtils.isEmpty(properties)) {
            return null;
        }
        return properties.stream()
            .filter(p -> StringUtils.equalsIgnoreCase(p.getKey(), propertyKey.getValue()))
            .findFirst()
            .map(p -> String.valueOf(p.getValue()))
            .orElse(null);
    }

    @Override
    public Node convertToNode(Table table, TableConfig tableConfig) {
        Node node = super.convertToNode(table, tableConfig);
        if (!(node instanceof CreateTable)) {
            return node;
        }
        CreateTable createTable = (CreateTable)node;
        if (hasTableProperty(createTable.getProperties(), ClickHousePropertyKey.TABLE_PRIMARY_KEY)) {
            return createTable;
        }
        String primaryKey = toPrimaryKey(createTable);
        if (StringUtils.isBlank(primaryKey)) {
            return createTable;
        }
        createTable.getProperties().add(new Property(ClickHousePropertyKey.TABLE_PRIMARY_KEY.getValue(), primaryKey));
        return createTable;
    }

    @Override
    protected BaseExpression toDefaultValueExpression(BaseDataType baseDataType, String defaultValue) {
        if (defaultValue == null || baseDataType == null) {
            return null;
        }
        String value = StringLiteralUtil.strip(defaultValue);
        if (StringUtils.equalsIgnoreCase(value, "NULL")) {
            return new NullLiteral();
        }
        IDataTypeName typeName = baseDataType.getTypeName();
        if (typeName instanceof ISimpleDataTypeName) {
            SimpleDataTypeName simpleDataTypeName = ((ISimpleDataTypeName)typeName).getSimpleDataTypeName();
            if (simpleDataTypeName == SimpleDataTypeName.NUMBER) {
                return new DecimalLiteral(value);
            }
            if (simpleDataTypeName == SimpleDataTypeName.BOOLEAN) {
                return new BooleanLiteral(value);
            }
        }
        return new StringLiteral(value);
    }

    @Override
    public List<BaseClientProperty> toBaseClientProperty(CreateTable createTable) {
        List<BaseClientProperty> properties = super.toBaseClientProperty(createTable);
        if (properties == null) {
            properties = Lists.newArrayList();
        }
        //主键约束回写为ClickHouse的PRIMARY KEY子句，保证反向转换后仍能生成合法的建表语句
        if (hasProperty(properties, ClickHousePropertyKey.TABLE_PRIMARY_KEY)) {
            return properties;
        }
        String primaryKey = toPrimaryKey(createTable);
        if (StringUtils.isBlank(primaryKey)) {
            return properties;
        }
        BaseClientProperty baseClientProperty = getPropertyConverter().create(ClickHousePropertyKey.TABLE_PRIMARY_KEY.getValue(), primaryKey);
        properties.add(baseClientProperty);
        return properties;
    }

    private boolean hasProperty(List<BaseClientProperty> properties, ClickHousePropertyKey propertyKey) {
        return properties.stream().anyMatch(p -> StringUtils.equalsIgnoreCase(p.getKey(), propertyKey.getValue()));
    }

    private boolean hasTableProperty(List<Property> properties, ClickHousePropertyKey propertyKey) {
        if (CollectionUtils.isEmpty(properties)) {
            return false;
        }
        return properties.stream().anyMatch(p -> StringUtils.equalsIgnoreCase(p.getName(), propertyKey.getValue()));
    }

    private String toPrimaryKey(CreateTable createTable) {
        if (!createTable.isConstraintEmpty()) {
            for (BaseConstraint constraint : createTable.getConstraintStatements()) {
                if (!(constraint instanceof PrimaryConstraint)) {
                    continue;
                }
                PrimaryConstraint primaryConstraint = (PrimaryConstraint)constraint;
                List<Identifier> colNames = primaryConstraint.getColNames();
                if (CollectionUtils.isEmpty(colNames)) {
                    continue;
                }
                return colNames.stream().map(Identifier::getValue).collect(Collectors.joining(",", "(", ")"));
            }
        }
        if (createTable.isColumnEmpty()) {
            return null;
        }
        List<String> primaryColumns = createTable.getColumnDefines().stream()
            .filter(c -> BooleanUtils.isTrue(c.getPrimary()))
            .map(c -> c.getColName().getValue())
            .collect(Collectors.toList());
        if (CollectionUtils.isEmpty(primaryColumns)) {
            return null;
        }
        return primaryColumns.size() == 1 ? primaryColumns.get(0) : primaryColumns.stream()
            .collect(Collectors.joining(",", "(", ")"));
    }

    @Override
    protected String toDefaultBaseValue(BaseExpression defaultValue) {
        if (defaultValue instanceof DoubleLiteral) {
            return BigDecimal.valueOf(((DoubleLiteral)defaultValue).getValue()).stripTrailingZeros().toPlainString();
        }
        return super.toDefaultBaseValue(defaultValue);
    }

    @Override
    protected String toSchema(CreateTable createTable, ClickHouseContext transformContext) {
        //ClickHouse的库表模型是 database.table 两层，第一段即database
        return null;
    }

    @Override
    protected String toDatabase(CreateTable createTable, String database) {
        if (createTable.getQualifiedName().isJoinPath()) {
            return createTable.getQualifiedName().getFirst();
        }
        return database;
    }
}
