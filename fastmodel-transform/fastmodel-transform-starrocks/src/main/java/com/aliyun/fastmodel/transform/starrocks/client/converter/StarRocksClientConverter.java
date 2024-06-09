package com.aliyun.fastmodel.transform.starrocks.client.converter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.common.utils.StripUtils;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName.Dimension;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.atom.FunctionCall;
import com.aliyun.fastmodel.core.tree.expr.enums.DateTimeEnum;
import com.aliyun.fastmodel.core.tree.expr.literal.IntervalLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.ListStringLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.LongLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.PrimaryConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.index.IndexColumnName;
import com.aliyun.fastmodel.core.tree.statement.table.index.TableIndex;
import com.aliyun.fastmodel.core.tree.util.IdentifierUtil;
import com.aliyun.fastmodel.transform.api.client.PropertyConverter;
import com.aliyun.fastmodel.transform.api.client.converter.BaseClientConverter;
import com.aliyun.fastmodel.transform.api.client.dto.constraint.Constraint;
import com.aliyun.fastmodel.transform.api.client.dto.constraint.ConstraintType;
import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.api.client.dto.property.StringProperty;
import com.aliyun.fastmodel.transform.api.client.dto.table.Column;
import com.aliyun.fastmodel.transform.api.client.dto.table.Table;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.api.datatype.simple.ISimpleDataTypeName;
import com.aliyun.fastmodel.transform.api.datatype.simple.SimpleDataTypeName;
import com.aliyun.fastmodel.transform.starrocks.client.constraint.StarRocksConstraintType;
import com.aliyun.fastmodel.transform.starrocks.client.constraint.StarRocksDistributeConstraint;
import com.aliyun.fastmodel.transform.starrocks.client.property.table.ColumnExpressionPartitionProperty;
import com.aliyun.fastmodel.transform.starrocks.client.property.table.ListPartitionProperty;
import com.aliyun.fastmodel.transform.starrocks.client.property.table.MultiRangePartitionProperty;
import com.aliyun.fastmodel.transform.starrocks.client.property.table.SingleRangePartitionProperty;
import com.aliyun.fastmodel.transform.starrocks.client.property.table.TablePartitionRaw;
import com.aliyun.fastmodel.transform.starrocks.client.property.table.TimeExpressionPartitionProperty;
import com.aliyun.fastmodel.transform.starrocks.client.property.table.partition.ArrayClientPartitionKey;
import com.aliyun.fastmodel.transform.starrocks.client.property.table.partition.BaseClientPartitionKey;
import com.aliyun.fastmodel.transform.starrocks.client.property.table.partition.ColumnExpressionClientPartition;
import com.aliyun.fastmodel.transform.starrocks.client.property.table.partition.LessThanClientPartitionKey;
import com.aliyun.fastmodel.transform.starrocks.client.property.table.partition.ListClientPartition;
import com.aliyun.fastmodel.transform.starrocks.client.property.table.partition.MultiRangeClientPartition;
import com.aliyun.fastmodel.transform.starrocks.client.property.table.partition.PartitionClientValue;
import com.aliyun.fastmodel.transform.starrocks.client.property.table.partition.SingleRangeClientPartition;
import com.aliyun.fastmodel.transform.starrocks.client.property.table.partition.TimeExpressionClientPartition;
import com.aliyun.fastmodel.transform.starrocks.context.StarRocksContext;
import com.aliyun.fastmodel.transform.starrocks.format.StarRocksOutVisitor;
import com.aliyun.fastmodel.transform.starrocks.format.StarRocksProperty;
import com.aliyun.fastmodel.transform.starrocks.parser.StarRocksLanguageParser;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.constraint.AggregateKeyConstraint;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.constraint.DuplicateKeyConstraint;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.constraint.desc.DistributeConstraint;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.constraint.desc.OrderByConstraint;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.datatype.StarRocksDataTypeName;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.ArrayPartitionKey;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.ExpressionPartitionBy;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.LessThanPartitionKey;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.ListPartitionValue;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.ListPartitionedBy;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.MultiItemListPartition;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.MultiRangePartition;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.PartitionDesc;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.PartitionKey;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.PartitionValue;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.RangePartitionedBy;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.SingleItemListPartition;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.SingleRangePartition;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * StarRocksClientConverter
 *
 * @author panguanjing
 * @date 2023/9/16
 */
public class StarRocksClientConverter extends BaseClientConverter<StarRocksContext> {

    public static final String UUID = "uuid";
    public static final String UUID_NUMERIC = "uuid_numeric";
    public static final String SUFFIX = "()";

    private final StarRocksLanguageParser starRocksLanguageParser = new StarRocksLanguageParser();

    private final StarRocksPropertyConverter starRocksPropertyConverter = new StarRocksPropertyConverter();

    @Override
    public PropertyConverter getPropertyConverter() {
        return starRocksPropertyConverter;
    }

    @Override
    protected List<Constraint> toOutlineConstraint(CreateTable createTable) {
        List<Constraint> outlineConstraint = super.toOutlineConstraint(createTable);
        if (createTable.isConstraintEmpty() && createTable.isIndexEmpty()) {
            return outlineConstraint;
        }
        List<BaseConstraint> constraintStatements = createTable.getConstraintStatements();
        if (outlineConstraint == null || outlineConstraint.isEmpty()) {
            outlineConstraint = Lists.newArrayList();
        }
        if (!constraintStatements.isEmpty()) {
            for (BaseConstraint baseConstraint : constraintStatements) {
                convertConstraint(baseConstraint, outlineConstraint);
            }
        }
        if (!createTable.isIndexEmpty()) {
            for (TableIndex index : createTable.getTableIndexList()) {
                List<IndexColumnName> indexColumnNames = index.getIndexColumnNames();
                List<String> collect = indexColumnNames.stream().map(i -> {
                    return i.getColumnName().toString();
                }).collect(Collectors.toList());
                List<BaseClientProperty> es = Lists.newArrayList();
                List<Property> properties = index.getProperties();
                if (properties != null) {
                    for (Property p : properties) {
                        BaseClientProperty baseClientProperty = getPropertyConverter().create(p.getName(), p.getValue());
                        es.add(baseClientProperty);
                    }
                }
                Constraint constraint = Constraint.builder()
                    .name(index.getName().getValue())
                    .columns(collect)
                    .type(StarRocksConstraintType.INDEX)
                    .properties(es)
                    .build();
                outlineConstraint.add(constraint);
            }
        }
        return outlineConstraint;

    }

    private void convertConstraint(BaseConstraint baseConstraint, List<Constraint> outlineConstraint) {
        if (baseConstraint instanceof DistributeConstraint) {
            DistributeConstraint distributeConstraint = (DistributeConstraint)baseConstraint;
            StarRocksDistributeConstraint starRocksDistributeConstraint = toStarRocksDistributeConstraint(distributeConstraint);
            outlineConstraint.add(starRocksDistributeConstraint);
            return;
        }
        if (baseConstraint instanceof OrderByConstraint) {
            OrderByConstraint orderByConstraint = (OrderByConstraint)baseConstraint;
            Constraint constraint = toOrderClientConstraint(orderByConstraint);
            outlineConstraint.add(constraint);
            return;
        }

        if (baseConstraint instanceof AggregateKeyConstraint) {
            AggregateKeyConstraint aggregateKeyConstraint = (AggregateKeyConstraint)baseConstraint;
            Constraint aggreKeyConstraint = toAggregateClientConstraint(aggregateKeyConstraint);
            outlineConstraint.add(aggreKeyConstraint);
        }

        if (baseConstraint instanceof DuplicateKeyConstraint) {
            DuplicateKeyConstraint duplicateKeyConstraint = (DuplicateKeyConstraint)baseConstraint;
            Constraint duplicateClientConstraint = toDuplicateClientConstraint(duplicateKeyConstraint);
            outlineConstraint.add(duplicateClientConstraint);
        }
    }

    private Constraint toDuplicateClientConstraint(DuplicateKeyConstraint aggregateKeyConstraint) {
        List<String> collect = aggregateKeyConstraint.getColumns().stream().map(Identifier::getValue).collect(Collectors.toList());
        return Constraint.builder().name(aggregateKeyConstraint.getName().getValue())
            .columns(collect)
            .type(StarRocksConstraintType.DUPLICATE_KEY)
            .build();
    }

    private Constraint toAggregateClientConstraint(AggregateKeyConstraint aggregateKeyConstraint) {
        List<String> collect = aggregateKeyConstraint.getColumns().stream().map(Identifier::getValue).collect(Collectors.toList());
        return Constraint.builder().name(aggregateKeyConstraint.getName().getValue())
            .columns(collect).type(StarRocksConstraintType.AGGREGATE_KEY)
            .build();
    }

    private Constraint toOrderClientConstraint(OrderByConstraint orderByConstraint) {
        List<String> collect = orderByConstraint.getColumns().stream().map(Identifier::getValue).collect(Collectors.toList());
        return Constraint.builder().name(orderByConstraint.getName().getValue())
            .columns(collect)
            .type(StarRocksConstraintType.ORDER_BY)
            .build();
    }

    private StarRocksDistributeConstraint toStarRocksDistributeConstraint(DistributeConstraint distributeConstraint) {
        StarRocksDistributeConstraint starRocksDistributeConstraint = new StarRocksDistributeConstraint();
        starRocksDistributeConstraint.setBucket(distributeConstraint.getBucket());
        if (CollectionUtils.isNotEmpty(distributeConstraint.getColumns())) {
            starRocksDistributeConstraint.setColumns(
                distributeConstraint.getColumns().stream().map(Identifier::getValue).collect(Collectors.toList()));
        }
        starRocksDistributeConstraint.setRandom(distributeConstraint.getRandom());
        return starRocksDistributeConstraint;
    }

    @Override
    protected List<BaseConstraint> toConstraint(List<Column> columns, List<Constraint> constraints) {
        List<BaseConstraint> baseConstraints = super.toConstraint(columns, constraints);
        if (constraints == null) {
            return baseConstraints;
        }
        for (Constraint constraint : constraints) {
            ConstraintType type = constraint.getType();
            if (type == StarRocksConstraintType.DISTRIBUTE) {
                DistributeConstraint distributeConstraint = toDistributeConstraint(constraint);
                if (distributeConstraint == null) {
                    continue;
                }
                baseConstraints.add(distributeConstraint);
            }
            if (type == StarRocksConstraintType.ORDER_BY) {
                OrderByConstraint orderByConstraint = toOrderByConstraint(constraint);
                baseConstraints.add(orderByConstraint);
            }

            if (type == StarRocksConstraintType.DUPLICATE_KEY) {
                DuplicateKeyConstraint duplicateKeyConstraint = toDuplicateKeyConstraint(constraint);
                baseConstraints.add(duplicateKeyConstraint);
            }

            if (type == StarRocksConstraintType.AGGREGATE_KEY) {
                AggregateKeyConstraint aggregateKeyConstraint = toAggregateKeyConstraint(constraint);
                baseConstraints.add(aggregateKeyConstraint);
            }
        }
        return baseConstraints;
    }

    private AggregateKeyConstraint toAggregateKeyConstraint(Constraint constraint) {
        return new AggregateKeyConstraint(
            IdentifierUtil.sysIdentifier(),
            constraint.getColumns().stream().map(Identifier::new).collect(Collectors.toList())
        );
    }

    private DuplicateKeyConstraint toDuplicateKeyConstraint(Constraint constraint) {
        return new DuplicateKeyConstraint(
            IdentifierUtil.sysIdentifier(),
            constraint.getColumns().stream().map(Identifier::new).collect(Collectors.toList()),
            true
        );
    }

    private OrderByConstraint toOrderByConstraint(Constraint constraint) {
        String name = constraint.getName();
        Identifier nameIdentifier = null;
        if (StringUtils.isBlank(name)) {
            nameIdentifier = IdentifierUtil.sysIdentifier();
        } else {
            nameIdentifier = new Identifier(name);
        }
        List<Identifier> collect = constraint.getColumns().stream().map(Identifier::new).collect(Collectors.toList());
        return new OrderByConstraint(nameIdentifier, collect);
    }

    private DistributeConstraint toDistributeConstraint(Constraint constraint) {
        if (constraint instanceof StarRocksDistributeConstraint) {
            StarRocksDistributeConstraint starRocksDistributeConstraint = (StarRocksDistributeConstraint)constraint;
            List<Identifier> list = null;
            if (CollectionUtils.isNotEmpty(constraint.getColumns())) {
                list = constraint.getColumns().stream().map(Identifier::new).collect(Collectors.toList());
                return new DistributeConstraint(list, starRocksDistributeConstraint.getBucket());
            } else {
                return new DistributeConstraint(starRocksDistributeConstraint.getRandom(), starRocksDistributeConstraint.getBucket());
            }
        }
        List<Identifier> list = null;
        if (CollectionUtils.isNotEmpty(constraint.getColumns())) {
            list = constraint.getColumns().stream().map(Identifier::new).collect(Collectors.toList());
            return new DistributeConstraint(list, null);
        }
        return null;
    }

    @Override
    protected Column getColumn(ColumnDefinition c, boolean partitionKey, Integer partitionKeyIndex) {
        Column column = super.getColumn(c, partitionKey, partitionKeyIndex);
        List<Property> columnProperties = c.getColumnProperties();
        if (CollectionUtils.isEmpty(columnProperties)) {
            return column;
        }
        List<BaseClientProperty> baseClientProperties = Lists.newArrayList();
        for (Property property : columnProperties) {
            BaseClientProperty baseClientProperty = getPropertyConverter().create(property.getName(), property.getValue());
            baseClientProperties.add(baseClientProperty);
        }
        column.setProperties(baseClientProperties);
        return column;
    }

    @Override
    protected BaseDataType getDataType(Column column) {
        String dataTypeName = column.getDataType();
        if (StringUtils.isBlank(dataTypeName)) {
            throw new IllegalArgumentException("dataType name can't be null:" + column.getName());
        }
        IDataTypeName byValue = StarRocksDataTypeName.getByValue(dataTypeName);
        Dimension dimension = byValue.getDimension();
        ReverseContext context = ReverseContext.builder().build();
        if (dimension == null || dimension == Dimension.ZERO) {
            return starRocksLanguageParser.parseDataType(dataTypeName, context);
        }
        if (dimension == Dimension.ONE) {
            boolean isValidLength = column.getLength() != null && column.getLength() > 0;
            if (isValidLength) {
                String dt = String.format(ONE_DIMENSION, dataTypeName, column.getLength());
                return starRocksLanguageParser.parseDataType(dt, context);
            }
        }
        if (dimension == Dimension.TWO) {
            if (column.getPrecision() != null) {
                if (column.getScale() == null) {
                    return starRocksLanguageParser.parseDataType(String.format(ONE_DIMENSION, dataTypeName, column.getPrecision()), context);
                }
                return starRocksLanguageParser.parseDataType(String.format(TWO_DIMENSION, dataTypeName, column.getPrecision(), column.getScale()),
                    context);
            }
        }
        return starRocksLanguageParser.parseDataType(dataTypeName, context);
    }

    @Override
    protected PartitionedBy toPartitionedBy(Table table, List<Column> columns) {
        //convert to starRockPartitionBy
        List<BaseClientProperty> properties = table.getProperties();
        List<ColumnDefinition> list = columns.stream()
            .filter(Column::isPartitionKey)
            .sorted(Comparator.comparing(Column::getPartitionKeyIndex))
            .map(x -> {
                List<BaseClientProperty> clientProperties = x.getProperties();
                return ColumnDefinition.builder().colName(new Identifier(x.getName()))
                    .properties(toProperty(table, clientProperties))
                    .build();
            })
            .collect(Collectors.toList());
        return toPartition(list, properties);
    }

    @Override
    protected List<TableIndex> toTableIndex(Table table, List<Column> columns) {
        if (table == null || CollectionUtils.isEmpty(table.getConstraints())) {
            return Collections.emptyList();
        }
        List<Constraint> indexConstraints = table.getConstraints().stream()
            .filter(constraint -> StarRocksConstraintType.INDEX == constraint.getType())
            .collect(Collectors.toList());
        if (CollectionUtils.isEmpty(indexConstraints)) {
            return Collections.emptyList();
        }
        return indexConstraints.stream().map(indexConstraint -> {
            Identifier indexName = new Identifier(indexConstraint.getName());
            List<IndexColumnName> indexColumnNames = indexConstraint.getColumns().stream()
                .map(column -> new IndexColumnName(new Identifier(column), null, null)).collect(Collectors.toList());

            List<Property> properties = null;
            if (CollectionUtils.isNotEmpty(indexConstraint.getProperties())) {
                properties = indexConstraint.getProperties().stream().map(property -> {
                    return new Property(property.getKey(), (String)property.getValue());
                }).collect(Collectors.toList());
            }
            return new TableIndex(indexName, indexColumnNames, properties);
        }).collect(Collectors.toList());
    }

    @Override
    protected List<BaseClientProperty> toBaseClientProperty(CreateTable createTable) {
        List<BaseClientProperty> propertyList = Lists.newArrayList();
        if (!createTable.isPropertyEmpty()) {
            List<Property> properties = createTable.getProperties();
            for (Property property : properties) {
                BaseClientProperty baseClientProperty = getPropertyConverter().create(property.getName(), property.getValue());
                propertyList.add(baseClientProperty);
            }
        }

        PartitionedBy partitionedBy = createTable.getPartitionedBy();
        if (createTable.isPartitionEmpty()) {
            return propertyList;
        }
        if (partitionedBy instanceof RangePartitionedBy) {
            processRangePartition((RangePartitionedBy)partitionedBy, propertyList);
        }

        if (partitionedBy instanceof ListPartitionedBy) {
            processListPartition((ListPartitionedBy)partitionedBy, propertyList);
        }

        if (partitionedBy instanceof ExpressionPartitionBy) {
            processExpressionPartition((ExpressionPartitionBy)partitionedBy, propertyList);
        }

        return propertyList;
    }

    /**
     * 如果是三层模型，那么必须是a.b.c三层定义
     * 如果是二层模型，那么传入的a.b, 那么只会将a认为是database
     *
     * @param createTable
     * @param transformContext
     * @return
     */
    @Override
    protected String toSchema(CreateTable createTable, StarRocksContext transformContext) {
        QualifiedName qualifiedName = createTable.getQualifiedName();
        if (!qualifiedName.isJoinPath()) {
            return transformContext.getSchema();
        }
        boolean isSecondSchema = qualifiedName.getOriginalParts().size() == SECOND_INDEX;
        if (isSecondSchema) {
            return transformContext.getSchema();
        }
        boolean isThirdSchema = qualifiedName.getOriginalParts().size() == THIRD_INDEX;
        if (isThirdSchema) {
            return qualifiedName.getParts().get(1);
        }
        return transformContext.getSchema();
    }

    /**
     * get database
     *
     * @param createTable
     * @param database
     * @return
     */
    protected String toDatabase(CreateTable createTable, String database) {
        QualifiedName qualifiedName = createTable.getQualifiedName();
        boolean isThirdSchema = qualifiedName.isJoinPath() && qualifiedName.getOriginalParts().size() == THIRD_INDEX;
        if (isThirdSchema) {
            return qualifiedName.getFirst();
        }
        boolean isSecond = qualifiedName.isJoinPath() && qualifiedName.getOriginalParts().size() == SECOND_INDEX;
        if (isSecond) {
            return qualifiedName.getFirst();
        }
        return database;
    }

    private void processRangePartition(RangePartitionedBy rangePartitionedBy, List<BaseClientProperty> propertyList) {
        List<PartitionDesc> rangePartitions = rangePartitionedBy.getRangePartitions();
        //结构化的返回
        List<BaseClientProperty> list = Lists.newArrayList();
        for (PartitionDesc partitionDesc : rangePartitions) {
            BaseClientProperty baseClientProperty = parseRangePartition(partitionDesc);
            if (baseClientProperty != null) {
                list.add(baseClientProperty);
            }
        }
        //如果解析不了，那么使用visitor的方式进行解析
        if (list.isEmpty()) {
            StarRocksOutVisitor starRocksOutVisitor = new StarRocksOutVisitor(StarRocksContext.builder().build());
            starRocksOutVisitor.visitRangePartitionedBy(rangePartitionedBy, 0);
            TablePartitionRaw baseClientProperty = new TablePartitionRaw();
            baseClientProperty.setKey(StarRocksProperty.TABLE_PARTITION_RAW.getValue());
            baseClientProperty.setValueString(starRocksOutVisitor.getBuilder().toString());
            propertyList.add(baseClientProperty);
        } else {
            propertyList.addAll(list);
        }
    }

    private void processListPartition(ListPartitionedBy listPartitionedBy, List<BaseClientProperty> propertyList) {
        List<PartitionDesc> listPartitions = listPartitionedBy.getListPartitions();
        //结构化的返回
        List<BaseClientProperty> list = Lists.newArrayList();
        for (PartitionDesc partitionDesc : listPartitions) {
            BaseClientProperty baseClientProperty = parseListPartition(partitionDesc);
            if (baseClientProperty != null) {
                list.add(baseClientProperty);
            }
        }

        //如果解析不了，那么使用visitor的方式进行解析
        if (list.isEmpty()) {
            StarRocksOutVisitor starRocksOutVisitor = new StarRocksOutVisitor(StarRocksContext.builder().build());
            starRocksOutVisitor.visitListPartitionedBy(listPartitionedBy, 0);
            StringProperty baseClientProperty = new StringProperty();
            baseClientProperty.setKey(StarRocksProperty.TABLE_PARTITION_RAW.getValue());
            baseClientProperty.setValueString(starRocksOutVisitor.getBuilder().toString());
            propertyList.add(baseClientProperty);
        } else {
            propertyList.addAll(list);
        }
    }

    private void processExpressionPartition(ExpressionPartitionBy expressionPartitionBy,
        List<BaseClientProperty> propertyList) {
        FunctionCall functionCall = expressionPartitionBy.getFunctionCall();
        if (functionCall == null) {
            // 列表达式
            ColumnExpressionClientPartition partition = new ColumnExpressionClientPartition();
            List<String> columnNameList = expressionPartitionBy.getColumnDefinitions().stream()
                .map(columnDefinition -> columnDefinition.getColName().getValue())
                .collect(Collectors.toList());
            partition.setColumnNameList(columnNameList);

            ColumnExpressionPartitionProperty columnExpressionPartitionProperty = new ColumnExpressionPartitionProperty();
            columnExpressionPartitionProperty.setValue(partition);
            propertyList.add(columnExpressionPartitionProperty);
        } else {
            // 时间函数表达式
            TimeExpressionClientPartition timeExpressionClientPartition = new TimeExpressionClientPartition();
            timeExpressionClientPartition.setFuncName(functionCall.getFuncName().getFirst());
            if (expressionPartitionBy.getTimeUnitArg() != null) {
                timeExpressionClientPartition.setTimeUnit(StripUtils.strip(expressionPartitionBy.getTimeUnitArg().getOrigin()));
            }
            if (expressionPartitionBy.getIntervalLiteralArg() != null) {
                timeExpressionClientPartition.setInterval(expressionPartitionBy.getIntervalLiteralArg());
            }

            //property
            TimeExpressionPartitionProperty timeExpressionPartitionProperty = new TimeExpressionPartitionProperty();
            timeExpressionPartitionProperty.setValue(timeExpressionClientPartition);
            propertyList.add(timeExpressionPartitionProperty);
        }
    }

    /**
     * 将partition desc转为 baseClientProperty
     *
     * @param partitionDesc
     */
    private BaseClientProperty parseRangePartition(PartitionDesc partitionDesc) {
        if (partitionDesc instanceof SingleRangePartition) {
            SingleRangePartition singleRangePartition = (SingleRangePartition)partitionDesc;
            SingleRangeClientPartition rangePartitionValue = new SingleRangeClientPartition();
            rangePartitionValue.setName(singleRangePartition.getName().getValue());
            rangePartitionValue.setIfNotExists(singleRangePartition.isIfNotExists());
            List<Property> propertyList = singleRangePartition.getPropertyList();
            if (propertyList != null && !propertyList.isEmpty()) {
                LinkedHashMap<String, String> linkedHashMap = Maps.newLinkedHashMap();
                for (Property p : propertyList) {
                    linkedHashMap.put(p.getName(), p.getValue());
                }
                rangePartitionValue.setProperties(linkedHashMap);
            }
            BaseClientPartitionKey partitionKey = toClientPartitionKey(singleRangePartition.getPartitionKey());
            rangePartitionValue.setPartitionKey(partitionKey);

            //property
            SingleRangePartitionProperty singleRangePartitionProperty = new SingleRangePartitionProperty();
            singleRangePartitionProperty.setValue(rangePartitionValue);
            return singleRangePartitionProperty;

        } else if (partitionDesc instanceof MultiRangePartition) {
            MultiRangePartition multiRangePartition = (MultiRangePartition)partitionDesc;
            MultiRangeClientPartition multiRangePartitionValue = new MultiRangeClientPartition();
            multiRangePartitionValue.setStart(multiRangePartition.getStart().getValue());
            multiRangePartitionValue.setEnd(multiRangePartition.getEnd().getValue());
            if (multiRangePartition.getLongLiteral() != null) {
                LongLiteral longLiteral = multiRangePartition.getLongLiteral();
                multiRangePartitionValue.setInterval(longLiteral.getValue());
            }
            IntervalLiteral intervalLiteral = multiRangePartition.getIntervalLiteral();
            if (intervalLiteral != null) {
                DateTimeEnum fromDateTime = intervalLiteral.getFromDateTime();
                multiRangePartitionValue.setDateTimeEnum(fromDateTime);
                LongLiteral value = (LongLiteral)intervalLiteral.getValue();
                multiRangePartitionValue.setInterval(value.getValue());
            }
            //property
            MultiRangePartitionProperty multiRangePartitionProperty = new MultiRangePartitionProperty();
            multiRangePartitionProperty.setValue(multiRangePartitionValue);
            return multiRangePartitionProperty;
        }
        return null;
    }

    private BaseClientProperty parseListPartition(PartitionDesc partitionDesc) {
        if (partitionDesc instanceof SingleItemListPartition) {
            SingleItemListPartition singleListPartition = (SingleItemListPartition)partitionDesc;
            ListClientPartition listPartitionValue = new ListClientPartition();
            listPartitionValue.setName(singleListPartition.getName().getOrigin());
            List<List<PartitionClientValue>> partitionValue = singleListPartition.getListStringLiteral()
                .getStringLiteralList().stream()
                .map(stringLiteral -> {
                    PartitionClientValue partitionClientValue = PartitionClientValue.builder()
                        .value(StripUtils.strip(stringLiteral.getValue())).build();
                    return Lists.newArrayList(partitionClientValue);
                }).collect(Collectors.toList());
            ArrayClientPartitionKey arrayClientPartitionKey = ArrayClientPartitionKey.builder()
                .partitionValue(partitionValue).build();
            listPartitionValue.setPartitionKey(arrayClientPartitionKey);

            //property
            ListPartitionProperty listPartitionProperty = new ListPartitionProperty();
            listPartitionProperty.setValue(listPartitionValue);
            return listPartitionProperty;
        } else if (partitionDesc instanceof MultiItemListPartition) {
            MultiItemListPartition multiItemListPartition = (MultiItemListPartition)partitionDesc;
            ListClientPartition listPartitionValue = new ListClientPartition();
            listPartitionValue.setName(multiItemListPartition.getName().getOrigin());
            List<List<PartitionClientValue>> partitionValue = multiItemListPartition.getListStringLiterals().stream()
                .map(listStringLiteral -> listStringLiteral.getStringLiteralList().stream()
                    .map(stringLiteral -> PartitionClientValue.builder()
                        .value(StripUtils.strip(stringLiteral.getValue())).build())
                    .collect(Collectors.toList())).collect(Collectors.toList());
            ArrayClientPartitionKey arrayClientPartitionKey = ArrayClientPartitionKey.builder()
                .partitionValue(partitionValue).build();
            listPartitionValue.setPartitionKey(arrayClientPartitionKey);

            //property
            ListPartitionProperty listPartitionProperty = new ListPartitionProperty();
            listPartitionProperty.setValue(listPartitionValue);
            return listPartitionProperty;
        }
        return null;
    }

    private BaseClientPartitionKey toClientPartitionKey(PartitionKey partitionKey) {
        if (partitionKey instanceof ArrayPartitionKey) {
            ArrayPartitionKey arrayPartitionKey = (ArrayPartitionKey)partitionKey;
            List<ListPartitionValue> partitionValues1 = arrayPartitionKey.getPartitionValues();
            ArrayClientPartitionKey arrayClientPartitionKey = new ArrayClientPartitionKey();
            List<List<PartitionClientValue>> partitionValues = partitionValues1.stream().map(x -> {
                List<PartitionValue> partitionValueList = x.getPartitionValueList();
                return partitionValueList.stream().map(p -> {
                    PartitionClientValue partitionClientValue = new PartitionClientValue();
                    partitionClientValue.setMaxValue(p.isMaxValue());
                    if (p.getStringLiteral() != null) {
                        partitionClientValue.setValue(p.getStringLiteral().getValue());
                    }
                    return partitionClientValue;
                }).collect(Collectors.toList());
            }).collect(Collectors.toList());
            arrayClientPartitionKey.setPartitionValue(partitionValues);
            return arrayClientPartitionKey;
        } else if (partitionKey instanceof LessThanPartitionKey) {
            LessThanPartitionKey lessThanPartitionKey = (LessThanPartitionKey)partitionKey;
            LessThanClientPartitionKey lessThanClientPartitionKey = new LessThanClientPartitionKey();
            lessThanClientPartitionKey.setMaxValue(lessThanPartitionKey.isMaxValue());
            List<PartitionClientValue> list = lessThanPartitionKey.getPartitionValues().getPartitionValueList().stream().map(
                x -> {
                    PartitionClientValue partitionClientValue = new PartitionClientValue();
                    partitionClientValue.setMaxValue(x.isMaxValue());
                    if (x.getStringLiteral() != null) {
                        partitionClientValue.setValue(x.getStringLiteral().getValue());
                    }
                    return partitionClientValue;
                }
            ).collect(Collectors.toList());
            lessThanClientPartitionKey.setPartitionValueList(list);
            return lessThanClientPartitionKey;
        }
        return null;
    }

    private PartitionedBy toPartition(List<ColumnDefinition> columnDefinitionList, List<BaseClientProperty> properties) {
        if (properties == null || properties.isEmpty()) {
            return null;
        }

        // range partition
        List<BaseClientProperty> rangePartitionProperties = properties.stream().filter(property ->
                StringUtils.equalsIgnoreCase(property.getKey(), StarRocksProperty.TABLE_RANGE_PARTITION.getValue()))
            .collect(Collectors.toList());
        if (CollectionUtils.isNotEmpty(rangePartitionProperties)) {
            List<PartitionDesc> list = Lists.newArrayList();
            rangePartitionProperties.forEach(property -> toRangePartition(list, property));
            return new RangePartitionedBy(columnDefinitionList, list);
        }

        // list partition
        List<BaseClientProperty> listPartitionProperties = properties.stream().filter(property ->
                StringUtils.equalsIgnoreCase(property.getKey(), StarRocksProperty.TABLE_LIST_PARTITION.getValue()))
            .collect(Collectors.toList());
        if (CollectionUtils.isNotEmpty(listPartitionProperties)) {
            List<PartitionDesc> list = Lists.newArrayList();
            listPartitionProperties.forEach(property -> toListPartition(list, columnDefinitionList, property));
            return new ListPartitionedBy(columnDefinitionList, list);
        }

        // expression partition
        List<BaseClientProperty> expressionPartitionProperties = properties.stream().filter(property ->
                StringUtils.equalsIgnoreCase(property.getKey(), StarRocksProperty.TABLE_EXPRESSION_PARTITION.getValue()))
            .collect(Collectors.toList());
        if (CollectionUtils.isNotEmpty(expressionPartitionProperties)) {
            FunctionCall functionCall = buildFunctionCall(expressionPartitionProperties.get(0));
            return new ExpressionPartitionBy(columnDefinitionList, functionCall, null);
        }

        return null;
    }

    private void toRangePartition(List<PartitionDesc> list, BaseClientProperty baseClientProperty) {
        if (baseClientProperty instanceof SingleRangePartitionProperty) {
            SingleRangePartitionProperty value = (SingleRangePartitionProperty)baseClientProperty;
            SingleRangeClientPartition rangePartitionValue = value.getValue();
            BaseClientPartitionKey partitionKeyValue = rangePartitionValue.getPartitionKey();
            PartitionKey partitionKey = null;
            if (partitionKeyValue instanceof LessThanClientPartitionKey) {
                LessThanClientPartitionKey lessThanClientPartitionKey = (LessThanClientPartitionKey)partitionKeyValue;
                List<PartitionClientValue> partitionValueList = Optional.ofNullable(lessThanClientPartitionKey.getPartitionValueList()).orElse(
                    Lists.newArrayList());
                List<PartitionValue> collect = partitionValueList
                    .stream().map(x -> {
                        if (x.getValue() != null) {
                            PartitionValue partitionValue = new PartitionValue(x.isMaxValue(), new StringLiteral(x.getValue()));
                            return partitionValue;
                        } else {
                            PartitionValue partitionValue = new PartitionValue(x.isMaxValue(), null);
                            return partitionValue;
                        }
                    })
                    .collect(Collectors.toList());
                partitionKey = new LessThanPartitionKey(
                    lessThanClientPartitionKey.isMaxValue(),
                    new ListPartitionValue(collect)
                );
            } else if (partitionKeyValue instanceof ArrayClientPartitionKey) {
                ArrayClientPartitionKey arrayClientPartitionKey = (ArrayClientPartitionKey)partitionKeyValue;
                List<ListPartitionValue> collect = arrayClientPartitionKey.getPartitionValue().stream().map(
                    x -> {
                        List<PartitionValue> partitionValueList = Lists.newArrayList();
                        for (PartitionClientValue partitionClientValue : x) {
                            PartitionValue partitionValue = new PartitionValue(partitionClientValue.isMaxValue(),
                                new StringLiteral(partitionClientValue.getValue()));
                            partitionValueList.add(partitionValue);
                        }
                        return new ListPartitionValue(partitionValueList);
                    }
                ).collect(Collectors.toList());
                partitionKey = new ArrayPartitionKey(collect);
            }
            PartitionDesc rangePartition = new SingleRangePartition(
                new Identifier(rangePartitionValue.getName()),
                rangePartitionValue.isIfNotExists(),
                partitionKey,
                null
            );
            list.add(rangePartition);
        } else if (baseClientProperty instanceof MultiRangePartitionProperty) {
            MultiRangePartitionProperty multiRangePartitionProperty = (MultiRangePartitionProperty)baseClientProperty;
            MultiRangeClientPartition value = multiRangePartitionProperty.getValue();
            LongLiteral longLiteral = new LongLiteral(String.valueOf(value.getInterval()));
            PartitionDesc rangePartition = new MultiRangePartition(
                new StringLiteral(value.getStart()),
                new StringLiteral(value.getEnd()),
                value.getDateTimeEnum() != null ? new IntervalLiteral(longLiteral, value.getDateTimeEnum()) : null,
                value.getDateTimeEnum() == null ? longLiteral : null
            );
            list.add(rangePartition);
        }
    }

    private void toListPartition(List<PartitionDesc> list, List<ColumnDefinition> columnDefinitionList,
        BaseClientProperty baseClientProperty) {
        if (!(baseClientProperty instanceof ListPartitionProperty)) {
            return;
        }
        boolean multiPartition = columnDefinitionList.size() > 1;
        ListPartitionProperty listPartitionProperty = (ListPartitionProperty)baseClientProperty;
        ListClientPartition listClientPartition = listPartitionProperty.getValue();
        BaseClientPartitionKey partitionKeyValue = listClientPartition.getPartitionKey();
        if (!(partitionKeyValue instanceof ArrayClientPartitionKey)) {
            return;
        }
        ArrayClientPartitionKey arrayClientPartitionKey = (ArrayClientPartitionKey)partitionKeyValue;
        if (multiPartition) {
            List<ListStringLiteral> collect = arrayClientPartitionKey.getPartitionValue().stream().map(
                x -> {
                    List<StringLiteral> partitionValueList = Lists.newArrayList();
                    for (PartitionClientValue partitionClientValue : x) {
                        partitionValueList.add(new StringLiteral(partitionClientValue.getValue()));
                    }
                    return new ListStringLiteral(partitionValueList);
                }
            ).collect(Collectors.toList());
            MultiItemListPartition listPartition = new MultiItemListPartition(
                new Identifier(listClientPartition.getName()),
                false,
                collect,
                null
            );
            list.add(listPartition);
        } else {
            List<StringLiteral> collect = arrayClientPartitionKey.getPartitionValue().stream().map(
                x -> {
                    PartitionClientValue partitionClientValue = x.get(0);
                    return new StringLiteral(partitionClientValue.getValue());
                }
            ).collect(Collectors.toList());
            ListStringLiteral partitionKey = new ListStringLiteral(collect);
            SingleItemListPartition listPartition = new SingleItemListPartition(
                new Identifier(listClientPartition.getName()),
                false,
                partitionKey,
                null
            );
            list.add(listPartition);
        }
    }

    private FunctionCall buildFunctionCall(BaseClientProperty baseClientProperty) {
        if (!(baseClientProperty instanceof TimeExpressionPartitionProperty)) {
            return null;
        }
        TimeExpressionPartitionProperty timeExpressionPartitionProperty = (TimeExpressionPartitionProperty)baseClientProperty;
        TimeExpressionClientPartition timeExpressionClientPartition = timeExpressionPartitionProperty.getValue();
        List<BaseExpression> arguments = new ArrayList<>();
        if (StringUtils.isNotBlank(timeExpressionClientPartition.getTimeUnit())) {
            arguments.add(new StringLiteral(timeExpressionClientPartition.getTimeUnit()));
        }
        if (timeExpressionClientPartition.getInterval() != null) {
            arguments.add(timeExpressionClientPartition.getInterval());
        }
        return new FunctionCall(QualifiedName.of(timeExpressionClientPartition.getFuncName()),
            false, arguments);

    }

    protected BaseConstraint setPrimaryConstraintColumns(List<Column> columns) {
        if (CollectionUtils.isEmpty(columns)) {
            return null;
        }
        List<Column> primaryKeyColumns = columns.stream().filter(
            c -> BooleanUtils.isTrue(c.isPrimaryKey())
        ).collect(Collectors.toList());
        BaseConstraint baseConstraint = null;
        if (primaryKeyColumns.size() > 0) {
            List<Identifier> list = new ArrayList<>();
            for (Column column : primaryKeyColumns) {
                list.add(new Identifier(column.getName()));
                column.setPrimaryKey(false);
                column.setNullable(false);
            }
            baseConstraint = new PrimaryConstraint(IdentifierUtil.sysIdentifier(), list);
        }
        return baseConstraint;
    }

    protected BaseExpression toDefaultValueExpression(BaseDataType baseDataType, String defaultValue) {
        if (defaultValue == null) {
            return null;
        }
        IDataTypeName typeName = baseDataType.getTypeName();
        if (StringUtils.equalsIgnoreCase(defaultValue, UUID + SUFFIX)) {
            return new FunctionCall(QualifiedName.of(UUID), false, Lists.newArrayList());
        }
        if (StringUtils.equalsIgnoreCase(defaultValue, UUID_NUMERIC + SUFFIX)) {
            return new FunctionCall(QualifiedName.of(UUID_NUMERIC), false, Lists.newArrayList());
        }
        if (typeName instanceof ISimpleDataTypeName) {
            //starRocks 数字也是用字符标识
            ISimpleDataTypeName simpleDataTypeName = (ISimpleDataTypeName)typeName;
            if (simpleDataTypeName.getSimpleDataTypeName() == SimpleDataTypeName.NUMBER) {
                return new StringLiteral(defaultValue);
            }
        }
        return super.toDefaultValueExpression(baseDataType, defaultValue);
    }
}
