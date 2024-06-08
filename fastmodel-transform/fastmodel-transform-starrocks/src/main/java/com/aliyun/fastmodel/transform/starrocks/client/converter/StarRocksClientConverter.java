package com.aliyun.fastmodel.transform.starrocks.client.converter;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName.Dimension;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.enums.DateTimeEnum;
import com.aliyun.fastmodel.core.tree.expr.literal.IntervalLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.LongLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.PrimaryConstraint;
import com.aliyun.fastmodel.core.tree.util.IdentifierUtil;
import com.aliyun.fastmodel.transform.api.client.PropertyConverter;
import com.aliyun.fastmodel.transform.api.client.converter.BaseClientConverter;
import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.api.client.dto.property.StringProperty;
import com.aliyun.fastmodel.transform.api.client.dto.table.Column;
import com.aliyun.fastmodel.transform.api.client.dto.table.Table;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.starrocks.client.property.table.MultiRangePartitionProperty;
import com.aliyun.fastmodel.transform.starrocks.client.property.table.SingleRangePartitionProperty;
import com.aliyun.fastmodel.transform.starrocks.client.property.table.TablePartitionRaw;
import com.aliyun.fastmodel.transform.starrocks.client.property.table.partition.ArrayClientPartitionKey;
import com.aliyun.fastmodel.transform.starrocks.client.property.table.partition.BaseClientPartitionKey;
import com.aliyun.fastmodel.transform.starrocks.client.property.table.partition.LessThanClientPartitionKey;
import com.aliyun.fastmodel.transform.starrocks.client.property.table.partition.MultiRangeClientPartition;
import com.aliyun.fastmodel.transform.starrocks.client.property.table.partition.PartitionClientValue;
import com.aliyun.fastmodel.transform.starrocks.client.property.table.partition.SingleRangeClientPartition;
import com.aliyun.fastmodel.transform.starrocks.context.StarRocksContext;
import com.aliyun.fastmodel.transform.starrocks.format.StarRocksOutVisitor;
import com.aliyun.fastmodel.transform.starrocks.format.StarRocksProperty;
import com.aliyun.fastmodel.transform.starrocks.parser.StarRocksLanguageParser;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.datatype.StarRocksDataTypeName;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.ArrayPartitionKey;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.LessThanPartitionKey;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.ListPartitionValue;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.ListPartitionedBy;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.MultiRangePartition;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.PartitionDesc;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.PartitionKey;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.PartitionValue;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.RangePartitionedBy;
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

    private final StarRocksLanguageParser starRocksLanguageParser = new StarRocksLanguageParser();

    private final StarRocksPropertyConverter starRocksPropertyConverter = new StarRocksPropertyConverter();

    @Override
    public PropertyConverter getPropertyConverter() {
        return starRocksPropertyConverter;
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
        List<PartitionDesc> rangePartitions = toRangePartition(properties);
        return new RangePartitionedBy(list, rangePartitions);
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
            RangePartitionedBy rangePartitionedBy = (RangePartitionedBy)partitionedBy;
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
        //list partition by 只支持raw的方式
        if (partitionedBy instanceof ListPartitionedBy) {
            StarRocksOutVisitor starRocksOutVisitor = new StarRocksOutVisitor(StarRocksContext.builder().build());
            ListPartitionedBy listPartitionedBy = (ListPartitionedBy)partitionedBy;
            starRocksOutVisitor.visitListPartitionedBy(listPartitionedBy, 0);
            StringProperty baseClientProperty = new StringProperty();
            baseClientProperty.setKey(StarRocksProperty.TABLE_PARTITION_RAW.getValue());
            baseClientProperty.setValueString(starRocksOutVisitor.getBuilder().toString());
            propertyList.add(baseClientProperty);
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

    private List<PartitionDesc> toRangePartition(List<BaseClientProperty> properties) {
        if (properties == null || properties.isEmpty()) {
            return Lists.newArrayList();
        }
        List<BaseClientProperty> first = properties.stream().filter(
            p -> StringUtils.equalsIgnoreCase(p.getKey(), StarRocksProperty.TABLE_RANGE_PARTITION.getValue())).collect(Collectors.toList());
        List<PartitionDesc> list = Lists.newArrayList();
        for (BaseClientProperty baseClientProperty : first) {
            toPartitionDesc(list, baseClientProperty);
        }
        return list;
    }

    private void toPartitionDesc(List<PartitionDesc> list, BaseClientProperty baseClientProperty) {
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
}
