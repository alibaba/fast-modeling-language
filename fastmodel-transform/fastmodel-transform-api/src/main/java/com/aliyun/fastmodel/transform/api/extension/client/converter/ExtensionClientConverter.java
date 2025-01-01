package com.aliyun.fastmodel.transform.api.extension.client.converter;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName.Dimension;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.atom.FunctionCall;
import com.aliyun.fastmodel.core.tree.expr.atom.TableOrColumn;
import com.aliyun.fastmodel.core.tree.expr.enums.DateTimeEnum;
import com.aliyun.fastmodel.core.tree.expr.literal.IntervalLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.ListStringLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.LongLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.util.IdentifierUtil;
import com.aliyun.fastmodel.core.tree.util.StringLiteralUtil;
import com.aliyun.fastmodel.transform.api.client.converter.BaseClientConverter;
import com.aliyun.fastmodel.transform.api.client.dto.constraint.Constraint;
import com.aliyun.fastmodel.transform.api.client.dto.constraint.ConstraintType;
import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.api.client.dto.property.StringProperty;
import com.aliyun.fastmodel.transform.api.client.dto.table.Column;
import com.aliyun.fastmodel.transform.api.client.dto.table.Table;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.aliyun.fastmodel.transform.api.datatype.simple.ISimpleDataTypeName;
import com.aliyun.fastmodel.transform.api.datatype.simple.SimpleDataTypeName;
import com.aliyun.fastmodel.transform.api.extension.client.constraint.DistributeClientConstraint;
import com.aliyun.fastmodel.transform.api.extension.client.constraint.ExtensionClientConstraintType;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.ListPartitionProperty;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.MultiRangePartitionProperty;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.SingleRangePartitionProperty;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.TablePartitionRaw;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.TimeExpressionPartitionProperty;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.partition.ArrayClientPartitionKey;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.partition.BaseClientPartitionKey;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.partition.LessThanClientPartitionKey;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.partition.ListClientPartition;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.partition.MultiRangeClientPartition;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.partition.PartitionClientValue;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.partition.SingleRangeClientPartition;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.partition.TimeExpressionClientPartition;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.AggregateKeyConstraint;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.DuplicateKeyConstraint;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.desc.DistributeConstraint;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.desc.OrderByConstraint;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.ExpressionPartitionBy;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.ListPartitionedBy;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.RangePartitionedBy;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.desc.MultiItemListPartition;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.desc.MultiRangePartition;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.desc.PartitionDesc;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.desc.SingleItemListPartition;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.desc.SingleRangePartition;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.keyvalue.ArrayPartitionKey;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.keyvalue.LessThanPartitionKey;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.keyvalue.ListPartitionValue;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.keyvalue.PartitionKey;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.keyvalue.PartitionValue;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

import static com.aliyun.fastmodel.transform.api.extension.client.property.ExtensionPropertyKey.TABLE_EXPRESSION_PARTITION;
import static com.aliyun.fastmodel.transform.api.extension.client.property.ExtensionPropertyKey.TABLE_LIST_PARTITION;
import static com.aliyun.fastmodel.transform.api.extension.client.property.ExtensionPropertyKey.TABLE_PARTITION_RAW;
import static com.aliyun.fastmodel.transform.api.extension.client.property.ExtensionPropertyKey.TABLE_RANGE_PARTITION;

/**
 * ExtensionClientConverter
 *
 * @author panguanjing
 * @date 2024/1/21
 */
public abstract class ExtensionClientConverter<T extends TransformContext> extends BaseClientConverter<T> {

    public static final String UUID = "uuid";
    public static final String UUID_NUMERIC = "uuid_numeric";
    public static final String SUFFIX = "()";

    /**
     * 根据dataTypeName返回类型定义
     *
     * @param dataTypeName
     * @return
     */
    public abstract IDataTypeName getDataTypeName(String dataTypeName);

    /**
     * 根据节点返回文本信息
     *
     * @param node
     * @return
     */
    public abstract String getRaw(Node node);

    @Override
    public List<BaseClientProperty> toBaseClientProperty(CreateTable createTable) {
        List<BaseClientProperty> propertyList = Lists.newArrayList();
        if (!createTable.isPropertyEmpty()) {
            List<Property> properties = createTable.getProperties();
            for (Property property : properties) {
                BaseClientProperty baseClientProperty = getPropertyConverter().create(property.getName(), property.getValue());
                propertyList.add(baseClientProperty);
            }
        }
        PartitionedBy partitionedBy = createTable.getPartitionedBy();
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

    private void processRangePartition(RangePartitionedBy rangePartitionedBy, List<BaseClientProperty> propertyList) {
        List<PartitionDesc> rangePartitions = rangePartitionedBy.getRangePartitions();
        List<BaseClientProperty> list = Lists.newArrayList();
        //结构化的返回
        if (rangePartitions != null) {
            for (PartitionDesc partitionDesc : rangePartitions) {
                BaseClientProperty baseClientProperty = parseRangePartition(partitionDesc);
                if (baseClientProperty != null) {
                    list.add(baseClientProperty);
                }
            }
        }
        //如果解析不了，那么使用visitor的方式进行解析
        if (list.isEmpty()) {
            TablePartitionRaw baseClientProperty = new TablePartitionRaw();
            baseClientProperty.setKey(TABLE_PARTITION_RAW.getValue());
            String raw = getRaw(rangePartitionedBy);
            baseClientProperty.setValueString(raw);
            propertyList.add(baseClientProperty);
        } else {
            propertyList.addAll(list);
        }
    }

    private void processListPartition(ListPartitionedBy listPartitionedBy, List<BaseClientProperty> propertyList) {
        List<PartitionDesc> listPartitions = listPartitionedBy.getListPartitions();
        //结构化的返回
        List<BaseClientProperty> list = Lists.newArrayList();
        if (listPartitions != null) {
            for (PartitionDesc partitionDesc : listPartitions) {
                BaseClientProperty baseClientProperty = parseListPartition(partitionDesc);
                if (baseClientProperty != null) {
                    list.add(baseClientProperty);
                }
            }
        }

        //如果解析不了，那么使用visitor的方式进行解析
        if (list.isEmpty()) {
            String raw = getRaw(listPartitionedBy);
            StringProperty baseClientProperty = new StringProperty();
            baseClientProperty.setKey(TABLE_PARTITION_RAW.getValue());
            baseClientProperty.setValueString(raw);
            propertyList.add(baseClientProperty);
        } else {
            propertyList.addAll(list);
        }
    }

    private void processExpressionPartition(ExpressionPartitionBy expressionPartitionBy,
        List<BaseClientProperty> propertyList) {
        FunctionCall functionCall = expressionPartitionBy.getFunctionCall();
        if (functionCall == null) {
            return;
            // 时间函数表达式
        }
        //默认是先是格式，再是列
        TimeExpressionPartitionProperty timeExpressionPartitionProperty = getTimeExpressionPartitionProperty(expressionPartitionBy);
        propertyList.add(timeExpressionPartitionProperty);
    }

    protected TimeExpressionPartitionProperty getTimeExpressionPartitionProperty(ExpressionPartitionBy expressionPartitionBy) {
        TimeExpressionClientPartition timeExpressionClientPartition = new TimeExpressionClientPartition();
        FunctionCall functionCall = expressionPartitionBy.getFunctionCall();
        timeExpressionClientPartition.setFuncName(functionCall.getFuncName().getFirst());
        if (expressionPartitionBy.getTimeUnitArg(0) != null) {
            timeExpressionClientPartition.setTimeUnit(expressionPartitionBy.getTimeUnitArg(0).getValue());
        }
        if (expressionPartitionBy.getIntervalLiteralArg(1) != null) {
            timeExpressionClientPartition.setInterval(expressionPartitionBy.getIntervalLiteralArg(1));
        }
        TableOrColumn tableOrColumn = expressionPartitionBy.getColumn(0);
        if (tableOrColumn != null) {
            timeExpressionClientPartition.setColumn(tableOrColumn.getQualifiedName().toString());
        } else {
            tableOrColumn = expressionPartitionBy.getColumn(1);
            if (tableOrColumn != null) {
                timeExpressionClientPartition.setColumn(tableOrColumn.getQualifiedName().toString());
            }
        }
        //property
        TimeExpressionPartitionProperty timeExpressionPartitionProperty = new TimeExpressionPartitionProperty();
        timeExpressionPartitionProperty.setValue(timeExpressionClientPartition);
        return timeExpressionPartitionProperty;
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
            ListPartitionValue start = multiRangePartition.getStart();
            String startRaw = getRaw(start);
            multiRangePartitionValue.setStart(StringLiteralUtil.strip(startRaw));
            ListPartitionValue end = multiRangePartition.getEnd();
            String endRaw = getRaw(end);
            multiRangePartitionValue.setEnd(StringLiteralUtil.strip(endRaw));
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
                        .value(StringLiteralUtil.strip(stringLiteral.getValue())).build();
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
                        .value(StringLiteralUtil.strip(stringLiteral.getValue())).build())
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

    public BaseClientPartitionKey toClientPartitionKey(PartitionKey partitionKey) {
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
                        StringLiteral stringLiteral = (StringLiteral)p.getStringLiteral();
                        partitionClientValue.setValue(stringLiteral.getValue());
                    }
                    return partitionClientValue;
                }).collect(Collectors.toList());
            }).collect(Collectors.toList());
            arrayClientPartitionKey.setPartitionValue(partitionValues);
            return arrayClientPartitionKey;
        } else if (partitionKey instanceof LessThanPartitionKey) {
            LessThanPartitionKey lessThanPartitionKey = (LessThanPartitionKey)partitionKey;
            LessThanClientPartitionKey lessThanClientPartitionKey = new LessThanClientPartitionKey();
            if (BooleanUtils.isTrue(lessThanPartitionKey.isMaxValue())) {
                lessThanClientPartitionKey.setMaxValue(lessThanPartitionKey.isMaxValue());
            } else {
                List<PartitionClientValue> list = getPartitionClientValues(lessThanPartitionKey);
                lessThanClientPartitionKey.setPartitionValueList(list);
            }
            return lessThanClientPartitionKey;
        }
        return null;
    }

    private List<PartitionClientValue> getPartitionClientValues(LessThanPartitionKey lessThanPartitionKey) {
        ListPartitionValue partitionValues = lessThanPartitionKey.getPartitionValues();
        List<PartitionClientValue> list = partitionValues.getPartitionValueList().stream().map(
            x -> {
                PartitionClientValue partitionClientValue = new PartitionClientValue();
                partitionClientValue.setMaxValue(x.isMaxValue());
                if (x.getStringLiteral() != null) {
                    String raw = getRaw(x.getStringLiteral());
                    partitionClientValue.setValue(StringLiteralUtil.strip(raw));
                }
                return partitionClientValue;
            }
        ).collect(Collectors.toList());
        return list;
    }

    @Override
    protected PartitionedBy toPartitionedBy(Table table, List<Column> columns) {
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
        if (properties == null || properties.isEmpty()) {
            return super.toPartitionedBy(table, columns);
        }
        return toPartition(list, properties);
    }

    protected PartitionedBy toPartition(List<ColumnDefinition> columnDefinitionList, List<BaseClientProperty> properties) {
        // range partition
        List<BaseClientProperty> rangePartitionProperties = properties.stream().filter(property ->
                StringUtils.equalsIgnoreCase(property.getKey(), TABLE_RANGE_PARTITION.getValue()))
            .collect(Collectors.toList());
        if (CollectionUtils.isNotEmpty(rangePartitionProperties)) {
            List<PartitionDesc> list = Lists.newArrayList();
            rangePartitionProperties.forEach(property -> toRangePartition(list, property));
            return new RangePartitionedBy(columnDefinitionList, list);
        }

        // list partition
        List<BaseClientProperty> listPartitionProperties = properties.stream().filter(property ->
                StringUtils.equalsIgnoreCase(property.getKey(), TABLE_LIST_PARTITION.getValue()))
            .collect(Collectors.toList());
        if (CollectionUtils.isNotEmpty(listPartitionProperties)) {
            List<PartitionDesc> list = Lists.newArrayList();
            listPartitionProperties.forEach(property -> toListPartition(list, columnDefinitionList, property));
            return new ListPartitionedBy(columnDefinitionList, list);
        }

        // expression partition
        List<BaseClientProperty> expressionPartitionProperties = properties.stream().filter(property ->
                StringUtils.equalsIgnoreCase(property.getKey(), TABLE_EXPRESSION_PARTITION.getValue()))
            .collect(Collectors.toList());
        if (CollectionUtils.isNotEmpty(expressionPartitionProperties)) {
            FunctionCall functionCall = buildFunctionCall(expressionPartitionProperties.get(0));
            return new ExpressionPartitionBy(columnDefinitionList, functionCall, null);
        }

        return null;
    }

    private void toRangePartition(List<PartitionDesc> list, BaseClientProperty baseClientProperty) {
        if (baseClientProperty instanceof SingleRangePartitionProperty) {
            SingleRangePartitionProperty baseClientProperty1 = (SingleRangePartitionProperty)baseClientProperty;
            PartitionDesc rangePartition = getPartitionDesc(baseClientProperty1.getValue());
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

    public PartitionDesc getPartitionDesc(SingleRangeClientPartition rangePartitionValue) {
        BaseClientPartitionKey partitionKeyValue = rangePartitionValue.getPartitionKey();
        PartitionKey partitionKey = null;
        if (partitionKeyValue instanceof LessThanClientPartitionKey) {
            LessThanClientPartitionKey lessThanClientPartitionKey = (LessThanClientPartitionKey)partitionKeyValue;
            List<PartitionClientValue> partitionValueList = Optional.ofNullable(lessThanClientPartitionKey.getPartitionValueList()).orElse(
                Lists.newArrayList());
            List<PartitionValue> collect = partitionValueList
                .stream().map(x -> {
                    return getPartitionValue(x);
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
                        PartitionValue partitionValue = getPartitionValue(partitionClientValue);
                        partitionValueList.add(partitionValue);
                    }
                    return new ListPartitionValue(partitionValueList);
                }
            ).collect(Collectors.toList());
            partitionKey = new ArrayPartitionKey(collect);
        }
        return new SingleRangePartition(
            new Identifier(rangePartitionValue.getName()),
            rangePartitionValue.isIfNotExists(),
            partitionKey,
            null
        );
    }

    protected PartitionValue getPartitionValue(PartitionClientValue partitionClientValue) {
        StringLiteral stringLiteral = null;
        if (partitionClientValue.getValue() != null) {
            stringLiteral = new StringLiteral(partitionClientValue.getValue());
        }
        return new PartitionValue(partitionClientValue.isMaxValue(), stringLiteral);
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
        return getFunctionCall((TimeExpressionPartitionProperty)baseClientProperty);

    }

    protected FunctionCall getFunctionCall(TimeExpressionPartitionProperty baseClientProperty) {
        TimeExpressionClientPartition timeExpressionClientPartition = baseClientProperty.getValue();
        List<BaseExpression> arguments = new ArrayList<>();
        if (StringUtils.isNotBlank(timeExpressionClientPartition.getTimeUnit())) {
            arguments.add(new StringLiteral(timeExpressionClientPartition.getTimeUnit()));
            if (StringUtils.isNotBlank(timeExpressionClientPartition.getColumn())) {
                arguments.add(new TableOrColumn(QualifiedName.of(timeExpressionClientPartition.getColumn())));
            }
        }
        if (timeExpressionClientPartition.getInterval() != null) {
            if (StringUtils.isNotBlank(timeExpressionClientPartition.getColumn())) {
                arguments.add(new TableOrColumn(QualifiedName.of(timeExpressionClientPartition.getColumn())));
            }
            arguments.add(timeExpressionClientPartition.getInterval());
        }
        return new FunctionCall(QualifiedName.of(timeExpressionClientPartition.getFuncName()),
            false, arguments);
    }

    @Override
    public BaseDataType getDataType(Column column) {
        ReverseContext context = ReverseContext.builder().build();
        String dataTypeName = column.getDataType();
        if (StringUtils.isBlank(dataTypeName)) {
            throw new IllegalArgumentException("dataType name can't be null:" + column.getName());
        }
        IDataTypeName byValue = getDataTypeName(dataTypeName);
        if (byValue == null) {
            return getLanguageParser().parseDataType(dataTypeName, context);
        }
        Dimension dimension = byValue.getDimension();
        if (dimension == null || dimension == Dimension.ZERO) {
            return getLanguageParser().parseDataType(dataTypeName, context);
        }
        if (dimension == Dimension.ONE) {
            boolean isValidLength = column.getLength() != null && column.getLength() > 0;
            if (isValidLength) {
                String dt = String.format(ONE_DIMENSION, dataTypeName, column.getLength());
                return getLanguageParser().parseDataType(dt, context);
            }
        }
        if (dimension == Dimension.TWO) {
            if (column.getPrecision() != null) {
                if (column.getScale() == null) {
                    return getLanguageParser().parseDataType(String.format(ONE_DIMENSION, dataTypeName, column.getPrecision()), context);
                }
                return getLanguageParser().parseDataType(String.format(TWO_DIMENSION, dataTypeName, column.getPrecision(), column.getScale()),
                    context);
            }
        }
        return getLanguageParser().parseDataType(dataTypeName, context);
    }

    @Override
    protected List<BaseConstraint> toConstraint(List<Column> columns, List<Constraint> constraints) {
        List<BaseConstraint> baseConstraints = super.toConstraint(columns, constraints);
        if (constraints == null) {
            return baseConstraints;
        }
        for (Constraint constraint : constraints) {
            ConstraintType type = constraint.getType();
            if (type == ExtensionClientConstraintType.DISTRIBUTE) {
                DistributeConstraint distributeConstraint = toDistributeConstraint(constraint);
                if (distributeConstraint == null) {
                    continue;
                }
                baseConstraints.add(distributeConstraint);
            }
            if (type == ExtensionClientConstraintType.ORDER_BY) {
                OrderByConstraint orderByConstraint = toOrderByConstraint(constraint);
                baseConstraints.add(orderByConstraint);
            }

            if (type == ExtensionClientConstraintType.DUPLICATE_KEY) {
                DuplicateKeyConstraint duplicateKeyConstraint = toDuplicateKeyConstraint(constraint);
                baseConstraints.add(duplicateKeyConstraint);
            }

            if (type == ExtensionClientConstraintType.AGGREGATE_KEY) {
                AggregateKeyConstraint aggregateKeyConstraint = toAggregateKeyConstraint(constraint);
                baseConstraints.add(aggregateKeyConstraint);
            }
        }
        return baseConstraints;
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
        if (constraint instanceof DistributeClientConstraint) {
            DistributeClientConstraint clientConstraint = (DistributeClientConstraint)constraint;
            List<Identifier> list = null;
            if (CollectionUtils.isNotEmpty(constraint.getColumns())) {
                list = constraint.getColumns().stream().map(Identifier::new).collect(Collectors.toList());
                return new DistributeConstraint(list, clientConstraint.getBucket());
            } else {
                if (BooleanUtils.isTrue(clientConstraint.getRandom())) {
                    return new DistributeConstraint(clientConstraint.getRandom(), clientConstraint.getBucket());
                }
            }
        }
        List<Identifier> list = null;
        if (CollectionUtils.isNotEmpty(constraint.getColumns())) {
            list = constraint.getColumns().stream().map(Identifier::new).collect(Collectors.toList());
            return new DistributeConstraint(list, null);
        }
        return null;
    }

}
