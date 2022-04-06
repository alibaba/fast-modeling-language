/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.mc.client.converter;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.core.exception.ParseException;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName.Dimension;
import com.aliyun.fastmodel.core.tree.datatype.NumericParameter;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.aliyun.fastmodel.transform.api.client.PropertyConverter;
import com.aliyun.fastmodel.transform.api.client.converter.BaseClientConverter;
import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.api.client.dto.table.Column;
import com.aliyun.fastmodel.transform.api.client.dto.table.Table;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.mc.client.property.LifeCycle;
import com.aliyun.fastmodel.transform.mc.context.MaxComputeContext;
import com.aliyun.fastmodel.transform.mc.parser.MaxComputeLanguageParser;
import com.aliyun.fastmodel.transform.mc.parser.tree.datatype.MaxComputeDataTypeName;
import com.aliyun.fastmodel.transform.mc.parser.tree.datatype.MaxComputeGenericDataType;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * maxcompute client converter
 *
 * @author panguanjing
 * @date 2022/8/5
 */
public class MaxComputeClientConverter extends BaseClientConverter<MaxComputeContext> {
    private final MaxComputePropertyConverter maxComputePropertyConverter;
    private final MaxComputeLanguageParser maxComputeLanguageParser;

    public MaxComputeClientConverter() {
        maxComputePropertyConverter = new MaxComputePropertyConverter();
        maxComputeLanguageParser = new MaxComputeLanguageParser();
    }

    @Override
    public PropertyConverter getPropertyConverter() {
        return maxComputePropertyConverter;
    }

    @Override
    public Long toLifeCycleSeconds(CreateTable createTable) {
        List<Property> properties = createTable.getProperties();
        Optional<Property> first = properties.stream().filter(p -> StringUtils.equalsIgnoreCase(LifeCycle.LIFECYCLE, p.getName())).findFirst();
        if (!first.isPresent()) {
            return null;
        }
        LifeCycle lifeCycle = new LifeCycle();
        lifeCycle.setValueString(first.get().getValue());
        return lifeCycle.toSeconds();
    }

    @Override
    protected List<Property> toProperty(Table table, List<BaseClientProperty> properties) {
        Long lifeCycleSeconds = table.getLifecycleSeconds();
        //if properties and life cycle is null
        if (CollectionUtils.isEmpty(properties) && lifeCycleSeconds == null) {
            return Lists.newArrayList();
        }
        Property property = null;
        if (lifeCycleSeconds != null) {
            property = new Property(LifeCycle.LIFECYCLE, String.valueOf(LifeCycle.toLifeCycle(lifeCycleSeconds)));
        }
        if (CollectionUtils.isEmpty(properties)) {
            return Lists.newArrayList(property);
        }
        List<Property> list = super.toProperty(table, properties);
        if (property != null) {
            return Lists.asList(property, list.toArray(new Property[0]));
        }
        return list;
    }

    @Override
    protected List<ColumnDefinition> toColumnDefinition(List<Column> columns) {
        //mc是columns分开的处理
        return columns.stream().filter(column -> !column.isPartitionKey()).map(this::toColumnDefinition).collect(Collectors.toList());
    }

    @Override
    protected BaseDataType getDataType(Column column) {
        String dataTypeName = column.getDataType();
        if (StringUtils.isBlank(dataTypeName)) {
            throw new IllegalArgumentException("dataType name can't be null:" + column.getName());
        }
        IDataTypeName byValue = MaxComputeDataTypeName.getByValue(dataTypeName);
        Dimension dimension = byValue.getDimension();
        if (dimension == null || dimension == Dimension.ZERO) {
            return DataTypeUtil.simpleType(byValue);
        }
        if (dimension == Dimension.ONE) {
            boolean isValidLength = column.getLength() != null && column.getLength() > 0;
            if (isValidLength) {
                NumericParameter numericParameter = new NumericParameter(String.valueOf(column.getLength()));
                return new MaxComputeGenericDataType(byValue, numericParameter);
            }
        }
        if (dimension == Dimension.TWO) {
            if (column.getPrecision() != null) {
                NumericParameter numericParameter = new NumericParameter(String.valueOf(column.getPrecision()));
                if (column.getScale() == null) {
                    return DataTypeUtil.simpleType(byValue, numericParameter);
                }
                NumericParameter scale = new NumericParameter(String.valueOf(column.getScale()));
                return new MaxComputeGenericDataType(byValue, numericParameter, scale);
            }
        }
        //complex data use parse dataType
        ReverseContext context = ReverseContext.builder().build();
        try {
            return maxComputeLanguageParser.parseDataType(dataTypeName, context);
        } catch (ParseException e) {
            throw new IllegalArgumentException("not support dataTypeName with:" + dataTypeName);
        }
    }
}
