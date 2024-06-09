/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.hologres.client.converter;

import java.util.List;
import java.util.Optional;

import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName.Dimension;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.literal.TimestampLiteral;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.transform.api.client.PropertyConverter;
import com.aliyun.fastmodel.transform.api.client.converter.BaseClientConverter;
import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.api.client.dto.table.Column;
import com.aliyun.fastmodel.transform.api.client.dto.table.Table;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.hologres.client.property.HoloPropertyKey;
import com.aliyun.fastmodel.transform.hologres.client.property.TimeToLiveSeconds;
import com.aliyun.fastmodel.transform.hologres.context.HologresTransformContext;
import com.aliyun.fastmodel.transform.hologres.parser.HologresParser;
import com.aliyun.fastmodel.transform.hologres.parser.tree.datatype.HologresDataTypeName;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * 将ddl 转为 fml模型对象
 *
 * @author panguanjing
 * @date 2022/6/7
 */
public class HologresClientConverter extends BaseClientConverter<HologresTransformContext> {

    private HologresParser hologresParser = null;

    public HologresClientConverter() {
        hologresParser = new HologresParser();
    }

    @Override
    public BaseDataType getDataType(Column column) {
        String dataTypeName = column.getDataType();
        if (StringUtils.isBlank(dataTypeName)) {
            throw new IllegalArgumentException("dataType name can't be null:" + column.getName());
        }
        IDataTypeName byValue = HologresDataTypeName.getByValue(dataTypeName);
        Dimension dimension = byValue.getDimension();
        ReverseContext context = ReverseContext.builder().build();
        if (dimension == null || dimension == Dimension.ZERO) {
            return hologresParser.parseDataType(dataTypeName, context);
        }
        if (dimension == Dimension.ONE) {
            boolean isValidLength = column.getLength() != null && column.getLength() > 0;
            if (isValidLength) {
                String dt = String.format(ONE_DIMENSION, dataTypeName, column.getLength());
                return hologresParser.parseDataType(dt, context);
            }
        }
        if (dimension == Dimension.TWO) {
            if (column.getPrecision() != null) {
                if (column.getScale() == null) {
                    return hologresParser.parseDataType(String.format(ONE_DIMENSION, dataTypeName, column.getPrecision()), context);
                }
                return hologresParser.parseDataType(String.format(TWO_DIMENSION, dataTypeName, column.getPrecision(), column.getScale()), context);
            }
        }
        return hologresParser.parseDataType(dataTypeName, context);
    }

    @Override
    public List<Property> toProperty(Table table, List<BaseClientProperty> properties) {
        List<Property> list = super.toProperty(table, properties);

        Long lifeCycleSeconds = table.getLifecycleSeconds();
        if (lifeCycleSeconds != null) {
            Property property = new Property(TimeToLiveSeconds.TIME_TO_LIVE_IN_SECONDS, String.valueOf(lifeCycleSeconds));
            list.add(0, property);
        }

        if (table.isExternal()) {
            Property property = new Property(HoloPropertyKey.FOREIGN.getValue(), "true");
            list.add(property);
        }
        return list;
    }

    @Override
    public PropertyConverter getPropertyConverter() {
        return HologresPropertyConverter.getInstance();
    }

    @Override
    public Long toLifeCycleSeconds(CreateTable createTable) {
        if (createTable.isPropertyEmpty()) {
            return null;
        }
        Property property = createTable.getProperties().stream().filter(
            p -> StringUtils.equalsIgnoreCase(p.getName(), TimeToLiveSeconds.TIME_TO_LIVE_IN_SECONDS)).findFirst().orElse(null);
        if (property == null) {
            return null;
        }
        TimeToLiveSeconds timeToLiveSeconds = new TimeToLiveSeconds();
        timeToLiveSeconds.setValueString(property.getValue());
        return timeToLiveSeconds.getValue();
    }

    @Override
    public Boolean isExternal(CreateTable createTable) {
        List<Property> properties = createTable.getProperties();
        if (CollectionUtils.isEmpty(properties)) {
            return false;
        }
        Optional<Property> first = properties.stream().filter(
            p -> StringUtils.equalsIgnoreCase(HoloPropertyKey.FOREIGN.getValue(), p.getName())).findFirst();
        return first.filter(property -> BooleanUtils.toBoolean(property.getValue())).isPresent();
    }

    @Override
    protected List<BaseClientProperty> toBaseClientProperty(CreateTable createTable) {
        List<BaseClientProperty> propertyList = super.toBaseClientProperty(createTable);

        // remove foreign key
        propertyList.removeIf(p -> StringUtils.equalsIgnoreCase(HoloPropertyKey.FOREIGN.getValue(), p.getKey()));
        // remove lifecycle
        propertyList.removeIf(p -> StringUtils.equalsIgnoreCase(HoloPropertyKey.TIME_TO_LIVE_IN_SECONDS.getValue(), p.getKey()));

        return propertyList;
    }

    @Override
    protected BaseExpression toDefaultValueExpression(BaseDataType baseDataType, String defaultValue) {
        if (defaultValue == null) {
            return null;
        }
        IDataTypeName typeName = baseDataType.getTypeName();
        String type = typeName.getValue();
        String value = defaultValue;
        if (StringUtils.equalsIgnoreCase(HologresDataTypeName.TIMESTAMPTZ.getValue(), type)) {
            return new TimestampLiteral(value);
        }
        return super.toDefaultValueExpression(baseDataType, defaultValue);
    }

}
