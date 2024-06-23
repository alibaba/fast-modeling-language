/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.hive.client.converter;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.core.exception.ParseException;
import com.aliyun.fastmodel.core.parser.LanguageParser;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
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
import com.aliyun.fastmodel.transform.hive.context.HiveTransformContext;
import com.aliyun.fastmodel.transform.hive.format.HivePropertyKey;
import com.aliyun.fastmodel.transform.hive.parser.HiveLanguageParser;
import com.aliyun.fastmodel.transform.hive.parser.tree.datatype.HiveDataTypeName;
import com.aliyun.fastmodel.transform.hive.parser.tree.datatype.HiveGenericDataType;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * maxcompute client converter
 *
 * @author panguanjing
 * @date 2022/8/5
 */
public class HiveClientConverter extends BaseClientConverter<HiveTransformContext> {
    private final HivePropertyConverter hivePropertyConverter;
    private final HiveLanguageParser hiveLanguageParser;

    public HiveClientConverter() {
        hivePropertyConverter = new HivePropertyConverter();
        hiveLanguageParser = new HiveLanguageParser();
    }

    @Override
    public Table convertToTable(Node table, HiveTransformContext context) {
        Table table1 = super.convertToTable(table, context);
        CreateTable createTable = (CreateTable)table;
        List<Property> properties = createTable.getProperties();
        if (properties.isEmpty()) {
            return table1;
        }
        Optional<Property> first = properties.stream().filter(
            p -> StringUtils.equalsIgnoreCase(p.getName(), HivePropertyKey.EXTERNAL_TABLE.getValue())).findFirst();
        if (first.isPresent() && Objects.equals(first.get().getValue(), BooleanUtils.toStringTrueFalse(Boolean.TRUE))) {
            table1.setExternal(true);
        }
        return table1;
    }

    @Override
    public PropertyConverter getPropertyConverter() {
        return hivePropertyConverter;
    }

    @Override
    protected List<ColumnDefinition> toColumnDefinition(Table table, List<Column> columns) {
        //mc是columns分开的处理
        return columns.stream().filter(column -> !column.isPartitionKey()).map(
            c -> {
                return toColumnDefinition(table, c);
            }
        ).collect(Collectors.toList());
    }

    @Override
    protected List<Property> toProperty(Table table, List<BaseClientProperty> properties) {
        boolean external = table.isExternal();
        List<Property> propertyList = super.toProperty(table, properties);
        if (external) {
            Property property = new Property(HivePropertyKey.EXTERNAL_TABLE.getValue(), BooleanUtils.toStringTrueFalse(true));
            propertyList.add(property);
        }
        return propertyList;
    }

    @Override
    public LanguageParser getLanguageParser() {
        return hiveLanguageParser;
    }

    @Override
    public BaseDataType getDataType(Column column) {
        String dataTypeName = column.getDataType();
        if (StringUtils.isBlank(dataTypeName)) {
            throw new IllegalArgumentException("dataType name can't be null:" + column.getName());
        }
        IDataTypeName byValue = HiveDataTypeName.getByValue(dataTypeName);
        Dimension dimension = byValue.getDimension();
        if (dimension == null || dimension == Dimension.ZERO) {
            return DataTypeUtil.simpleType(byValue);
        }
        if (dimension == Dimension.ONE) {
            boolean isValidLength = column.getLength() != null && column.getLength() > 0;
            if (isValidLength) {
                NumericParameter numericParameter = new NumericParameter(String.valueOf(column.getLength()));
                return new HiveGenericDataType(byValue, numericParameter);
            }
        }
        if (dimension == Dimension.TWO) {
            if (column.getPrecision() != null) {
                NumericParameter numericParameter = new NumericParameter(String.valueOf(column.getPrecision()));
                if (column.getScale() == null) {
                    return DataTypeUtil.simpleType(byValue, numericParameter);
                }
                NumericParameter scale = new NumericParameter(String.valueOf(column.getScale()));
                return new HiveGenericDataType(byValue, numericParameter, scale);
            }
        }
        //complex data use parse dataType
        ReverseContext context = ReverseContext.builder().build();
        try {
            return hiveLanguageParser.parseDataType(dataTypeName, context);
        } catch (ParseException e) {
            throw new IllegalArgumentException("not support dataTypeName with:" + dataTypeName);
        }
    }

    @Override
    protected String toSchema(CreateTable createTable, HiveTransformContext transformContext) {
        return null;
    }

    @Override
    protected String toDatabase(CreateTable createTable, String database) {
        QualifiedName qualifiedName = createTable.getQualifiedName();
        boolean isSecondSchema = qualifiedName.isJoinPath() && qualifiedName.getOriginalParts().size() == SECOND_INDEX;
        if (isSecondSchema) {
            return qualifiedName.getFirst();
        }
        return database;
    }
}
