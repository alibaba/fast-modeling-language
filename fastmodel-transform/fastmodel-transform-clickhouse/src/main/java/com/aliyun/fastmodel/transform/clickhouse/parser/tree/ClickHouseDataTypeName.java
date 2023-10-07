/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.clickhouse.parser.tree;

import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import org.apache.commons.lang3.StringUtils;

/**
 * click house data Type name
 *
 * @author panguanjing
 * @date 2022/7/10
 */
public enum ClickHouseDataTypeName implements IDataTypeName {

    /**
     * number
     */
    INT8("Int8"),

    /**
     * 16
     */
    INT16("Int16"),

    /**
     * 32
     */
    INT32("Int32"),

    /**
     * 64
     */
    INT64("Int64"),
    /**
     * uint8
     */
    UInt8("UInt8"),
    /**
     * uint16
     */
    UInt16("UInt16"),
    /**
     * uInt32
     */
    UInt32("UInt32"),
    /**
     * Uint64
     */
    UInt64("UInt64"),
    /**
     * Uint128
     */
    UInt128("UInt128"),
    /**
     * UInt256
     */
    UInt256("UInt256"),
    /**
     * Float32
     */
    Float32("Float32"),
    /**
     * Float64
     */
    Float64("Float64"),
    /**
     * Decimal32
     */
    Decimal32("Decimal32"),
    /**
     * Decimal64
     */
    Decimal64("Decimal64"),
    /**
     * Decimal128
     */
    Decimal128("Decimal128"),
    /**
     * Decimal256
     */
    Decimal256("Decimal256"),
    /**
     * bool
     */
    Bool("Bool"),
    /**
     * string
     */
    String("String"),
    /**
     * fixed string
     */
    FixedString("FixedString"),
    /**
     * uuid
     */
    UUID("UUID"),
    /**
     * date
     */
    Date("Date"),
    /**
     * date32
     */
    Date32("Date32"),
    /**
     * datetime
     */
    DateTime("DateTime"),
    /**
     * datetime64
     */
    DateTime64("DateTime64"),
    /**
     * enum
     */
    Enum("Enum"),
    /**
     * LowCardinality
     */
    LowCardinality("LowCardinality"),
    /**
     * array
     */
    Array("Array"),
    /**
     * AggregateFunction
     */
    AggregateFunction("AggregateFunction"),
    /**
     * json
     */
    JSON("JSON"),
    /**
     * Nested
     */
    Nested("Nested"),
    /**
     * Tuple
     */
    Tuple("Tuple"),
    /**
     * nullable
     */
    Nullable("Nullable"),
    /**
     * map
     */
    Map("Map"),
    /**
     * SimpleAggregateFunction
     */
    SimpleAggregateFunction("SimpleAggregateFunction");

    private final String value;

    ClickHouseDataTypeName(String value) {
        this.value = value;
    }

    public static IDataTypeName getByValue(String value) {
        ClickHouseDataTypeName[] clickHouseDataTypeNames = ClickHouseDataTypeName.values();
        for (ClickHouseDataTypeName clickHouseDataTypeName : clickHouseDataTypeNames) {
            if (StringUtils.equalsIgnoreCase(clickHouseDataTypeName.getValue(), value)) {
                return clickHouseDataTypeName;
            }
        }
        throw new IllegalArgumentException("unsupported the dataType with value:" + value);
    }

    @Override
    public String getName() {
        return this.name();
    }

    @Override
    public String getValue() {
        return value;
    }
}
