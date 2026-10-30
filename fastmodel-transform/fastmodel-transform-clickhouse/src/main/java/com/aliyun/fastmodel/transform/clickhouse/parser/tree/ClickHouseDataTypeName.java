/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.clickhouse.parser.tree;

import java.util.Locale;
import java.util.Set;

import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName.Dimension;
import com.aliyun.fastmodel.transform.api.datatype.simple.ISimpleDataTypeName;
import com.aliyun.fastmodel.transform.api.datatype.simple.SimpleDataTypeName;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.StringUtils;

/**
 * click house data Type name
 * <p>
 * ClickHouse 的类型名大小写敏感(String、UInt64、DateTime64)，value 为 ClickHouse 的规范类型名；
 * aliases 为同步建表场景常见的通用类型名(如 BIGINT、VARCHAR)，统一映射到 ClickHouse 的规范类型。
 *
 * @author panguanjing
 * @date 2022/7/10
 */
public enum ClickHouseDataTypeName implements ISimpleDataTypeName {

    /**
     * int8
     */
    INT8("Int8", Dimension.ZERO, SimpleDataTypeName.NUMBER, "TINYINT"),

    /**
     * int16
     */
    INT16("Int16", Dimension.ZERO, SimpleDataTypeName.NUMBER, "SMALLINT"),

    /**
     * int32
     */
    INT32("Int32", Dimension.ZERO, SimpleDataTypeName.NUMBER, "INT", "INTEGER", "MEDIUMINT"),

    /**
     * int64
     */
    INT64("Int64", Dimension.ZERO, SimpleDataTypeName.NUMBER, "BIGINT", "LONG"),

    /**
     * int128
     */
    INT128("Int128", Dimension.ZERO, SimpleDataTypeName.NUMBER, "LARGEINT"),

    /**
     * int256
     */
    INT256("Int256", Dimension.ZERO, SimpleDataTypeName.NUMBER),

    /**
     * uint8
     */
    UInt8("UInt8", Dimension.ZERO, SimpleDataTypeName.NUMBER),

    /**
     * uint16
     */
    UInt16("UInt16", Dimension.ZERO, SimpleDataTypeName.NUMBER),

    /**
     * uint32
     */
    UInt32("UInt32", Dimension.ZERO, SimpleDataTypeName.NUMBER),

    /**
     * uint64
     */
    UInt64("UInt64", Dimension.ZERO, SimpleDataTypeName.NUMBER),

    /**
     * uint128
     */
    UInt128("UInt128", Dimension.ZERO, SimpleDataTypeName.NUMBER),

    /**
     * uint256
     */
    UInt256("UInt256", Dimension.ZERO, SimpleDataTypeName.NUMBER),

    /**
     * float32
     */
    Float32("Float32", Dimension.ZERO, SimpleDataTypeName.NUMBER, "FLOAT"),

    /**
     * float64
     */
    Float64("Float64", Dimension.ZERO, SimpleDataTypeName.NUMBER, "DOUBLE", "REAL"),

    /**
     * decimal
     */
    Decimal("Decimal", Dimension.TWO, SimpleDataTypeName.NUMBER, "DECIMAL", "NUMERIC"),

    /**
     * decimal32
     */
    Decimal32("Decimal32", Dimension.ONE, SimpleDataTypeName.NUMBER),

    /**
     * decimal64
     */
    Decimal64("Decimal64", Dimension.ONE, SimpleDataTypeName.NUMBER),

    /**
     * decimal128
     */
    Decimal128("Decimal128", Dimension.ONE, SimpleDataTypeName.NUMBER),

    /**
     * decimal256
     */
    Decimal256("Decimal256", Dimension.ONE, SimpleDataTypeName.NUMBER),

    /**
     * bool
     */
    Bool("Bool", Dimension.ZERO, SimpleDataTypeName.BOOLEAN, "BOOLEAN"),

    /**
     * string
     */
    String("String", Dimension.ZERO, SimpleDataTypeName.STRING,
        "VARCHAR", "CHAR", "TEXT", "TINYTEXT", "MEDIUMTEXT", "LONGTEXT", "CLOB", "BLOB", "BINARY", "VARBINARY"),

    /**
     * fixed string
     */
    FixedString("FixedString", Dimension.ONE, SimpleDataTypeName.STRING),

    /**
     * uuid
     */
    UUID("UUID", Dimension.ZERO, SimpleDataTypeName.STRING),

    /**
     * date
     */
    Date("Date", Dimension.ZERO, SimpleDataTypeName.DATE),

    /**
     * date32
     */
    Date32("Date32", Dimension.ZERO, SimpleDataTypeName.DATE),

    /**
     * datetime
     */
    DateTime("DateTime", Dimension.ZERO, SimpleDataTypeName.DATE, "TIMESTAMP"),

    /**
     * datetime64,参数的第一个精度与 DateTime64(p) 的 p 对应
     */
    DateTime64("DateTime64", Dimension.TWO, SimpleDataTypeName.DATE),

    /**
     * enum
     */
    Enum("Enum", Dimension.ZERO, SimpleDataTypeName.STRING),

    /**
     * enum8
     */
    Enum8("Enum8", Dimension.ZERO, SimpleDataTypeName.STRING),

    /**
     * enum16
     */
    Enum16("Enum16", Dimension.ZERO, SimpleDataTypeName.STRING),

    /**
     * LowCardinality
     */
    LowCardinality("LowCardinality", Dimension.ZERO, SimpleDataTypeName.STRING),

    /**
     * array
     */
    Array("Array", Dimension.ZERO, SimpleDataTypeName.STRING),

    /**
     * AggregateFunction
     */
    AggregateFunction("AggregateFunction", Dimension.ZERO, SimpleDataTypeName.STRING),

    /**
     * json
     */
    JSON("JSON", Dimension.ZERO, SimpleDataTypeName.STRING, "OBJECT"),

    /**
     * Nested
     */
    Nested("Nested", Dimension.ZERO, SimpleDataTypeName.STRING, "STRUCT"),

    /**
     * Tuple
     */
    Tuple("Tuple", Dimension.ZERO, SimpleDataTypeName.STRING),

    /**
     * nullable
     */
    Nullable("Nullable", Dimension.ZERO, SimpleDataTypeName.STRING),

    /**
     * map
     */
    Map("Map", Dimension.ZERO, SimpleDataTypeName.STRING),

    /**
     * SimpleAggregateFunction
     */
    SimpleAggregateFunction("SimpleAggregateFunction", Dimension.ZERO, SimpleDataTypeName.STRING);

    /**
     * 类型的左括号,用于从完整类型文本(如 FixedString(16))中提取类型名
     */
    private static final String PARAM_PREFIX = "(";

    private final String value;

    private final Dimension dimension;

    private final SimpleDataTypeName simpleDataTypeName;

    private final Set<String> aliases;

    ClickHouseDataTypeName(String value, Dimension dimension, SimpleDataTypeName simpleDataTypeName, String... aliases) {
        this.value = value;
        this.dimension = dimension;
        this.simpleDataTypeName = simpleDataTypeName;
        this.aliases = ImmutableSet.copyOf(aliases);
    }

    /**
     * 根据类型名获取ClickHouse的数据类型,支持带参数的类型文本以及通用类型别名
     *
     * @param value 类型文本
     * @return {@link ClickHouseDataTypeName}
     */
    public static ClickHouseDataTypeName getByValue(String value) {
        String typeName = StringUtils.trimToEmpty(value);
        int index = typeName.indexOf(PARAM_PREFIX);
        if (index > 0) {
            typeName = typeName.substring(0, index).trim();
        }
        for (ClickHouseDataTypeName clickHouseDataTypeName : values()) {
            if (StringUtils.equalsIgnoreCase(clickHouseDataTypeName.getValue(), typeName)) {
                return clickHouseDataTypeName;
            }
            for (String alias : clickHouseDataTypeName.aliases) {
                if (StringUtils.equalsIgnoreCase(alias, typeName)) {
                    return clickHouseDataTypeName;
                }
            }
        }
        throw new IllegalArgumentException("unsupported the dataType with value:" + value);
    }

    /**
     * 是否为ClickHouse的原生类型名(大小写不敏感),通用别名不算原生类型
     *
     * @param value 类型文本
     * @return true为原生类型
     */
    public static boolean isNativeType(String value) {
        String typeName = StringUtils.trimToEmpty(value).toUpperCase(Locale.ROOT);
        for (ClickHouseDataTypeName clickHouseDataTypeName : values()) {
            if (StringUtils.equalsIgnoreCase(clickHouseDataTypeName.getValue(), typeName)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String getName() {
        return this.name();
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public Dimension getDimension() {
        return dimension;
    }

    @Override
    public SimpleDataTypeName getSimpleDataTypeName() {
        return simpleDataTypeName;
    }
}
