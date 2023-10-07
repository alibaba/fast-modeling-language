/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.hologres.parser.tree.datatype;

import com.aliyun.fastmodel.transform.api.datatype.simple.ISimpleDataTypeName;
import com.aliyun.fastmodel.transform.api.datatype.simple.SimpleDataTypeName;
import org.apache.commons.lang3.StringUtils;

/**
 * type name
 * https://help.aliyun.com/document_detail/130398.html
 *
 * @author panguanjing
 * @date 2022/6/9
 */
public enum HologresDataTypeName implements ISimpleDataTypeName {
    /**
     * integer
     */
    INTEGER("INTEGER", "INT4", Dimension.ZERO, SimpleDataTypeName.NUMBER),

    /**
     * integer2
     */
    INTEGER2("INTEGER", "INT", Dimension.ZERO, SimpleDataTypeName.NUMBER),
    /**
     * bigint
     */
    BIGINT("BIGINT", "INT8", Dimension.ZERO, SimpleDataTypeName.NUMBER),
    /**
     * boolean
     */
    BOOLEAN("BOOLEAN", "BOOL", Dimension.ZERO, SimpleDataTypeName.BOOLEAN),
    /**
     * real
     */
    REAL("REAL", "FLOAT4", Dimension.ZERO, SimpleDataTypeName.NUMBER),

    /**
     * double
     */
    DOUBLE_PRECISION("DOUBLE PRECISION", "FLOAT8", Dimension.ZERO, SimpleDataTypeName.NUMBER),

    /**
     * text
     */
    TEXT("TEXT", "VARCHAR", Dimension.ZERO, SimpleDataTypeName.STRING),

    /**
     * time stamptz
     */
    TIMESTAMPTZ("TIMESTAMP WITH TIME ZONE", "TIMESTAMPTZ", Dimension.ZERO, SimpleDataTypeName.DATE),

    /**
     * decimal
     */
    DECIMAL("DECIMAL", "NUMERIC", Dimension.TWO, SimpleDataTypeName.NUMBER),

    /**
     * date
     */
    DATE("DATE", "", Dimension.ZERO, SimpleDataTypeName.DATE),

    /**
     * timestamp
     */
    TIMESTAMP("TIMESTAMP", "", Dimension.ZERO, SimpleDataTypeName.DATE),

    /**
     * char
     */
    CHAR("CHAR", "BPCHAR", Dimension.ONE, SimpleDataTypeName.STRING),

    /**
     * varchar
     */
    VARCHAR("VARCHAR", "", Dimension.ONE, SimpleDataTypeName.STRING),

    /**
     * serial:https://help.aliyun.com/document_detail/187391.html
     */
    SERIAL("SERIAL", "", Dimension.ZERO, SimpleDataTypeName.NUMBER),

    /**
     * 自增序列字段:https://help.aliyun.com/document_detail/187391.html
     */
    BIGSERIAL("BIGSERIAL", "", Dimension.ZERO, SimpleDataTypeName.NUMBER),

    /**
     * small int
     */
    SMALLINT("SMALLINT", "INT2", Dimension.ZERO, SimpleDataTypeName.NUMBER),
    /**
     * json
     */
    JSON("JSON", "", Dimension.ZERO, SimpleDataTypeName.STRING),
    /**
     * jsonb
     */
    JSONB("JSONB", "", Dimension.ZERO, SimpleDataTypeName.STRING),

    /**
     * BYTEA
     */
    BYTEA("BYTEA", "", Dimension.ZERO, SimpleDataTypeName.STRING),

    /**
     * roaring bitmap
     */
    ROARING_BITMAP("ROARINGBITMAP", "", Dimension.ZERO, SimpleDataTypeName.STRING),

    /**
     * bit
     */
    BIT("BIT", "", Dimension.ONE, SimpleDataTypeName.STRING),
    /**
     * timez
     */
    TIMETZ("TIMETZ", "", Dimension.ZERO, SimpleDataTypeName.DATE),
    /**
     * time
     */
    TIME("TIME", "", Dimension.ZERO, SimpleDataTypeName.DATE),
    /**
     * inet
     */
    INET("INET", "", Dimension.ZERO, SimpleDataTypeName.STRING),
    /**
     * money
     */
    MONEY("MONEY", "", Dimension.ZERO, SimpleDataTypeName.STRING),

    /**
     * varbit
     */
    VARBIT("VARBIT", "", Dimension.ONE, SimpleDataTypeName.STRING),
    /**
     * interval
     */
    INTERVAL("INTERVAL", "", Dimension.ZERO, SimpleDataTypeName.STRING),
    /**
     * oid
     */
    OID("OID", "", Dimension.ZERO, SimpleDataTypeName.STRING),
    /**
     * uuid
     */
    UUID("UUID", "", Dimension.ZERO, SimpleDataTypeName.STRING);

    /**
     * value
     */
    private final String value;

    /**
     * alias
     */
    private final String alias;

    /**
     * dimension
     */
    private final Dimension dimension;


    private final SimpleDataTypeName simpleDataTypeName;

    HologresDataTypeName(String value, String alias, Dimension dimension, SimpleDataTypeName simpleDataTypeName) {
        this.value = value;
        this.alias = alias;
        this.dimension = dimension;
        this.simpleDataTypeName =simpleDataTypeName;
    }

    /**
     * 先根据value来进行处理，如果处理不到，会读取alias
     *
     * @param value 值或者别名
     * @return {@link HologresDataTypeName}
     */
    public static ISimpleDataTypeName getByValue(String value) {
        int index = value.indexOf(HologresArrayDataTypeName.VALUE_SUFFIX);
        if (index > 0) {
            return HologresArrayDataTypeName.getByValue(value);
        }
        HologresDataTypeName[] hologresDataTypeNames = HologresDataTypeName.values();
        //先从value中获取
        for (HologresDataTypeName hologresGenericDataType : hologresDataTypeNames) {
            boolean equalsIgnoreCase = StringUtils.equalsIgnoreCase(hologresGenericDataType.getValue(), value);
            if (equalsIgnoreCase) {
                return hologresGenericDataType;
            }
        }
        //如果value获取不到，再走别名
        for (HologresDataTypeName hologresGenericDataType : hologresDataTypeNames) {
            boolean notBlank = StringUtils.isNotBlank(hologresGenericDataType.getAlias());
            boolean equals = StringUtils.equalsIgnoreCase(hologresGenericDataType.getAlias(), value);
            if (notBlank && equals) {
                return hologresGenericDataType;
            }
        }
        throw new IllegalArgumentException("not support the dataType with value:" + value);
    }

    public String getAlias() {
        return alias;
    }

    @Override
    public String getName() {
        return this.name();
    }

    @Override
    public String getValue() {
        return this.value;
    }

    @Override
    public Dimension getDimension() {
        return this.dimension;
    }

    @Override
    public SimpleDataTypeName getSimpleDataTypeName() {
        return simpleDataTypeName;
    }
}
