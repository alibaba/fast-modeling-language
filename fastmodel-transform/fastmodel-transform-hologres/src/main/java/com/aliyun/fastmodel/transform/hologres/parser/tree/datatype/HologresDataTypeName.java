/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.hologres.parser.tree.datatype;

import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import org.apache.commons.lang3.StringUtils;

/**
 * type name
 * https://help.aliyun.com/document_detail/130398.html
 *
 * @author panguanjing
 * @date 2022/6/9
 */
public enum HologresDataTypeName implements IDataTypeName {
    /**
     * integer
     */
    INTEGER("INTEGER", "INT4", Dimension.ZERO),

    /**
     * integer2
     */
    INTEGER2("INTEGER", "INT", Dimension.ZERO),
    /**
     * bigint
     */
    BIGINT("BIGINT", "INT8", Dimension.ZERO),
    /**
     * boolean
     */
    BOOLEAN("BOOLEAN", "BOOL", Dimension.ZERO),
    /**
     * real
     */
    REAL("REAL", "FLOAT4", Dimension.ZERO),

    /**
     * double
     */
    DOUBLE_PRECISION("DOUBLE PRECISION", "FLOAT8", Dimension.ZERO),

    /**
     * text
     */
    TEXT("TEXT", "VARCHAR", Dimension.ZERO),

    /**
     * time stamptz
     */
    TIMESTAMPTZ("TIMESTAMP WITH TIME ZONE", "TIMESTAMPTZ", Dimension.ZERO),

    /**
     * decimal
     */
    DECIMAL("DECIMAL", "NUMERIC", Dimension.TWO),

    /**
     * date
     */
    DATE("DATE", "", Dimension.ZERO),

    /**
     * timestamp
     */
    TIMESTAMP("TIMESTAMP", "", Dimension.ZERO),

    /**
     * char
     */
    CHAR("CHAR", "BPCHAR", Dimension.ONE),

    /**
     * varchar
     */
    VARCHAR("VARCHAR", "", Dimension.ONE),

    /**
     * serial
     */
    SERIAL("SERIAL", "", Dimension.ZERO),

    /**
     * small int
     */
    SMALLINT("SMALLINT", "INT2", Dimension.ZERO),
    /**
     * json
     */
    JSON("JSON", "", Dimension.ZERO),
    /**
     * jsonb
     */
    JSONB("JSONB", "", Dimension.ZERO),

    /**
     * BYTEA
     */
    BYTEA("BYTEA", "", Dimension.ZERO),

    /**
     * roaring bitmap
     */
    ROARING_BITMAP("ROARINGBITMAP", "", Dimension.ZERO),

    /**
     * bit
     */
    BIT("BIT", "", Dimension.ONE),
    /**
     * timez
     */
    TIMETZ("TIMETZ", "", Dimension.ZERO),
    /**
     * time
     */
    TIME("TIME", "", Dimension.ZERO),
    /**
     * inet
     */
    INET("INET", "", Dimension.ZERO),
    /**
     * money
     */
    MONEY("MONEY", "", Dimension.ZERO),

    /**
     * varbit
     */
    VARBIT("VARBIT", "", Dimension.ONE),
    /**
     * interval
     */
    INTERVAL("INTERVAL", "", Dimension.ZERO),
    /**
     * oid
     */
    OID("OID", "", Dimension.ZERO),
    /**
     * uuid
     */
    UUID("UUID", "", Dimension.ZERO);

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

    HologresDataTypeName(String value, String alias, Dimension dimension) {
        this.value = value;
        this.alias = alias;
        this.dimension = dimension;
    }

    /**
     * 先根据value来进行处理，如果处理不到，会读取alias
     *
     * @param value 值或者别名
     * @return {@link HologresDataTypeName}
     */
    public static IDataTypeName getByValue(String value) {
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

}
