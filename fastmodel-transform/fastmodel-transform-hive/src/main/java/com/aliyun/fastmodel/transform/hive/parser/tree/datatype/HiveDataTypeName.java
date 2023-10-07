/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.hive.parser.tree.datatype;

import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import com.aliyun.fastmodel.transform.api.datatype.simple.ISimpleDataTypeName;
import com.aliyun.fastmodel.transform.api.datatype.simple.SimpleDataTypeName;
import org.apache.commons.lang3.StringUtils;

/**
 * maxcompute dataType name
 *
 * @author panguanjing
 * @date 2022/8/7
 */
public enum HiveDataTypeName implements ISimpleDataTypeName {
    /**
     * BIGINT
     */
    BIGINT("BIGINT", Dimension.ZERO, SimpleDataTypeName.NUMBER),
    /**
     * double
     */
    DOUBLE("DOUBLE", Dimension.ZERO, SimpleDataTypeName.NUMBER),

    /**
     * DOUBLE_PRECISION
     */
    DOUBLE_PRECISION("DOUBLE PRECISION", Dimension.ZERO, SimpleDataTypeName.NUMBER),
    /**
     * decimal
     */
    DECIMAL("DECIMAL", Dimension.TWO, SimpleDataTypeName.NUMBER),
    /**
     * string
     */
    STRING("STRING", Dimension.ZERO, SimpleDataTypeName.STRING),

    /**
     * boolean
     */
    BOOLEAN("BOOLEAN", Dimension.ZERO, SimpleDataTypeName.BOOLEAN),
    /**
     * array
     */
    ARRAY("ARRAY", Dimension.MULTIPLE, SimpleDataTypeName.STRING),
    /**
     * map
     */
    MAP("MAP", Dimension.MULTIPLE, SimpleDataTypeName.STRING),
    /**
     *
     */
    STRUCT("STRUCT", Dimension.MULTIPLE, SimpleDataTypeName.STRING),

    /**
     * UNION_TYPE
     */
    UNION_TYPE("UNIONTYPE", Dimension.MULTIPLE, SimpleDataTypeName.STRING),
    /**
     * 2.0 version
     */
    TINYINT("TINYINT", Dimension.ZERO, SimpleDataTypeName.NUMBER),
    /**
     * smallint
     */
    SMALLINT("SMALLINT", Dimension.ZERO, SimpleDataTypeName.NUMBER),

    /**
     * int
     */
    INT("INT", Dimension.ZERO, SimpleDataTypeName.NUMBER),

    /**
     * INTEGER
     */
    INTEGER("INTEGER", Dimension.ZERO, SimpleDataTypeName.NUMBER),

    /**
     * binary
     */
    BINARY("BINARY", Dimension.ZERO, SimpleDataTypeName.STRING),

    /**
     * float
     */
    FLOAT("FLOAT", Dimension.ZERO, SimpleDataTypeName.NUMBER),

    /**
     * varchar
     */
    VARCHAR("VARCHAR", Dimension.ONE, SimpleDataTypeName.STRING),
    /**
     * char
     */
    CHAR("CHAR", Dimension.ONE, SimpleDataTypeName.STRING),

    /**
     * date
     */
    DATE("DATE", Dimension.ZERO, SimpleDataTypeName.DATE),
    /**
     * timestamp
     */
    TIMESTAMP("TIMESTAMP", Dimension.ZERO, SimpleDataTypeName.DATE);

    /**
     * complex prefix
     */
    public static final String COMPLEX_PREFIX = "<";

    private final String value;

    private final Dimension dimension;

    private final SimpleDataTypeName simpleDataTypeName;

    HiveDataTypeName(String value, Dimension dimension, SimpleDataTypeName simpleDataTypeName) {
        this.value = value;
        this.dimension = dimension;
        this.simpleDataTypeName = simpleDataTypeName;
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

    /**
     * 根据传入的value获取mc的dataType
     *
     * @param value
     * @return
     */
    public static HiveDataTypeName getByValue(String value) {
        if (value.indexOf(COMPLEX_PREFIX) > 0) {
            return getByValue(value.substring(0, value.indexOf(COMPLEX_PREFIX)));
        }
        HiveDataTypeName[] maxComputeDataTypeNames = HiveDataTypeName.values();
        for (HiveDataTypeName m : maxComputeDataTypeNames) {
            if (StringUtils.equalsIgnoreCase(m.getValue(), value)) {
                return m;
            }
        }
        throw new IllegalArgumentException("unsupported dataType with value: " + value);
    }

    @Override
    public SimpleDataTypeName getSimpleDataTypeName() {
        return simpleDataTypeName;
    }
}
