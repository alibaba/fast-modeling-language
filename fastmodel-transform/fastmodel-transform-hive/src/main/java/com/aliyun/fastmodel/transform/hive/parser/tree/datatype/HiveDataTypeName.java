/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.hive.parser.tree.datatype;

import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import org.apache.commons.lang3.StringUtils;

/**
 * maxcompute dataType name
 *
 * @author panguanjing
 * @date 2022/8/7
 */
public enum HiveDataTypeName implements IDataTypeName {
    /**
     * 1.0 version
     */
    BIGINT("BIGINT", Dimension.ZERO),
    /**
     * double
     */
    DOUBLE("DOUBLE", Dimension.ZERO),

    /**
     * DOUBLE_PRECISION
     */
    DOUBLE_PRECISION("DOUBLE PRECISION", Dimension.ZERO),
    /**
     * decimal
     */
    DECIMAL("DECIMAL", Dimension.TWO),
    /**
     * string
     */
    STRING("STRING", Dimension.ZERO),

    /**
     * boolean
     */
    BOOLEAN("BOOLEAN", Dimension.ZERO),
    /**
     * array
     */
    ARRAY("ARRAY", Dimension.MULTIPLE),
    /**
     * map
     */
    MAP("MAP", Dimension.MULTIPLE),
    /**
     *
     */
    STRUCT("STRUCT", Dimension.MULTIPLE),

    /**
     * UNION_TYPE
     */
    UNION_TYPE("UNIONTYPE", Dimension.MULTIPLE),
    /**
     * 2.0 version
     */
    TINYINT("TINYINT", Dimension.ZERO),
    /**
     * smallint
     */
    SMALLINT("SMALLINT", Dimension.ZERO),

    /**
     * int
     */
    INT("INT", Dimension.ZERO),

    /**
     * INTEGER
     */
    INTEGER("INTEGER", Dimension.ZERO),

    /**
     * binary
     */
    BINARY("BINARY", Dimension.ZERO),

    /**
     * float
     */
    FLOAT("FLOAT", Dimension.ZERO),

    /**
     * varchar
     */
    VARCHAR("VARCHAR", Dimension.ONE),
    /**
     * char
     */
    CHAR("CHAR", Dimension.ONE),

    /**
     * date
     */
    DATE("DATE", Dimension.ZERO),
    /**
     * timestamp
     */
    TIMESTAMP("TIMESTAMP", Dimension.ZERO);

    /**
     * complex prefix
     */
    public static final String COMPLEX_PREFIX = "<";

    private final String value;

    private final Dimension dimension;

    HiveDataTypeName(String value, Dimension dimension) {
        this.value = value;
        this.dimension = dimension;
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

}
