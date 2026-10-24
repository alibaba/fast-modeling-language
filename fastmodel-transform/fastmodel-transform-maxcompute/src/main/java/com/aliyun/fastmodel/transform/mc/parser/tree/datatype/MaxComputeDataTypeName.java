/*
 * Copyright 2022 Alibaba Group Holding Ltd.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.aliyun.fastmodel.transform.mc.parser.tree.datatype;

import com.aliyun.fastmodel.transform.api.datatype.simple.ISimpleDataTypeName;
import com.aliyun.fastmodel.transform.api.datatype.simple.SimpleDataTypeName;
import org.apache.commons.lang3.StringUtils;

/**
 * maxcompute dataType name
 *
 * @author panguanjing
 * @date 2022/8/7
 */
public enum MaxComputeDataTypeName implements ISimpleDataTypeName {
    /**
     * 1.0 version
     */
    BIGINT("BIGINT", Dimension.ZERO, SimpleDataTypeName.NUMBER),
    /**
     * double
     */
    DOUBLE("DOUBLE", Dimension.ZERO, SimpleDataTypeName.NUMBER),
    /**
     * decimal
     */
    DECIMAL("DECIMAL", Dimension.TWO, SimpleDataTypeName.NUMBER),
    /**
     * string
     */
    STRING("STRING", Dimension.ZERO, SimpleDataTypeName.STRING),
    /**
     * datetime
     */
    DATETIME("DATETIME", Dimension.ZERO, SimpleDataTypeName.DATE),
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
    TIMESTAMP("TIMESTAMP", Dimension.ZERO, SimpleDataTypeName.DATE),

    /**
     * timestamp
     */
    TIMESTAMP_NTZ("TIMESTAMP_NTZ", Dimension.ZERO, SimpleDataTypeName.DATE),

    /**
     * JSON
     */
    JSON("JSON", Dimension.MULTIPLE, SimpleDataTypeName.STRING),

    /**
     * vector
     */
    VECTOR("VECTOR", Dimension.TWO, SimpleDataTypeName.STRING);

    /**
     * complex prefix
     */
    public static final String COMPLEX_PREFIX = "<";

    private final String value;

    private final Dimension dimension;

    private final SimpleDataTypeName simpleDataTypeName;

    MaxComputeDataTypeName(String value, Dimension dimension, SimpleDataTypeName simpleDataTypeName) {
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
    public static MaxComputeDataTypeName getByValue(String value) {
        if (value.indexOf(COMPLEX_PREFIX) > 0) {
            return getByValue(value.substring(0, value.indexOf(COMPLEX_PREFIX)));
        }
        MaxComputeDataTypeName[] maxComputeDataTypeNames = MaxComputeDataTypeName.values();
        for (MaxComputeDataTypeName m : maxComputeDataTypeNames) {
            if (StringUtils.equalsIgnoreCase(m.getValue(), value)) {
                return m;
            }
        }
        throw new IllegalArgumentException("unsupported dataType with value: " + value);
    }

    @Override
    public SimpleDataTypeName getSimpleDataTypeName() {
        return this.simpleDataTypeName;
    }
}
