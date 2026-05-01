/*
 * Copyright 2021-2022 Alibaba Group Holding Ltd.
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

package com.aliyun.fastmodel.transform.starrocks.parser.tree.datatype;

import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import com.aliyun.fastmodel.transform.api.datatype.simple.ISimpleDataTypeName;
import com.aliyun.fastmodel.transform.api.datatype.simple.SimpleDataTypeName;
import org.apache.commons.lang3.StringUtils;

/**
 * starRocks DataType Name
 *
 * @author panguanjing
 * @date 2023/9/12
 */
public enum StarRocksDataTypeName implements ISimpleDataTypeName {
    /**
     * boolean
     */
    BOOLEAN("BOOLEAN", Dimension.ZERO, SimpleDataTypeName.BOOLEAN),

    /**
     * tinyint
     */
    TINYINT("TINYINT", Dimension.ONE, SimpleDataTypeName.NUMBER),

    /**
     * small int
     */
    SMALLINT("SMALLINT", Dimension.ONE, SimpleDataTypeName.NUMBER),

    /**
     * signed int
     */
    SIGNED_INT("SIGNED INT", Dimension.ZERO, SimpleDataTypeName.NUMBER),

    /**
     * singed integer
     */
    SIGNED_INTEGER("SIGNED INTEGER", Dimension.ZERO, SimpleDataTypeName.NUMBER),

    /**
     * int
     */
    INT("INT", Dimension.ONE, SimpleDataTypeName.NUMBER),

    /**
     * integer
     */
    INTEGER("INTEGER", Dimension.ONE, SimpleDataTypeName.NUMBER),

    /**
     * bigint
     */
    BIGINT("BIGINT", Dimension.ONE, SimpleDataTypeName.NUMBER),

    /**
     * large int
     */
    LARGEINT("LARGEINT", Dimension.ONE, SimpleDataTypeName.NUMBER),

    /**
     * float
     */
    FLOAT("FLOAT", Dimension.ZERO, SimpleDataTypeName.NUMBER),

    /**
     * double
     */
    DOUBLE("DOUBLE", Dimension.ZERO, SimpleDataTypeName.NUMBER),

    /**
     * date
     */
    DATE("DATE", Dimension.ZERO, SimpleDataTypeName.DATE),

    /**
     * datev2 (Doris/StarRocks compatible)
     */
    DATEV2("DATEV2", Dimension.ZERO, SimpleDataTypeName.DATE),

    /**
     * datetime
     */
    DATETIME("DATETIME", Dimension.ZERO, SimpleDataTypeName.DATE),

    /**
     * datetimev2 (Doris/StarRocks compatible)
     */
    DATETIMEV2("DATETIMEV2", Dimension.ONE, SimpleDataTypeName.DATE),

    /**
     * time
     */
    TIME("TIME", Dimension.ZERO, SimpleDataTypeName.DATE),

    /**
     * char
     */
    CHAR("CHAR", Dimension.ONE, SimpleDataTypeName.STRING),

    /**
     * varchar
     */
    VARCHAR("VARCHAR", Dimension.ONE, SimpleDataTypeName.STRING),

    /**
     * string
     */
    STRING("STRING", Dimension.ZERO, SimpleDataTypeName.STRING),

    /**
     * text
     */
    TEXT("TEXT", Dimension.ZERO, SimpleDataTypeName.STRING),

    /**
     * bitmap
     */
    BITMAP("BITMAP", Dimension.ZERO, SimpleDataTypeName.NUMBER),

    /**
     * hll
     */
    HLL("HLL", Dimension.ZERO, SimpleDataTypeName.NUMBER),

    /**
     * percentile
     */
    PERCENTILE("PERCENTILE", Dimension.ZERO, SimpleDataTypeName.NUMBER),

    /**
     * json
     */
    JSON("JSON", Dimension.ZERO, SimpleDataTypeName.STRING),

    /**
     * varbinary
     */
    VARBINARY("VARBINARY", Dimension.ONE, SimpleDataTypeName.STRING),

    /**
     * binary
     */
    BINARY("BINARY", Dimension.ONE, SimpleDataTypeName.STRING),

    /**
     * decimal
     */
    DECIMAL("DECIMAL", Dimension.TWO, SimpleDataTypeName.NUMBER),

    /**
     * decimal
     */
    DECIMALV2("DECIMALV2", Dimension.TWO, SimpleDataTypeName.NUMBER),

    /**
     * decimalv3, default decimal type since StarRocks 3.0
     */
    DECIMALV3("DECIMALV3", Dimension.TWO, SimpleDataTypeName.NUMBER),

    /**
     * decimal32
     */
    DECIMAL32("DECIMAL32", Dimension.TWO, SimpleDataTypeName.NUMBER),

    /**
     * decimal64
     */
    DECIMAL64("DECIMAL64", Dimension.TWO, SimpleDataTypeName.NUMBER),

    /**
     * decimal128
     */
    DECIMAL128("DECIMAL128", Dimension.TWO, SimpleDataTypeName.NUMBER),

    /**
     * numeric
     */
    NUMERIC("NUMERIC", Dimension.TWO, SimpleDataTypeName.NUMBER),

    /**
     * number
     */
    NUMBER("NUMBER", Dimension.TWO, SimpleDataTypeName.NUMBER),

    /**
     * Array
     */
    ARRAY("ARRAY", Dimension.MULTIPLE, SimpleDataTypeName.STRING),

    /**
     * MAP
     */
    Map("MAP", Dimension.MULTIPLE, SimpleDataTypeName.STRING),
    ;

    /**
     * multi prefix
     */
    public static final String MULTI_PREFIX = "<";

    private final String value;

    private final Dimension dimension;

    private final SimpleDataTypeName simpleDataTypeName;

    StarRocksDataTypeName(String value, Dimension dimension,
        SimpleDataTypeName simpleDataTypeName) {
        this.value = value;
        this.dimension = dimension;
        this.simpleDataTypeName = simpleDataTypeName;
    }

    public static IDataTypeName getByValue(String value) {
        String v = value;
        if (v.indexOf(MULTI_PREFIX) > 0) {
            v = v.substring(0, v.indexOf(MULTI_PREFIX)).trim();
        }
        StarRocksDataTypeName[] starRocksDataTypeNames = StarRocksDataTypeName.values();
        for (StarRocksDataTypeName s : starRocksDataTypeNames) {
            if (StringUtils.equalsIgnoreCase(s.getValue(), v)) {
                return s;
            }
        }
        throw new IllegalArgumentException("not support the dataType with value:" + value);
    }

    @Override
    public SimpleDataTypeName getSimpleDataTypeName() {
        return simpleDataTypeName;
    }

    @Override
    public String getName() {
        return name();
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public Dimension getDimension() {
        return dimension;
    }

}
