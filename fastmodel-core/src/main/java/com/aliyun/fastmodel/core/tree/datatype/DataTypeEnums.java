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

package com.aliyun.fastmodel.core.tree.datatype;

import lombok.Getter;

/**
 * 内建的列数据类型
 *
 * @author panguanjing
 * @date 2020/9/14
 */
public enum DataTypeEnums implements IDataTypeName {
    /**
     * tinyInt
     */
    TINYINT(Dimension.ZERO),
    /**
     * smallInt
     */
    SMALLINT(Dimension.ZERO),

    /**
     * MEDIUMINT
     */
    MEDIUMINT(Dimension.ZERO),
    /**
     * int
     */
    INT(Dimension.ZERO),
    /**
     * bigint
     */
    BIGINT(Dimension.ZERO),

    /**
     * float
     */
    FLOAT(Dimension.ZERO),
    /**
     * double
     */
    DOUBLE(Dimension.ZERO),
    /**
     * decimal
     */
    DECIMAL(Dimension.TWO),
    /**
     * char
     */
    CHAR(Dimension.ONE),
    /**
     * varchar
     */
    VARCHAR(Dimension.ONE),
    /**
     * string
     */
    STRING(Dimension.ZERO),

    /**
     * Text
     */
    TEXT(Dimension.ZERO),
    /**
     * binary
     */
    BINARY(Dimension.ZERO),
    /**
     * date
     */
    DATE(Dimension.ZERO),
    /**
     * datetime
     */
    DATETIME(Dimension.ZERO),
    /**
     * timestamp
     */
    TIMESTAMP(Dimension.ZERO),
    /**
     * boolean
     */
    BOOLEAN(Dimension.ZERO),
    /**
     * array
     */
    ARRAY(Dimension.MULTIPLE),
    /**
     * map
     */
    MAP(Dimension.MULTIPLE),
    /**
     * struct
     */
    STRUCT(Dimension.MULTIPLE),

    /**
     * json
     */
    JSON(Dimension.ZERO),

    /**
     * 自定义
     */
    CUSTOM(Dimension.ZERO);

    private final Dimension dimension;

    DataTypeEnums(Dimension dimension) {this.dimension = dimension;}

    /**
     * 根据类型获取字段类型
     *
     * @param value 名称
     * @return {@link DataTypeEnums}
     * @throws IllegalArgumentException if can't find the columnDataType
     */
    public static DataTypeEnums getDataType(String value) {
        DataTypeEnums[] dataTypeEnums = DataTypeEnums.values();
        for (DataTypeEnums type : dataTypeEnums) {
            if (type.name().equalsIgnoreCase(value)) {
                return type;
            }
        }
        return DataTypeEnums.CUSTOM;
    }

    public static boolean isIntDataType(IDataTypeName dataTypeEnums) {
        return dataTypeEnums == DataTypeEnums.BIGINT || dataTypeEnums == DataTypeEnums.INT
            || dataTypeEnums == DataTypeEnums.SMALLINT || dataTypeEnums == DataTypeEnums.TINYINT
            || dataTypeEnums == DataTypeEnums.MEDIUMINT;
    }

    @Override
    public String getName() {
        return this.name();
    }

    @Override
    public String getValue() {
        return this.name();
    }

    @Override
    public Dimension getDimension() {
        return dimension;
    }

}
