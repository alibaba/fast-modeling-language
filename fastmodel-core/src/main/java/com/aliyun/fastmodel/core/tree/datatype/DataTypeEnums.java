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

/**
 * 内建的列数据类型
 *
 * @author panguanjing
 * @date 2020/9/14
 */
public enum DataTypeEnums {
    /**
     * tinyInt
     */
    TINYINT,
    /**
     * smallInt
     */
    SMALLINT,

    /**
     * MEDIUMINT
     */
    MEDIUMINT,
    /**
     * int
     */
    INT,
    /**
     * bigint
     */
    BIGINT,

    /**
     * float
     */
    FLOAT,
    /**
     * double
     */
    DOUBLE,
    /**
     * decimal
     */
    DECIMAL,
    /**
     * char
     */
    CHAR,
    /**
     * varchar
     */
    VARCHAR,
    /**
     * string
     */
    STRING,

    /**
     * Text
     */
    TEXT,
    /**
     * binary
     */
    BINARY,
    /**
     * date
     */
    DATE,
    /**
     * datetime
     */
    DATETIME,
    /**
     * timestamp
     */
    TIMESTAMP,
    /**
     * boolean
     */
    BOOLEAN,
    /**
     * array
     */
    ARRAY,
    /**
     * map
     */
    MAP,
    /**
     * struct
     */
    STRUCT,

    /**
     * json
     */
    JSON,

    /**
     * 自定义
     */
    CUSTOM;

    /**
     * 根据类型获取字段类型
     *
     * @param name 名称
     * @return {@link DataTypeEnums}
     * @throws IllegalArgumentException if can't find the columnDataType
     */
    public static DataTypeEnums getDataType(String name) {
        DataTypeEnums[] dataTypeEnums = DataTypeEnums.values();
        for (DataTypeEnums type : dataTypeEnums) {
            if (type.name().equalsIgnoreCase(name)) {
                return type;
            }
        }
        return DataTypeEnums.CUSTOM;
    }

    public static boolean isIntDataType(DataTypeEnums dataTypeEnums) {
        return dataTypeEnums == DataTypeEnums.BIGINT || dataTypeEnums == DataTypeEnums.INT
            || dataTypeEnums == DataTypeEnums.SMALLINT || dataTypeEnums == DataTypeEnums.TINYINT
            || dataTypeEnums == DataTypeEnums.MEDIUMINT;
    }
}
