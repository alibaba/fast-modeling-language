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

package com.aliyun.fastmodel.driver.model;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

import lombok.Getter;

/**
 * 驱动程序DataType
 *
 * @author panguanjing
 * @date 2021/1/4
 */
@Getter
public class DriverDataType {

    private final int type;

    private final Class<?> javaType;

    public DriverDataType(int type) {
        this.type = type;
        javaType = getClassForTypeId(type);
    }

    public DriverDataType(Class<?> clazz) {
        javaType = clazz;
        type = getTypeIdForObject(clazz);
    }

    /**
     * Returns the class associated with a java.sql.Types id
     *
     * @param type
     * @return
     */
    public static Class<?> getClassForTypeId(int type) {
        switch (type) {
            case Types.ARRAY:
                return Array.class;
            case Types.BIGINT:
                return Long.class;
            case Types.TINYINT:
                return Byte.class;
            case Types.BINARY:
            case Types.LONGVARBINARY:
            case Types.VARBINARY:
                return Byte[].class;
            case Types.BIT:
            case Types.BOOLEAN:
                return Boolean.class;
            case Types.CHAR:
                return Character.class;
            case Types.DATE:
                return java.util.Date.class;
            case Types.DOUBLE:
                return Double.class;
            case Types.FLOAT:
            case Types.REAL:
                return Float.class;
            case Types.INTEGER:
                return Integer.class;
            case Types.NUMERIC:
                return BigDecimal.class;
            case Types.SMALLINT:
                return Short.class;
            case Types.LONGVARCHAR:
            case Types.VARCHAR:
                return String.class;
            case Types.TIME:
                return Time.class;
            case Types.TIMESTAMP:
                return Timestamp.class;
            default:
                return Object.class;
        }
    }

    /**
     * 根据class获取SQL Type类型
     *
     * @param c
     * @return
     */
    public static int getTypeIdForObject(Class<?> c) {
        if (c == Long.class) {
            return Types.BIGINT;
        }
        if (c == Boolean.class) {
            return Types.BOOLEAN;
        }
        if (c == Character.class) {
            return Types.CHAR;
        }
        if (c == Timestamp.class) {
            return Types.TIMESTAMP;
        }
        if (c == java.sql.Date.class) {
            return Types.DATE;
        }
        if (c == java.util.Date.class) {
            return Types.DATE;
        }
        if (c == Double.class) {
            return Types.DOUBLE;
        }
        if (c == Integer.class) {
            return Types.INTEGER;
        }
        if (c == BigDecimal.class) {
            return Types.NUMERIC;
        }
        if (c == Short.class) {
            return Types.SMALLINT;
        }
        if (c == Float.class) {
            return Types.FLOAT;
        }
        if (c == String.class) {
            return Types.VARCHAR;
        }
        if (c == Time.class) {
            return Types.TIME;
        }
        if (c == Byte.class) {
            return Types.TINYINT;
        }
        if (c == Byte[].class) {
            return Types.VARBINARY;
        }
        if (c == Object[].class) {
            return Types.JAVA_OBJECT;
        }
        if (c == Object.class) {
            return Types.JAVA_OBJECT;
        }
        if (c == Array.class) {
            return Types.ARRAY;
        } else {
            return Types.OTHER;
        }
    }
}
