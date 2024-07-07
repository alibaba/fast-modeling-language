/*
 * Copyright [2024] [name of copyright owner]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.fastmodel.transform.adbmysql.parser.tree;

import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import com.aliyun.fastmodel.transform.api.datatype.simple.ISimpleDataTypeName;
import com.aliyun.fastmodel.transform.api.datatype.simple.SimpleDataTypeName;
import org.apache.commons.lang3.StringUtils;

/**
 * adb mysql dataTypeName
 * <a href="https://help.aliyun.com/zh/analyticdb-for-mysql/developer-reference/basic-data-types?spm=a2c4g.11186623.0.0.1e887967chIFev">...</a>
 *
 * @author panguanjing
 * @date 2024/3/24
 */
public enum AdbMysqlDataTypeName implements ISimpleDataTypeName {
    BOOLEAN("BOOLEAN", SimpleDataTypeName.BOOLEAN),

    TINYINT("TINYINT", SimpleDataTypeName.NUMBER),

    SMALLINT("SMALLINT", SimpleDataTypeName.NUMBER),

    INT("INT", SimpleDataTypeName.NUMBER),

    INTEGER("INTEGER", SimpleDataTypeName.NUMBER),

    BIGINT("BIGINT", SimpleDataTypeName.NUMBER),

    FLOAT("FLOAT", SimpleDataTypeName.NUMBER),

    DOUBLE("DOUBLE", SimpleDataTypeName.NUMBER),

    DECIMAL("DECIMAL", SimpleDataTypeName.NUMBER, Dimension.TWO),

    NUMERIC("NUMERIC", SimpleDataTypeName.NUMBER),

    VARCHAR("VARCHAR", SimpleDataTypeName.STRING, Dimension.ONE),

    BINARY("BINARY", SimpleDataTypeName.STRING),

    DATE("DATE", SimpleDataTypeName.DATE),

    TIME("TIME", SimpleDataTypeName.DATE),

    DATETIME("DATETIME", SimpleDataTypeName.DATE),

    TIMESTAMP("TIMESTAMP", SimpleDataTypeName.DATE),

    POINT("POINT", SimpleDataTypeName.STRING),

    ARRAY("ARRAY", SimpleDataTypeName.STRING, Dimension.MULTIPLE),
    MAP("MAP", SimpleDataTypeName.STRING, Dimension.MULTIPLE),

    JSON("JSON", SimpleDataTypeName.STRING, Dimension.ZERO);

    private final String value;

    private final SimpleDataTypeName simpleDataTypeName;

    private final Dimension dimension;

    /**
     * multi prefix
     */
    public static final String MULTI_PREFIX = "<";

    AdbMysqlDataTypeName(String value, SimpleDataTypeName simpleDataTypeName) {
        this(value, simpleDataTypeName, Dimension.ZERO);
    }

    AdbMysqlDataTypeName(String value, SimpleDataTypeName simpleDataTypeName, Dimension dimension) {
        this.value = value;
        this.simpleDataTypeName = simpleDataTypeName;
        this.dimension = dimension;
    }

    @Override
    public String getName() {
        return value;
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

    /**
     * get by value
     *
     * @param value
     * @return
     */
    public static IDataTypeName getByValue(String value) {
        String v = value;
        if (v.indexOf(MULTI_PREFIX) > 0) {
            v = v.substring(0, v.indexOf(MULTI_PREFIX)).trim();
        }
        AdbMysqlDataTypeName[] dataTypeNames = AdbMysqlDataTypeName.values();
        for (AdbMysqlDataTypeName s : dataTypeNames) {
            if (StringUtils.equalsIgnoreCase(s.getValue(), v)) {
                return s;
            }
        }
        throw new IllegalArgumentException("not support the dataType with value:" + value);
    }
}
