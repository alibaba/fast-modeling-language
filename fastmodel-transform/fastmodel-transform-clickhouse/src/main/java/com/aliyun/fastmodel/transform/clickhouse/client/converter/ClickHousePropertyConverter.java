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

package com.aliyun.fastmodel.transform.clickhouse.client.converter;

import java.util.Map;
import java.util.function.Function;

import com.aliyun.fastmodel.transform.api.client.converter.BasePropertyConverter;
import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.api.client.dto.property.StringProperty;
import com.aliyun.fastmodel.transform.clickhouse.format.ClickHousePropertyKey;
import com.google.common.collect.Maps;

/**
 * clickHouse property converter
 *
 * @author panguanjing
 */
public class ClickHousePropertyConverter extends BasePropertyConverter {

    private static final Map<String, Function<String, BaseClientProperty>> FUNCTION_MAP = Maps.newHashMap();

    static {
        for (ClickHousePropertyKey propertyKey : ClickHousePropertyKey.values()) {
            FUNCTION_MAP.put(propertyKey.getValue(), value -> toProperty(propertyKey.getValue(), value));
        }
    }

    private static BaseClientProperty toProperty(String key, String value) {
        StringProperty stringProperty = new StringProperty();
        stringProperty.setKey(key);
        stringProperty.setValueString(value);
        return stringProperty;
    }

    @Override
    public BaseClientProperty create(String name, String value) {
        if (value != null && (value.indexOf(';') >= 0 || value.indexOf('\n') >= 0 || value.indexOf('\r') >= 0
            || value.indexOf('\u0000') >= 0 || value.contains("--") || value.contains("/*") || value.contains("*/"))) {
            throw new IllegalArgumentException("Unsafe ClickHouse property value");
        }
        return super.create(name, value);
    }

    @Override
    protected Map<String, Function<String, BaseClientProperty>> getFunctionMap() {
        return FUNCTION_MAP;
    }
}
