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

package com.aliyun.fastmodel.transform.postgresql.client.converter;

import com.aliyun.fastmodel.transform.api.client.PropertyConverter;
import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.api.client.dto.property.StringProperty;

/**
 * PostgreSQL 属性转换器
 *
 * @author panguanjing
 * @date 2026/3/31
 */
public class PostgreSQLPropertyConverter implements PropertyConverter {

    @Override
    public BaseClientProperty create(String name, String value) {
        StringProperty stringProperty = new StringProperty();
        stringProperty.setKey(name);
        stringProperty.setValue(value);
        return stringProperty;
    }
}
