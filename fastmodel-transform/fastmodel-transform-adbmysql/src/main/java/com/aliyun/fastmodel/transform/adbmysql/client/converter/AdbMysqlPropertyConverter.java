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

package com.aliyun.fastmodel.transform.adbmysql.client.converter;

import java.util.Map;
import java.util.function.Function;

import com.aliyun.fastmodel.transform.api.client.converter.BasePropertyConverter;
import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.google.common.collect.Maps;

/**
 * AdbMysqlPropertyConverter
 *
 * @author panguanjing
 * @date 2024/3/24
 */
public class AdbMysqlPropertyConverter extends BasePropertyConverter {

    private static Map<String, Function<String, BaseClientProperty>> functionMap = Maps.newHashMap();

    @Override
    protected Map<String, Function<String, BaseClientProperty>> getFunctionMap() {
        return functionMap;
    }
}
