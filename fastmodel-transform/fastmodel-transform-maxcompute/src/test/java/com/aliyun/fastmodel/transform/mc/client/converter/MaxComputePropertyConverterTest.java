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

package com.aliyun.fastmodel.transform.mc.client.converter;

import java.util.Map;
import java.util.function.Function;

import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.api.client.dto.property.StringProperty;
import com.aliyun.fastmodel.transform.mc.format.MaxComputePropertyKey;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2022/8/5
 */
public class MaxComputePropertyConverterTest {
    MaxComputePropertyConverter maxComputePropertyConverter = new MaxComputePropertyConverter();

    @Test
    public void initFunctionMap() {
        Map<String, Function<String, BaseClientProperty>> functionMap = maxComputePropertyConverter.getFunctionMap();
        assertEquals(7, functionMap.size());
        Function<String, BaseClientProperty> stringBaseClientPropertyFunction = functionMap.get(MaxComputePropertyKey.LIFE_CYCLE.getValue());
        assertNotNull(stringBaseClientPropertyFunction);
    }

    @Test
    public void testGetTransactional() {
        Function<String, BaseClientProperty> function = maxComputePropertyConverter.getFunctionMap().get(
            MaxComputePropertyKey.TRANSACTIONAL.getValue());
        assertNotNull(function);
    }

    @Test
    public void testGetTableFormatVersion() {
        Function<String, BaseClientProperty> function = maxComputePropertyConverter.getFunctionMap().get(
            MaxComputePropertyKey.TABLE_FORMAT_VERSION.getValue());
        assertNotNull(function);

        BaseClientProperty property = function.apply("2");
        assertNotNull(property);
        assertTrue(property instanceof StringProperty);
        assertEquals("table.format.version", property.getKey());
        assertEquals("2", property.valueString());
    }
}
