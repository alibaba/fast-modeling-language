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

package com.aliyun.fastmodel.transform.mc.datatype.simple;

import com.aliyun.fastmodel.transform.api.datatype.simple.SimpleDataTypeName;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2022/11/7
 */
public class MaxComputeSimpleDataTypeNameConverterTest {

    @Test
    public void testConvert() {
        MaxComputeSimpleDataTypeNameConverter maxComputeSimpleDataTypeNameConverter = new MaxComputeSimpleDataTypeNameConverter();
        SimpleDataTypeName bigint = maxComputeSimpleDataTypeNameConverter.convert("bigint");
        assertEquals(bigint, SimpleDataTypeName.NUMBER);
        SimpleDataTypeName notExist = maxComputeSimpleDataTypeNameConverter.convert("date");
        assertEquals(notExist, SimpleDataTypeName.DATE);
        notExist = maxComputeSimpleDataTypeNameConverter.convert("varchar");
        assertEquals(notExist, SimpleDataTypeName.STRING);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testException() {
        SimpleDataTypeName notExist = new MaxComputeSimpleDataTypeNameConverter().convert("notExist");
        assertEquals(notExist, SimpleDataTypeName.STRING);
    }
}