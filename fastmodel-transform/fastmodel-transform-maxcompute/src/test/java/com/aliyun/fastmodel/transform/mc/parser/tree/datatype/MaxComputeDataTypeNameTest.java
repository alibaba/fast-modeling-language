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

package com.aliyun.fastmodel.transform.mc.parser.tree.datatype;

import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName.Dimension;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2022/8/7
 */
public class MaxComputeDataTypeNameTest {

    @Test
    public void getByValue() {
        MaxComputeDataTypeName string = MaxComputeDataTypeName.getByValue("string");
        assertEquals(string.getDimension(), Dimension.ZERO);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetNotExist() {
        MaxComputeDataTypeName.getByValue("text");
    }

    @Test
    public void testComplex() {
        MaxComputeDataTypeName byValue = MaxComputeDataTypeName.getByValue("array<string>");
        assertEquals(byValue, MaxComputeDataTypeName.ARRAY);
    }

    @Test
    public void testMap() {
        MaxComputeDataTypeName byValue = MaxComputeDataTypeName.getByValue("map<string, bigint>");
        assertEquals(byValue, MaxComputeDataTypeName.MAP);
    }

    @Test
    public void testJson() {
        MaxComputeDataTypeName byValue = MaxComputeDataTypeName.getByValue("json");
        assertEquals(byValue, MaxComputeDataTypeName.JSON);
    }

    @Test
    public void testTimestampNtz() {
        MaxComputeDataTypeName byValue = MaxComputeDataTypeName.getByValue("timestamp_ntz");
        assertEquals(byValue, MaxComputeDataTypeName.TIMESTAMP_NTZ);
    }

    @Test
    public void testVector() {
        MaxComputeDataTypeName byValue = MaxComputeDataTypeName.getByValue("vector");
        assertEquals(byValue, MaxComputeDataTypeName.VECTOR);
        assertEquals(byValue.getDimension(), Dimension.TWO);
    }
}