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

import java.sql.Types;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/1/4
 */
public class DriverDataTypeTest {

    @Test
    public void testDriverDataType() {
        int typeIdForObject = DriverDataType.getTypeIdForObject(Long.class);
        assertEquals(typeIdForObject, Types.BIGINT);
    }

    @Test
    public void testGetClassForTypeId() {
        Class<?> classForTypeId = DriverDataType.getClassForTypeId(Types.VARCHAR);
        assertEquals(String.class, classForTypeId);
        classForTypeId = DriverDataType.getClassForTypeId(Types.BOOLEAN);
        assertEquals(Boolean.class, classForTypeId);
    }

}