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

package com.aliyun.fastmodel.transform.oracle.datatype;

import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.datatype.NumericParameter;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/8/18
 */
public class Oracle2FmlDataTypeConverterTest {
    Oracle2FmlDataTypeConverter dataTypeConverter = new Oracle2FmlDataTypeConverter();

    @Test
    public void convert() {
        BaseDataType varchar2 = dataTypeConverter.convert(
            DataTypeUtil.simpleType("VARCHAR2", ImmutableList.of(new NumericParameter("10"))));
        assertEquals(varchar2, DataTypeUtil.simpleType("VARCHAR", ImmutableList.of(new NumericParameter("10"))));
    }

    @Test
    public void testInterval() {
        BaseDataType baseDataType = dataTypeConverter.convert(
            DataTypeUtil.simpleType("INTERVAL YEAR", ImmutableList.of())
        );
        assertEquals(baseDataType, DataTypeUtil.simpleType("VARCHAR", ImmutableList.of(new NumericParameter("30"))));
    }
}