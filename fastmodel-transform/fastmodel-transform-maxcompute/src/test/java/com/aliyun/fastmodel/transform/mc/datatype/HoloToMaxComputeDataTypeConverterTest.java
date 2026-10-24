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

package com.aliyun.fastmodel.transform.mc.datatype;

import java.util.List;

import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeParameter;
import com.aliyun.fastmodel.core.tree.datatype.GenericDataType;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.mc.parser.tree.datatype.MaxComputeDataTypeName;
import com.aliyun.fastmodel.transform.mc.parser.tree.datatype.MaxComputeGenericDataType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * HoloToMaxComputeDataTypeConverterTest
 *
 * @author panguanjing
 * @date 2022/8/15
 */
public class HoloToMaxComputeDataTypeConverterTest {

    Holo2MaxComputeDataTypeConverter holoToMaxComputeDataTypeConverter = new Holo2MaxComputeDataTypeConverter();

    @Test
    public void convert() {
        BaseDataType convert = holoToMaxComputeDataTypeConverter.convert(new GenericDataType("int4[]"));
        MaxComputeGenericDataType maxComputeGenericDataType = (MaxComputeGenericDataType)convert;
        assertEquals(maxComputeGenericDataType.getTypeName(), MaxComputeDataTypeName.ARRAY);
        List<DataTypeParameter> arguments = maxComputeGenericDataType.getArguments();
        assertEquals(1, arguments.size());
    }

    @Test
    public void getSourceDialect() {
        DialectMeta sourceDialect = holoToMaxComputeDataTypeConverter.getSourceDialect();
        assertEquals(sourceDialect, DialectMeta.DEFAULT_HOLO);
    }

    @Test
    public void getTargetDialect() {
        DialectMeta sourceDialect = holoToMaxComputeDataTypeConverter.getTargetDialect();
        assertEquals(sourceDialect, DialectMeta.DEFAULT_MAX_COMPUTE);
    }
}