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

package com.aliyun.fastmodel.core.tree.util;

import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeParameter;
import com.aliyun.fastmodel.core.tree.datatype.RowDataType;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * convert
 *
 * @author panguanjing
 * @date 2021/4/1
 */
public class DataTypeUtilTest {

    @Test
    public void testConvert() {
        BaseDataType baseDataType = DataTypeUtil.simpleType(DataTypeEnums.DATE, new DataTypeParameter[0]);
        BaseDataType convert = DataTypeUtil.convert(baseDataType, DataTypeEnums.DATETIME);
        assertEquals(convert.getTypeName(), DataTypeEnums.DATETIME);
    }

    @Test
    public void testConvertException() {
        BaseDataType convert = DataTypeUtil.convert(new RowDataType(ImmutableList.of()), DataTypeEnums.DATE);
        assertEquals(convert.getClass(), RowDataType.class);
    }
}