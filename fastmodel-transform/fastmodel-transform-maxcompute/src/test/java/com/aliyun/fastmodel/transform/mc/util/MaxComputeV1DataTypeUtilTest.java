/*
 * Copyright 2021 Alibaba Group Holding Ltd.
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

package com.aliyun.fastmodel.transform.mc.util;

import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import com.aliyun.fastmodel.transform.mc.parser.tree.datatype.MaxComputeDataTypeName;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * 类型转换
 *
 * @author panguanjing
 * @date 2021/6/1
 */
public class MaxComputeV1DataTypeUtilTest {

    @Test
    public void convert() {
        IDataTypeName convert = MaxComputeV1DataTypeUtil.convertIfNecessary(DataTypeEnums.VARCHAR);
        assertEquals(convert, DataTypeEnums.STRING);
    }

    @Test
    public void convert_toBigint() {
        IDataTypeName convert = MaxComputeV1DataTypeUtil.convertIfNecessary(DataTypeEnums.BIGINT);
        assertNull(convert);
    }

    @Test
    public void convert_toString() {
        IDataTypeName convert = MaxComputeV1DataTypeUtil.convertIfNecessary(DataTypeEnums.TEXT);
        assertEquals(convert, DataTypeEnums.STRING);
    }

    @Test
    public void convertsSharedAndNativeTypeNames() {
        String[] sources = {"VARCHAR", "CHAR", "TINYINT", "SMALLINT", "INT", "FLOAT", "DATE", "TIMESTAMP"};
        DataTypeEnums[] targets = {DataTypeEnums.STRING, DataTypeEnums.STRING, DataTypeEnums.BIGINT,
            DataTypeEnums.BIGINT, DataTypeEnums.BIGINT, DataTypeEnums.DOUBLE,
            DataTypeEnums.DATETIME, DataTypeEnums.DATETIME};
        for (int index = 0; index < sources.length; index++) {
            assertEquals(targets[index], MaxComputeV1DataTypeUtil.convertIfNecessary(
                DataTypeEnums.valueOf(sources[index])));
            assertEquals(targets[index], MaxComputeV1DataTypeUtil.convertIfNecessary(
                MaxComputeDataTypeName.valueOf(sources[index])));
        }
        assertNull(MaxComputeV1DataTypeUtil.convertIfNecessary(MaxComputeDataTypeName.BIGINT));
        assertNull(MaxComputeV1DataTypeUtil.convertIfNecessary(null));
    }
}
