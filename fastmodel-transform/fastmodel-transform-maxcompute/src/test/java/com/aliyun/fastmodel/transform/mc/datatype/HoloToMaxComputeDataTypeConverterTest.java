/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
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

import static org.junit.Assert.*;

/**
 * HoloToMaxComputeDataTypeConverterTest
 *
 * @author panguanjing
 * @date 2022/8/15
 */
public class HoloToMaxComputeDataTypeConverterTest {

    HoloToMaxComputeDataTypeConverter holoToMaxComputeDataTypeConverter = new HoloToMaxComputeDataTypeConverter();

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