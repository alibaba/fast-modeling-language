package com.aliyun.fastmodel.transform.mc.datatype;

import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.datatype.NumericParameter;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Sqlite2MaxComputeDataTypeConverterTest
 *
 * @author panguanjing
 * @date 2023/8/29
 */
public class Sqlite2MaxComputeDataTypeConverterTest {
    Sqlite2MaxComputeDataTypeConverter sqlite2MaxComputeDataTypeConverter = new Sqlite2MaxComputeDataTypeConverter();

    @Test
    public void testConverter() {
        BaseDataType dataType = DataTypeUtil.simpleType("TEXT", Lists.newArrayList());
        BaseDataType convert = sqlite2MaxComputeDataTypeConverter.convert(dataType);
        assertEquals("STRING", convert.toString());
        dataType = DataTypeUtil.simpleType("varchar", Lists.newArrayList(new NumericParameter("10")));
        convert = sqlite2MaxComputeDataTypeConverter.convert(dataType);
        assertEquals("STRING", convert.toString());
    }
}