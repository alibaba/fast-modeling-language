/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.hologres.datatype;

import com.aliyun.fastmodel.transform.api.datatype.simple.SimpleDataTypeName;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2022/11/7
 */
public class HologresSimpleDataTypeNameConverterTest {

    HologresSimpleDataTypeNameConverter hologresSimpleDataTypeNameConverter = new HologresSimpleDataTypeNameConverter();

    @Test
    public void testConvert() {
        SimpleDataTypeName inet = hologresSimpleDataTypeNameConverter.convert("inet");
        assertEquals(inet, SimpleDataTypeName.STRING);

        inet = hologresSimpleDataTypeNameConverter.convert("int4");
        assertEquals(inet, SimpleDataTypeName.NUMBER);
        inet = hologresSimpleDataTypeNameConverter.convert("int[]");
        assertEquals(inet, SimpleDataTypeName.STRING);

        inet = hologresSimpleDataTypeNameConverter.convert("boolean");
        assertEquals(inet, SimpleDataTypeName.BOOLEAN);

    }

    @Test(expected = IllegalArgumentException.class)
    public void testNotExistConvert() {
        SimpleDataTypeName notExist = hologresSimpleDataTypeNameConverter.convert("notExist");
        assertEquals(notExist, SimpleDataTypeName.STRING);
    }
}