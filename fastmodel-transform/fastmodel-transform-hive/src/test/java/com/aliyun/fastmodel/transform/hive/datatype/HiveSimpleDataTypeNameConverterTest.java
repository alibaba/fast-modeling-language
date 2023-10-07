/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.hive.datatype;

import com.aliyun.fastmodel.transform.api.datatype.simple.SimpleDataTypeName;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2022/11/7
 */
public class HiveSimpleDataTypeNameConverterTest {

    @Test
    public void convert() {
        HiveSimpleDataTypeNameConverter hiveSimpleDataTypeNameConverter = new HiveSimpleDataTypeNameConverter();
        SimpleDataTypeName bigint = hiveSimpleDataTypeNameConverter.convert("bigint");
        assertEquals(bigint, SimpleDataTypeName.NUMBER);
        bigint = hiveSimpleDataTypeNameConverter.convert("string");
        assertEquals(bigint, SimpleDataTypeName.STRING);
        bigint = hiveSimpleDataTypeNameConverter.convert("date");
        assertEquals(bigint, SimpleDataTypeName.DATE);
        bigint = hiveSimpleDataTypeNameConverter.convert("map");
        assertEquals(bigint, SimpleDataTypeName.STRING);
        bigint = hiveSimpleDataTypeNameConverter.convert("boolean");
        assertEquals(bigint, SimpleDataTypeName.BOOLEAN);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConvertException() {
        HiveSimpleDataTypeNameConverter simpleDataTypeNameConverter = new HiveSimpleDataTypeNameConverter();
        simpleDataTypeNameConverter.convert("notExist");
    }
}