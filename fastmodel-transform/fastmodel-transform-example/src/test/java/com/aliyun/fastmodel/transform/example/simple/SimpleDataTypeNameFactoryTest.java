/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.example.simple;

import com.aliyun.fastmodel.transform.api.datatype.simple.SimpleDataTypeName;
import com.aliyun.fastmodel.transform.api.datatype.simple.SimpleDataTypeNameFactory;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * SimpleDataTypeNameFactoryTest
 *
 * @author panguanjing
 * @date 2022/11/7
 */
public class SimpleDataTypeNameFactoryTest {
    @Test
    public void testDataTypeName() {
        SimpleDataTypeNameFactory instance = SimpleDataTypeNameFactory.getInstance();
        SimpleDataTypeName bigint = instance.convert("bigint", SimpleDataTypeName.STRING);
        assertEquals(bigint, SimpleDataTypeName.NUMBER);
        SimpleDataTypeName inet = instance.convert("inet", SimpleDataTypeName.BOOLEAN);
        assertEquals(inet, SimpleDataTypeName.STRING);
        inet = instance.convert("uuid", SimpleDataTypeName.BOOLEAN);
        assertEquals(inet, SimpleDataTypeName.STRING);
        inet = instance.convert("uuid_11", SimpleDataTypeName.BOOLEAN);
        assertEquals(inet, SimpleDataTypeName.BOOLEAN);
    }
}
