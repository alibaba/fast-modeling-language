/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.hologres.parser.tree.datatype;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * HologresGenericDataTypeTest
 *
 * @author panguanjing
 * @date 2022/6/21
 */
public class HologresGenericDataTypeTest {

    @Test
    public void testGetDataTypeName() {
        HologresGenericDataType hologresGenericDataType = new HologresGenericDataType(
            "text", null
        );
        assertEquals(hologresGenericDataType.getTypeName(), HologresDataTypeName.TEXT);
    }

    @Test
    public void getGetByAlias() {
        HologresGenericDataType hologresGenericDataType = new HologresGenericDataType(
            "int4", null
        );
        assertEquals(hologresGenericDataType.getTypeName(), HologresDataTypeName.INTEGER);
    }
}