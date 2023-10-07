/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.api.datatype.simple;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2022/11/7
 */
public class SimpleDataTypeNameFactoryTest {

    @Test
    public void convert() {
        SimpleDataTypeNameFactory instance = SimpleDataTypeNameFactory.getInstance();
        SimpleDataTypeName anInt = instance.convert("int", SimpleDataTypeName.DATE);
        assertEquals(anInt, SimpleDataTypeName.DATE);
    }
}