/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.hive.parser.tree.datatype;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * HiveDataTypeNameTest
 *
 * @author panguanjing
 * @date 2022/8/8
 */
public class HiveDataTypeNameTest {

    @Test
    public void getByValue() {
        HiveDataTypeName string = HiveDataTypeName.getByValue("string");
        assertEquals(string, HiveDataTypeName.STRING);
    }
}