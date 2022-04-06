/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.mc.parser.tree.datatype;

import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName.Dimension;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2022/8/7
 */
public class MaxComputeDataTypeNameTest {

    @Test
    public void getByValue() {
        MaxComputeDataTypeName string = MaxComputeDataTypeName.getByValue("string");
        assertEquals(string.getDimension(), Dimension.ZERO);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetNotExist() {
        MaxComputeDataTypeName.getByValue("text");
    }

    @Test
    public void testComplex() {
        MaxComputeDataTypeName byValue = MaxComputeDataTypeName.getByValue("array<string>");
        assertEquals(byValue, MaxComputeDataTypeName.ARRAY);
    }

    @Test
    public void testMap() {
        MaxComputeDataTypeName byValue = MaxComputeDataTypeName.getByValue("map<string, bigint>");
        assertEquals(byValue, MaxComputeDataTypeName.MAP);
    }
}