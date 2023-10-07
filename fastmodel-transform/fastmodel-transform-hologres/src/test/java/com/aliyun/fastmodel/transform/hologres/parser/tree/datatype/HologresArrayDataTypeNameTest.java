/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.hologres.parser.tree.datatype;

import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2022/6/16
 */
public class HologresArrayDataTypeNameTest {
    HologresArrayDataTypeName hologresArrayDataTypeName;

    @Before
    public void setUp() {
        hologresArrayDataTypeName = new HologresArrayDataTypeName(HologresDataTypeName.TEXT);
    }

    @Test
    public void testGetValue() {
        String value = hologresArrayDataTypeName.getValue();
        assertEquals(value, "TEXT[]");
    }

    @Test
    public void testGetName() {
        String name = hologresArrayDataTypeName.getName();
        assertEquals(name, "TEXT_ARRAY");
    }

    @Test
    public void testGetText() {
        IDataTypeName int4 = HologresDataTypeName.getByValue("int4");
        assertEquals(int4, HologresDataTypeName.INTEGER);
    }

    @Test
    public void testGetInt8() {
        IDataTypeName int8 = HologresDataTypeName.getByValue("int8");
        assertEquals(int8, HologresDataTypeName.BIGINT);
    }

    @Test
    public void testGetByValue() {
        HologresArrayDataTypeName byValue = HologresArrayDataTypeName.getByValue("int4[]");
        assertEquals(byValue.getSource(), HologresDataTypeName.INTEGER);
    }

    @Test
    public void testGetByValueText() {
        HologresArrayDataTypeName byValue = HologresArrayDataTypeName.getByValue("text[]");
        assertEquals(byValue.getSource(), HologresDataTypeName.TEXT);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIllegalArgument() {
        HologresArrayDataTypeName byValue = HologresArrayDataTypeName.getByValue("text");
        assertEquals(byValue.getSource(), HologresDataTypeName.TEXT);
    }
}

//Generated with love by TestMe :) Please report issues and submit feature requests at: http://weirddev.com/forum#!/testme