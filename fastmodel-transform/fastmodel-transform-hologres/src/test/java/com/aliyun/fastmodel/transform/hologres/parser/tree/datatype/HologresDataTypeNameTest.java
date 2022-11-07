/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.hologres.parser.tree.datatype;

import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * HologresDataTypeNameTest
 *
 * @author panguanjing
 * @date 2022/6/21
 */
public class HologresDataTypeNameTest {

    @Test
    public void getByValue() {
        IDataTypeName text = HologresDataTypeName.getByValue("text");
        assertEquals(text, HologresDataTypeName.TEXT);
    }

    @Test
    public void getAlias() {
        HologresDataTypeName timestamptz = HologresDataTypeName.TIMESTAMPTZ;
        assertEquals(timestamptz.getAlias(), "TIMESTAMPTZ");
    }

    @Test
    public void testGetAlias() {
        IDataTypeName bpchar = HologresDataTypeName.getByValue("bpchar");
        assertEquals(bpchar, HologresDataTypeName.CHAR);
    }

    @Test
    public void testVarchar() {
        IDataTypeName varchar = HologresDataTypeName.getByValue("varchar");
        assertEquals(varchar, HologresDataTypeName.VARCHAR);
    }

    @Test
    public void testText() {
        IDataTypeName varchar = HologresDataTypeName.getByValue("text");
        assertEquals(varchar, HologresDataTypeName.TEXT);
    }

    @Test
    public void testGetMoney() {
        IDataTypeName money = HologresDataTypeName.getByValue("money");
        assertEquals(money, HologresDataTypeName.MONEY);
    }

    @Test
    public void testSmallInt() {
        IDataTypeName money = HologresDataTypeName.getByValue("smallint");
        assertEquals(money, HologresDataTypeName.SMALLINT);
    }

    @Test
    public void testInt2() {
        IDataTypeName money = HologresDataTypeName.getByValue("int2");
        assertEquals(money, HologresDataTypeName.SMALLINT);
    }

    @Test
    public void testInt4() {
        IDataTypeName money = HologresDataTypeName.getByValue("int4");
        assertEquals(money, HologresDataTypeName.INTEGER);
    }

    @Test
    public void testInt() {
        IDataTypeName money = HologresDataTypeName.getByValue("int");
        assertEquals(money, HologresDataTypeName.INTEGER2);
    }

    @Test
    public void testNumeric() {
        assertEquals(HologresDataTypeName.getByValue("numeric"), HologresDataTypeName.DECIMAL);
    }

    @Test
    public void testGetDataType() {
        IDataTypeName byValue = HologresDataTypeName.getByValue("BIGINT[]");
        assertNotNull(byValue);
    }
}