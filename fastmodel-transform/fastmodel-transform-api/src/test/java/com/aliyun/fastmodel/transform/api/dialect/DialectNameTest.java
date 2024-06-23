/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.api.dialect;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * DialectNameTest
 *
 * @author panguanjing
 * @date 2022/6/16
 */
public class DialectNameTest {

    @Test
    public void getDialectName() {
        DialectName maxcompute = DialectName.getByCode("maxcompute");
        assertEquals(maxcompute, DialectName.MAXCOMPUTE);

        maxcompute = DialectName.getByCode("Max_compute");
        assertEquals(maxcompute, DialectName.MAXCOMPUTE);
    }

    @Test
    public void testDoris() {
        DialectName dialectName = DialectName.getByCode("doris");
        assertEquals(dialectName, DialectName.DORIS);
    }
}