/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.hologres.client.property;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2022/6/21
 */
public class SegmentKeyTest {

    @Test
    public void valueString() {
        SegmentKey segmentKeys = new SegmentKey();
        segmentKeys.setValueString("keys,key2");
        String list = segmentKeys.valueString();
        assertEquals("keys,key2", list);
    }

}