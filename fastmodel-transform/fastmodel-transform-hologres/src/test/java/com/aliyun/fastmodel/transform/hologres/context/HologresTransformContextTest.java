/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.hologres.context;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * HologresTransformContextTest
 *
 * @author panguanjing
 * @date 2022/7/2
 */
public class HologresTransformContextTest {

    @Test
    public void getTimeToLiveInSeconds() {
        HologresTransformContext hologresTransformContext = HologresTransformContext
            .builder().timeToLiveInSeconds(1L)
            .build();
        Long timeToLiveInSeconds = hologresTransformContext.getTimeToLiveInSeconds();
        assertEquals(timeToLiveInSeconds, new Long(1L));
    }
}