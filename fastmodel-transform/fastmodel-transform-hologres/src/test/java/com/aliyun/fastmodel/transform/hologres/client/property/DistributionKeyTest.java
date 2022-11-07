/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.hologres.client.property;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2022/6/7
 */
public class DistributionKeyTest {
    @Test
    public void testClient() {
        DistributionKey distributionKeyProperty = new DistributionKey();
        assertEquals(distributionKeyProperty.getKey(), "distribution_key");
    }

    @Test
    public void testToValue() {
        DistributionKey distributionKey = new DistributionKey();
        distributionKey.setValueString("c1,c2");
        assertEquals(distributionKey.getValue().size(), 2);
        assertEquals(distributionKey.getValue().get(0), "c1");
    }
}