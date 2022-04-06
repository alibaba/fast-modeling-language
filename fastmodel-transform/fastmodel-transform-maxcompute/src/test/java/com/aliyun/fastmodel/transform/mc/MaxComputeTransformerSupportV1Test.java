/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.mc;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * MaxComputeTransformerSupportV1Test
 *
 * @author panguanjing
 * @date 2022/7/12
 */
public class MaxComputeTransformerSupportV1Test {

    @Test
    public void transform() {
        MaxComputeTransformerSupportV1 supportV1 = new MaxComputeTransformerSupportV1();
        BaseStatement reverse = supportV1.reverse(new DialectNode("select * from public.test;"));
        assertNotNull(reverse);
    }

    @Test
    public void testReverseNoEnd() {
        MaxComputeTransformerSupportV1 supportV1 = new MaxComputeTransformerSupportV1();
        BaseStatement reverse = supportV1.reverse(new DialectNode("select * from public.test"));
        assertNotNull(reverse);
    }
}