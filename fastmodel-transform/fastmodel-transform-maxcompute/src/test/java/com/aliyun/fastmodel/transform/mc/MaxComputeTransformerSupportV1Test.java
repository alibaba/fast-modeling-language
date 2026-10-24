/*
 * Copyright 2022 Alibaba Group Holding Ltd.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
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