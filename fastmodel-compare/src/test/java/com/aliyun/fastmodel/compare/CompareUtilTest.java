/*
 * Copyright 2021-2022 Alibaba Group Holding Ltd.
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

package com.aliyun.fastmodel.compare;

import java.util.ArrayList;

import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/9/18
 */
public class CompareUtilTest {

    @Test
    public void testCompareUtil() {
        boolean changeCollection = CompareUtil.isChangeCollection(null, null);
        assertFalse(changeCollection);
        boolean changeCollection1 = CompareUtil.isChangeCollection(null, new ArrayList<>());
        assertFalse(changeCollection1);
        changeCollection1 = CompareUtil.isChangeCollection(new ArrayList<>(), new ArrayList<>());
        assertFalse(changeCollection1);
        changeCollection1 = CompareUtil.isChangeCollection(new ArrayList<>(), null);
        assertFalse(changeCollection1);
        changeCollection1 = CompareUtil.isChangeCollection(Lists.newArrayList("a"), Lists.newArrayList("a"));
        assertFalse(changeCollection1);
        changeCollection1 = CompareUtil.isChangeCollection(Lists.newArrayList("b"), Lists.newArrayList("a"));
        assertTrue(changeCollection1);
    }
}