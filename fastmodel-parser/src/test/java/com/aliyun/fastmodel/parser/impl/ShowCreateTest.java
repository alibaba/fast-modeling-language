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

package com.aliyun.fastmodel.parser.impl;

import com.aliyun.fastmodel.core.tree.statement.constants.ShowType;
import com.aliyun.fastmodel.core.tree.statement.showcreate.ShowCreate;
import com.aliyun.fastmodel.parser.BaseTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * show Test
 *
 * @author panguanjing
 * @date 2020/12/2
 */
public class ShowCreateTest extends BaseTest {

    @Test
    public void testShowCreateTable() {
        String template = "show create %s dingtalk.u1";
        ShowType[] showTypes = ShowType.values();
        for (ShowType t : showTypes) {
            String format = String.format(template, t.getCode());
            ShowCreate parse = parse(format, ShowCreate.class);
            assertEquals(parse.getShowType(), t);
        }
    }

    @Test
    public void testOutput() {
        String template = "show create %s dingtalk.u1 output=maxcompute";
        ShowType[] showTypes = ShowType.values();
        for (ShowType t : showTypes) {
            String format = String.format(template, t.getCode());
            ShowCreate parse = parse(format, ShowCreate.class);
            assertEquals(parse.getShowType(), t);
            assertNotNull(parse.getOutput());
            assertEquals(parse.getOutput().getDialect(), "maxcompute");
        }
    }
}

