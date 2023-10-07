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

package com.aliyun.fastmodel.driver.model;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/1/27
 */
public class DriverUtilTest {

    @Test
    public void testSql() {
        assertTrue(DriverUtil.isSelect("select * from table"));
        assertFalse(DriverUtil.isSelect("update a set b = c"));
        assertFalse(DriverUtil.isSelect("create table a.b"));
        assertTrue(DriverUtil.isSelect("show tables;"));
        assertTrue(DriverUtil.isSelect("describe a.b"));
        assertFalse(DriverUtil.isSelect("alter table a.b"));
        assertFalse(DriverUtil.isSelect("drop table a.b"));
        assertTrue(DriverUtil.isSelect("call a()"));
    }

    @Test
    public void testSqlNewLine() {
        boolean select = DriverUtil.isSelect("show\ntables");
        assertFalse(select);
    }
}