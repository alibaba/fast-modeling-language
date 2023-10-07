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

import com.aliyun.fastmodel.parser.BaseTest;
import org.junit.Test;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/10/1
 */
public class MarketTest extends BaseTest {

    @Test
    public void testCreate() {
        assertParse("create market a alias 'a' comment 'comment'", "CREATE MARKET a ALIAS 'a' COMMENT 'comment'");
    }

    @Test
    public void testCreate1() {
        assertParse("create market a.b.c alias 'a' comment 'comment'",
            "CREATE MARKET a.b.c ALIAS 'a' COMMENT 'comment'");
    }

    @Test
    public void testSetComment() {
        assertParse("alter market a set comment 'comment'", "ALTER MARKET a SET COMMENT 'comment'");
    }

    @Test
    public void testSetAlias() {
        assertParse("alter market a set alias 'alias'", "ALTER MARKET a SET ALIAS 'alias'");
    }

    @Test
    public void testSetProperties() {
        assertParse("alter market a set properties ('k'='b')", "ALTER MARKET a SET PROPERTIES ('k'='b')");
    }

    @Test
    public void testUnSetProperties() {
        assertParse("alter market a unset properties('a','b')", "ALTER MARKET a UNSET PROPERTIES ('a','b')");
    }

    @Test
    public void testDrop() {
        assertParse("drop market a", "DROP MARKET a");
    }
}
