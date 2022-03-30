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
public class SubjectTest extends BaseTest {

    @Test
    public void testCreate() {
        assertParse("create subject a alias 'a' comment 'comment'", "CREATE SUBJECT a ALIAS 'a' COMMENT 'comment'");

    }

    @Test
    public void testSetComment() {
        assertParse("alter subject a set comment 'comment'", "ALTER SUBJECT a SET COMMENT 'comment'");
    }

    @Test
    public void testSetAlias() {
        assertParse("alter subject a set alias 'alias'", "ALTER SUBJECT a SET ALIAS 'alias'");
    }

    @Test
    public void testSetProperties() {
        assertParse("alter subject a set properties ('k'='b')", "ALTER SUBJECT a SET PROPERTIES ('k'='b')");
    }

    @Test
    public void testUnSetProperties() {
        assertParse("alter subject a unset properties('a','b')", "ALTER SUBJECT a UNSET PROPERTIES ('a','b')");
    }

    @Test
    public void testDrop() {
        assertParse("drop subject a", "DROP SUBJECT a");
    }
}
