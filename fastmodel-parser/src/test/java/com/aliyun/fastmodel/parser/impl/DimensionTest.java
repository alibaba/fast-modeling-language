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
public class DimensionTest extends BaseTest {

    @Test
    public void testCreate() {
        assertParse(
            "create dimension a (b comment 'comment')",
            "CREATE DIMENSION a\n"
                + "(\n"
                + "   b COMMENT 'comment'\n"
                + ")"
        );
    }

    @Test
    public void testCreateTimePeriod() {
        assertParse(
            "create dimension a (b time_period comment 'comment', c alias 'alias')",
            "CREATE DIMENSION a\n"
                + "(\n"
                + "   b TIME_PERIOD COMMENT 'comment',\n"
                + "   c ALIAS 'alias'\n"
                + ")"
        );
    }

    @Test
    public void testCreateWithPrimary() {
        assertParse("create dimension a (b primary key comment 'comment')",
            "CREATE DIMENSION a\n"
                + "(\n"
                + "   b PRIMARY KEY COMMENT 'comment'\n"
                + ")");
    }

    @Test
    public void testSetComment() {
        assertParse("alter dimension a set comment 'comment'", "ALTER DIMENSION a SET COMMENT 'comment'");
    }

    @Test
    public void testSetAlias() {
        assertParse(
            "alter dimension a set alias 'as'",
            "ALTER DIMENSION a SET ALIAS 'as'"
        );
    }

    @Test
    public void testSetProperties() {
        assertParse("alter dimension a set properties('k'='b')", "ALTER DIMENSION a SET PROPERTIES ('k'='b')");
    }

    @Test
    public void testUnSetProperties() {
        assertParse("alter dimension a unset properties('a','b')", "ALTER DIMENSION a UNSET PROPERTIES ('a','b')");

    }

    @Test
    public void testDrop() {
        assertParse("drop dimension a", "DROP DIMENSION a");
    }

    @Test
    public void testAddattributes() {
        assertParse("alter dimension a add  attributes (a comment 'comment')", "ALTER DIMENSION a ADD ATTRIBUTES \n"
            + "(\n"
            + "   a COMMENT 'comment'\n"
            + ")");
    }

    @Test
    public void testDropattribute() {
        assertParse("alter dimension a drop attribute b", "ALTER DIMENSION a DROP ATTRIBUTE b");
    }

    @Test
    public void testChangeattribute() {
        assertParse("alter dimension a change attribute a b comment 'comment'",
            "ALTER DIMENSION a CHANGE ATTRIBUTE a b COMMENT 'comment'");
    }

    @Test
    public void testCreateDimensionWithSplit() {
        assertParse("create dimension cate_xxx alias '类目' (\n"
            + "    cate_id alias '类目ID' primary key comment '类目ID;couldEnum:fasle',\n"
            + "    cate_name alias '类目名称' comment '类目ID;couldEnum:fasle',\n"
            + "    cate_flag alias '类目等级' comment '类目等级;couldEnum:true'\n"
            + ") with ('extend_name' = 'category');", "CREATE DIMENSION cate_xxx ALIAS '类目'\n"
            + "(\n"
            + "   cate_id   ALIAS '类目ID' PRIMARY KEY COMMENT '类目ID;couldEnum:fasle',\n"
            + "   cate_name ALIAS '类目名称' COMMENT '类目ID;couldEnum:fasle',\n"
            + "   cate_flag ALIAS '类目等级' COMMENT '类目等级;couldEnum:true'\n"
            + ")\n"
            + "WITH('extend_name'='category')");
    }
}
