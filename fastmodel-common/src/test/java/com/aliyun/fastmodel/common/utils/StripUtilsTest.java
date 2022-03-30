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

package com.aliyun.fastmodel.common.utils;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author panguanjing
 * @date 2020/9/4
 */
public class StripUtilsTest {

    @Test
    public void testStrip() {
        String result = StripUtils.strip("'hello'");
        assertEquals("hello", result);
        result = StripUtils.strip("\"hello\"");
        assertEquals("hello", result);
    }

    @Test
    public void testStripWithSingleQuote() {
        String strip = "\"abc='test'\"";
        String strip1 = StripUtils.strip(strip);
        assertEquals(strip1, "abc='test'");
    }

    @Test
    public void testStripAppendSemicolon() {
        String s = StripUtils.appendSemicolon("create table a (b bigint comment 'comment')");
        assertEquals(s, "create table a (b bigint comment 'comment');");
    }

    @Test
    public void testRemoveNewLine() {
        String s = StripUtils.removeEmptyLine("user_id\nuser_name");
        assertEquals(s, "user_id\n"
            + "user_name");

        String s1 = StripUtils.removeEmptyLine("user_id\r\n");
        assertEquals(s1, "user_id");

        s1 = StripUtils.removeEmptyLine("user_id\n");
        assertEquals(s1, "user_id");

        s1 = StripUtils.removeEmptyLine("user_id\n\n\nuser_name\n");
        assertEquals(s1, "user_id\nuser_name");

        s1 = StripUtils.removeEmptyLine("user_id\r\n\r\n\nuser_name\n");
        assertEquals(s1, "user_id\r\nuser_name");
    }

    @Test
    public void testBlankLine() {
        String text = "line 1\n\nline 3\n\n\nline 5";
        String adjusted = text.replaceAll("(?m)^[ \t]*\r?\n", "");
        assertEquals(adjusted, "line 1\n"
            + "line 3\n"
            + "line 5");
    }
}

//Generated with love by TestMe :) Please report issues and submit feature requests at: http://weirddev
// .com/forum#!/testme