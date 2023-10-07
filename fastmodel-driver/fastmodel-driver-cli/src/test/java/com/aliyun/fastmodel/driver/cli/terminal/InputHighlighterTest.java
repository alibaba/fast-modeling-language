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

package com.aliyun.fastmodel.driver.cli.terminal;

import org.jline.reader.LineReader;
import org.jline.utils.AttributedString;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.assertTrue;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/1/26
 */
@RunWith(MockitoJUnitRunner.class)
public class InputHighlighterTest {

    InputHighlighter highlighter;

    @Mock
    LineReader lineReader;

    @Before
    public void setUp() throws Exception {
        highlighter = new InputHighlighter();
    }

    @Test
    public void testHigh() {
        AttributedString value = highlighter.highlight(lineReader, "show tables");
        int length = value.length();
        assertTrue(length > 0);
    }

    @Test
    public void testComment() {
        AttributedString v = highlighter.highlight(lineReader, "create dim table a.b comment 'abc'");
        int length = v.length();
        assertTrue(length > 0);
    }
}