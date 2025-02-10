/*
 * Copyright [2024] [name of copyright owner]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.google.common.base.CharMatcher;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2024/9/30
 */
public class ConsoleTest {
    @Test
    public void testTrial() {
        String input = "dump | show full etl from test_bu.dim_table;";
        // 使用 CharMatcher 来分割字符串
        String[] parts = CharMatcher.whitespace().trimAndCollapseFrom(input, ' ').split("\\|");
        // 获取第一个部分，去掉额外的空白字符
        String result = CharMatcher.whitespace().trimFrom(parts[0]);
        assertEquals("dump", result);
        assertEquals("show full etl from test_bu.dim_table;", CharMatcher.whitespace().trimFrom(parts[1]));
    }
}