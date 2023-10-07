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

package com.aliyun.aliyun.transform.zen.compare;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.aliyun.aliyun.transform.zen.ZenTransformer;
import com.aliyun.aliyun.transform.zen.parser.ZenParserImpl;
import com.aliyun.fastmodel.core.tree.ListNode;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/9/26
 */
public class ZenNodeReverseTest {
    ZenParserImpl zenParser = new ZenParserImpl();
    ZenTransformer zenTransformer = new ZenTransformer();

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void testTransform() throws IOException {
        String name = "/issue/issue.txt";
        List<String> strings = transformTest1(name);
        assertTrue(strings.isEmpty());
    }

    @Test
    public void testStruct() {
        String value = "a array<struct<id:tring>> 'comment'";
        DialectNode dialectNode = new DialectNode(value);
        ListNode reverse = (ListNode)zenTransformer.reverse(dialectNode);
        List<ColumnDefinition> list = (List<ColumnDefinition>)reverse.getChildren();
        assertEquals(1, list.size());
    }

    private List<String> transformTest1(String name) throws IOException {
        String zenText = IOUtils.toString(ZenNodeReverseTest.class.getResourceAsStream(name));
        DialectNode dialectNode = new DialectNode(zenText);
        ListNode reverse = (ListNode)zenTransformer.reverse(dialectNode);
        List<ColumnDefinition> list = (List<ColumnDefinition>)reverse.getChildren();
        Map<String, ColumnDefinition> maps = new HashMap<>();
        for (ColumnDefinition c : list) {
            maps.put(c.getColName().getValue(), c);
        }
        List<String> notContains = new ArrayList<>();
        String[] listString = zenText.split("\n");
        for (String s : listString) {
            if (s.startsWith("--")) {
                continue;
            }
            String[] s1 = StringUtils.split(s, " ");
            String s2 = s1[0];
            if (!maps.containsKey(s2)) {
                notContains.add(s2);
            }
        }
        return notContains;
    }
}
