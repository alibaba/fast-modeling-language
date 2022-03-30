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

package com.aliyun.aliyun.transform.zen;

import java.util.List;

import com.aliyun.aliyun.transform.zen.converter.ZenNodeColumnDefinitionConverter;
import com.aliyun.aliyun.transform.zen.parser.ZenParser;
import com.aliyun.aliyun.transform.zen.parser.ZenParserImpl;
import com.aliyun.aliyun.transform.zen.parser.tree.BaseZenNode;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * ZenNodeColumnDefinitionConverterTest
 *
 * @author panguanjing
 * @date 2021/7/16
 */
public class ZenNodeColumnDefinitionConverterTest {

    ZenNodeColumnDefinitionConverter zenNodeConverter = new ZenNodeColumnDefinitionConverter();

    ZenParser zenParser;

    @Before
    public void setUp() throws Exception {
        zenParser = new ZenParserImpl();
    }

    @Test
    public void convert() {
        String zen = "user_id+user_name";
        BaseZenNode baseZenNode = zenParser.parseNode(zen);
        List<ColumnDefinition> convert = zenNodeConverter.convert(baseZenNode, null);
        ColumnDefinition columnDefinition = convert.get(0);
        ColumnDefinition columnDefinition1 = convert.get(1);
        assertEquals(columnDefinition.getColName(), new Identifier("user_id"));
        assertEquals(columnDefinition1.getColName(), new Identifier("user_name"));
    }

    @Test
    public void testConvertMore() {
        String zen = "user_id*5";
        BaseZenNode baseZenNode = zenParser.parseNode(zen);
        List<ColumnDefinition> convert = zenNodeConverter.convert(baseZenNode, null);
        ColumnDefinition c = convert.get(0);
        assertEquals(c.getColName(), new Identifier("user_id1"));
    }

    @Test
    public void testConvertStart() {
        String zen = "user_id$$*1";
        BaseZenNode baseZenNode = zenParser.parseNode(zen);
        List<ColumnDefinition> convert = zenNodeConverter.convert(baseZenNode, null);
        ColumnDefinition columnDefinition = convert.get(0);
        assertEquals(columnDefinition.getColName().getValue(), "user_id11");
    }

    @Test
    public void testConverterAttribute() {
        String zen = "user_id.string.comment";
        BaseZenNode baseZenNode = zenParser.parseNode(zen);
        List<ColumnDefinition> convert = zenNodeConverter.convert(baseZenNode, null);
        ColumnDefinition columnDefinition = convert.get(0);
        assertEquals(columnDefinition.getColName(), new Identifier("user_id"));
    }

    @Test
    public void testConverterBigint() {
        String zen = "user_id|user_name|user_age.bigint.年龄";
        BaseZenNode baseZenNode = zenParser.parseNode(zen);
        List<ColumnDefinition> convert = zenNodeConverter.convert(baseZenNode, null);
        ColumnDefinition columnDefinition = convert.get(0);
        assertEquals(columnDefinition.getColName(), new Identifier("user_id"));
    }

    @Test
    public void testBlank() {
        String zen = "user_id string 会员Id\nuser_age bigint 年龄";
        BaseZenNode baseZenNode = zenParser.parseNode(zen);
        List<ColumnDefinition> convert = zenNodeConverter.convert(baseZenNode, null);
        ColumnDefinition columnDefinition = convert.get(0);
        assertEquals(columnDefinition.getColName(), new Identifier("user_id"));
    }
}