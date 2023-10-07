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

package com.aliyun.fastmodel.transform.hive.parser;

import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/9/4
 */
public class HiveLanguageParserTest {

    HiveLanguageParser hiveLanguageParser = new HiveLanguageParser();

    @Test
    public void parseNode() {
        CreateTable o = hiveLanguageParser.parseNode("create table a (b bigint comment 'comment abc');");
        assertEquals(o.toString(), "CREATE DIM TABLE a \n"
            + "(\n"
            + "   b BIGINT COMMENT 'comment abc'\n"
            + ")");
    }

    @Test
    public void testWithVar() {
        CreateTable o = hiveLanguageParser.parseNode("CREATE TABLE IF NOT EXISTS ${CDH_HIVE_001} (ID INT, NAME " + "STRING);");
        assertEquals(o.toString(), "CREATE DIM TABLE IF NOT EXISTS `${cdh_hive_001}` \n"
            + "(\n"
            + "   id   INT,\n"
            + "   name STRING\n"
            + ")");
    }

    @Test
    public void parseNodeWithoutSemicolon() {
        CreateTable o = hiveLanguageParser.parseNode("create table a (b bigint comment 'comment abc')");
        assertEquals(o.toString(), "CREATE DIM TABLE a \n"
            + "(\n"
            + "   b BIGINT COMMENT 'comment abc'\n"
            + ")");
    }


    @Test
    public void parseComplex() {
        CreateTable o = hiveLanguageParser.parseNode("create table a (b bigint comment 'comment abc', abc struct<course:string,score:int>, abc1 map<string,string>, bcd array<string>)");
        assertEquals(o.toString(), "CREATE DIM TABLE a \n"
            + "(\n"
            + "   b    BIGINT COMMENT 'comment abc',\n"
            + "   abc  STRUCT<course:STRING,score:INT>,\n"
            + "   abc1 MAP<STRING,STRING>,\n"
            + "   bcd  ARRAY<STRING>\n"
            + ")");
    }

    @Test
    public void parseWithProperty() {
        ReverseContext build = ReverseContext.builder()
            .property(new Property("business_process", "default"))
            .build();
        CreateTable o = (CreateTable)hiveLanguageParser.parseNode("create table a (b bigint comment 'comment abc')", build);
        assertEquals(o.toString(), "CREATE DIM TABLE a \n"
            + "(\n"
            + "   b BIGINT COMMENT 'comment abc'\n"
            + ")\n"
            + "WITH('business_process'='default')");
    }

    @Test
    public void testParseIssue() {
        CreateTable o = hiveLanguageParser.parseNode("create table __test__test (a bigint);");
        assertEquals(o.toString(), "CREATE DIM TABLE `__test__test` \n"
            + "(\n"
            + "   a BIGINT\n"
            + ")");
    }

    @Test
    public void parseDataType() {
        BaseDataType baseDataType = hiveLanguageParser.parseDataType("array<string>", ReverseContext.builder().build());
        assertNotNull(baseDataType);
    }
}