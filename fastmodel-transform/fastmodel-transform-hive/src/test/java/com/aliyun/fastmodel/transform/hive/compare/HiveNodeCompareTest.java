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

package com.aliyun.fastmodel.transform.hive.compare;

import java.util.List;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Hive Node Compare
 *
 * @author panguanjing
 * @date 2021/9/4
 */
public class HiveNodeCompareTest {
    HiveNodeCompare hiveNodeCompare = new HiveNodeCompare();

    @Test
    public void compare() {
        List<BaseStatement> compare = hiveNodeCompare.compare(new DialectNode("create table a (b bigint);"),
            new DialectNode("create table a (b string comment 'comment');"));
        String print = print(compare);
        assertEquals(print, "ALTER TABLE a CHANGE COLUMN b b STRING COMMENT 'comment'");
    }

    @Test
    public void compareWithNoCol() {
        List<BaseStatement> compare = hiveNodeCompare.compare(new DialectNode("create table a comment 'comment';"),
            new DialectNode("create table a (b string comment 'comment');"));
        String print = print(compare);
        assertEquals(print, "ALTER TABLE a ADD COLUMNS\n"
            + "(\n"
            + "   b STRING COMMENT 'comment'\n"
            + ");\n"
            + "ALTER TABLE a SET COMMENT ''");
    }

    @Test
    public void testCompareWithoutBefore() {
        List<BaseStatement> compare = hiveNodeCompare.compare(null,
            new DialectNode("create table a (b string comment 'comment');"));
        String print = print(compare);
        assertEquals(print, "CREATE DIM TABLE a \n"
            + "(\n"
            + "   b STRING COMMENT 'comment'\n"
            + ")");
    }

    @Test
    public void testCompareWithoutAfter() {
        List<BaseStatement> compare = hiveNodeCompare.compare(new DialectNode("create table a (b string comment 'comment');"), null);
        String print = print(compare);
        assertEquals(print, "DROP TABLE IF EXISTS a");
    }

    @Test
    public void compareWithProperties() {
        List<BaseStatement> compare = hiveNodeCompare.compare(
            new DialectNode("create table a (b bigint) tblproperties('life_cycle'='10');"),
            new DialectNode("create table a (b string)  tblproperties('life_cycle'= '11');"));
        String print = print(compare);
        assertEquals(print, "ALTER TABLE a CHANGE COLUMN b b STRING;\nALTER TABLE a SET PROPERTIES('life_cycle'='11')");
    }

    private String print(List<BaseStatement> compare) {
        return compare.stream().map(BaseStatement::toString).collect(Collectors.joining(";\n"));
    }
}