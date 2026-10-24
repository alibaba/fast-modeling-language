/*
 * Copyright 2021 Alibaba Group Holding Ltd.
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

package com.aliyun.fastmodel.transform.mc.compare;

import java.util.List;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.transform.api.compare.CompareContext;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/9/4
 */
public class MaxComputeNodeCompareTest {

    MaxComputeNodeCompare maxComputeNodeCompare = new MaxComputeNodeCompare();

    @Test
    public void testCompareWithoutColumns() {
        DialectNode before = new DialectNode("create table a comment 'abc'");
        DialectNode after = new DialectNode("create table a (c string comment 'comment')comment 'abc'");
        List<BaseStatement> compare = maxComputeNodeCompare.compare(before, after, CompareContext.builder().build());
        assertEquals(print(compare), "ALTER TABLE a ADD COLUMNS\n"
            + "(\n"
            + "   c STRING COMMENT 'comment'\n"
            + ")");
    }

    @Test
    public void testCompare() {
        DialectNode before = new DialectNode("create table a (b string comment 'comment');");
        DialectNode after = new DialectNode("create table a (c string comment 'comment');");
        List<BaseStatement> compare = maxComputeNodeCompare.compare(before, after, CompareContext.builder().build());
        assertEquals(print(compare), "ALTER TABLE a DROP COLUMN b;\n"
            + "ALTER TABLE a ADD COLUMNS\n"
            + "(\n"
            + "   c STRING COMMENT 'comment'\n"
            + ")");
    }

    private String print(List<BaseStatement> compare) {
        return compare.stream().map(BaseStatement::toString).collect(Collectors.joining(";\n"));
    }
}