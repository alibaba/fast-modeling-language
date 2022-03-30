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

import com.aliyun.fastmodel.core.tree.statement.table.CreateIndex;
import com.aliyun.fastmodel.core.tree.statement.table.DropIndex;
import com.aliyun.fastmodel.parser.BaseTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Index Test
 *
 * @author panguanjing
 * @date 2021/8/31
 */
public class IndexTest extends BaseTest {

    @Test
    public void testCreateIndex() {
        String fml = "create index t_name on t1(a,b)";
        CreateIndex parse = parse(fml, CreateIndex.class);
        assertEquals(parse.toString(), "CREATE INDEX t_name ON t1 (a,b)");
    }

    @Test
    public void testDropIndex() {
        String fml = "drop index t_name on t1;";
        DropIndex dropIndex = parse(fml, DropIndex.class);
        assertEquals(dropIndex.toString(), "DROP INDEX t_name ON t1");
    }
}
