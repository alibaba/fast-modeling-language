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

import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.statement.constants.TableDetailType;
import com.aliyun.fastmodel.core.tree.statement.table.CreateCodeTable;
import com.aliyun.fastmodel.parser.BaseTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/1/11
 */
public class CodeTableTest extends BaseTest {

    @Test
    public void testCreateCodeTable() {
        String sql = "create code table if not exists a.b comment 'ab'";
        CreateCodeTable parse = parse(sql, CreateCodeTable.class);
        assertEquals(parse.getQualifiedName(), QualifiedName.of("a.b"));
    }

    @Test
    public void testBuilder() {
        CreateCodeTable build = CreateCodeTable.builder().tableName(QualifiedName.of("a.b")).build();
        assertEquals(build.getTableDetailType(), TableDetailType.CODE);
    }
}
