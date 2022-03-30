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

import com.aliyun.fastmodel.core.tree.statement.CompositeStatement;
import com.aliyun.fastmodel.core.tree.statement.constants.StatementType;
import com.aliyun.fastmodel.parser.BaseTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * CompositeStatement Test
 *
 * @author panguanjing
 * @date 2021/12/24
 */
public class CompositeStatementTest extends BaseTest {

    @Test
    public void testParserer() {
        String fml = " create fact table f_1 (\n"
            + "    id BIGINT PRIMARY KEY COMMENT 'ID',\n"
            + "    name STRING NOT NULL COMMENT '名字'\n"
            + " ) COMMENT '事实表1';\n"
            + "create fact table f_2 (\n"
            + "    id BIGINT COMMENT 'ID',\n"
            + "    name STRING COMMENT '名字'\n"
            + ") COMMENT '事实表2';\n"
            + "REF f_1.id -> f_2.id : ID关联;";

        CompositeStatement parse = nodeParser.parseStatement(fml);
        assertEquals(parse.getStatements().get(0).getStatementType(), StatementType.TABLE);
    }
}
