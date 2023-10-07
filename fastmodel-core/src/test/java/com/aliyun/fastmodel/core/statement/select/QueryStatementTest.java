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

package com.aliyun.fastmodel.core.statement.select;

import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.select.Query;
import com.aliyun.fastmodel.core.tree.statement.select.With;
import com.aliyun.fastmodel.core.tree.util.QueryUtil;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2020/10/20
 */
public class QueryStatementTest {

    @Test
    public void testToString() {
        Query build = QueryUtil.simpleQuery(QueryUtil.selectList(new Identifier("1")));
        assertNotNull(build.toString());
    }

    @Test
    public void testToStringWithField() {
        Query build = QueryUtil.simpleQuery(QueryUtil.selectList(new Identifier("b")));
        assertNotNull(build.toString());
    }

}
