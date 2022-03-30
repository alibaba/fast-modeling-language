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

import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.ComparisonExpression;
import com.aliyun.fastmodel.core.tree.expr.similar.InListExpression;
import com.aliyun.fastmodel.core.tree.statement.constants.DeleteType;
import com.aliyun.fastmodel.core.tree.statement.delete.Delete;
import com.aliyun.fastmodel.core.tree.statement.show.WhereCondition;
import com.aliyun.fastmodel.parser.BaseTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/1/15
 */
public class DeleteTest extends BaseTest {

    @Test
    public void testDelete() {
        String sql = "delete from a.b where 1=1";
        Delete parse = parse(sql, Delete.class);
        assertEquals(parse.getDeleteType(), DeleteType.TABLE);
        assertEquals(parse.getWhereCondition().getBaseExpression().toString(), "1 = 1");
    }

    @Test
    public void testDeleteWithType() {
        String sql = "delete from a.b where 1=1;";
        Delete delete = parse(sql, Delete.class);
        assertEquals(delete.getDeleteType(), DeleteType.TABLE);
    }

    @Test
    public void testDeleteCodeType() {
        String sql = "delete from a.b where `code` in ('1','2')";
        Delete delete = parse(sql, Delete.class);
        WhereCondition whereCondition = delete.getWhereCondition();
        BaseExpression baseExpression = whereCondition.getBaseExpression();
        assertEquals(baseExpression.getClass(), InListExpression.class);
    }

    @Test
    public void testDeletCodeType() {
        String sql = "delete from a.b where `code` = '1'";
        Delete delete = parse(sql, Delete.class);
        WhereCondition whereCondition = delete.getWhereCondition();
        assertEquals(whereCondition.getBaseExpression().getClass(), ComparisonExpression.class);

    }
}
