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

package com.aliyun.fastmodel.core.tree.expr;

import java.util.ArrayList;
import java.util.List;

import com.aliyun.fastmodel.core.tree.expr.enums.ComparisonOperator;
import com.aliyun.fastmodel.core.tree.expr.enums.LogicalOperator;
import com.aliyun.fastmodel.core.tree.expr.literal.LongLiteral;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2020/11/4
 */
public class LogicalBinaryExpressionTest {

    @Test
    public void testEqual() {
        List<BaseExpression> list = prepare();
        BaseExpression logicalBinaryExpression = toTree(list);
        assertNotNull(logicalBinaryExpression);
    }


    @Test
    public void testEquals(){
        LogicalBinaryExpression logicalBinaryExpression1 = new LogicalBinaryExpression(LogicalOperator.AND,
            new LongLiteral("1"), new LongLiteral("2"));
        LogicalBinaryExpression logicalBinaryExpression2 = new LogicalBinaryExpression(LogicalOperator.AND,
            new LongLiteral("1"), new LongLiteral("2"));
        assertEquals(logicalBinaryExpression1, logicalBinaryExpression2);
    }

    private BaseExpression toTree(List<BaseExpression> list) {
        if (list.isEmpty()) {
            return null;
        }
        if (list.size() == 1) {
            return list.get(0);
        }
        return new LogicalBinaryExpression(LogicalOperator.AND, list.remove(0), toTree(list));
    }

    private List<BaseExpression> prepare() {
        List<BaseExpression> list = new ArrayList<>();
        list.add(new ComparisonExpression(ComparisonOperator.EQUAL, new Identifier("a"), new LongLiteral("1")));
        list.add(new ComparisonExpression(ComparisonOperator.EQUAL, new Identifier("b"), new LongLiteral("2")));
        list.add(new ComparisonExpression(ComparisonOperator.EQUAL, new Identifier("c"), new LongLiteral("3")));
        list.add(new ComparisonExpression(ComparisonOperator.EQUAL, new Identifier("d"), new LongLiteral("3")));
        return list;
    }
}