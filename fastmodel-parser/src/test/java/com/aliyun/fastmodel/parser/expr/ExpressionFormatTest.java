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

package com.aliyun.fastmodel.parser.expr;

import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.parser.BaseTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/1/19
 */
public class ExpressionFormatTest extends BaseTest {

    @Test
    public void formatBinaryExpression() {
        String expr = "(a+b)/(b*d)";
        BaseExpression parse = parseExpression(expr);
        assertEquals(parse.toString(), "(a + b) / (b * d)");
    }

    @Test
    public void formatLikePredicate() {
        String like = "b LIKE 'a%'";
        BaseExpression baseExpression = parseExpression(like);
        assertEquals(baseExpression.toString(), like);

        like = "(b like all 'a%')";
        baseExpression = parseExpression(like);
        assertEquals("(b LIKE ALL 'a%')", baseExpression.toString());
    }

    @Test
    public void betweenAnd() {
        String between = "price between 10 and 100";
        BaseExpression baseExpression = parseExpression(between);
        assertEquals(baseExpression.toString(), "price BETWEEN 10 AND 100");
    }

}
