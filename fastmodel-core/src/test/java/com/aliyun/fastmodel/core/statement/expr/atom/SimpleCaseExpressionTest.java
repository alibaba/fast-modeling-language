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

package com.aliyun.fastmodel.core.statement.expr.atom;

import java.util.Collections;
import java.util.List;

import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.CustomExpression;
import com.aliyun.fastmodel.core.tree.expr.atom.SimpleCaseExpression;
import com.aliyun.fastmodel.core.tree.expr.atom.WhenClause;
import org.hamcrest.Matchers;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

/**
 * @author panguanjing
 * @date 2020/10/23
 */
public class SimpleCaseExpressionTest {

    @Test
    public void testToString() {
        BaseExpression caseExpr = getCaseExpr();
        List<WhenClause> list = getListExpr();
        BaseExpression elseExpr = getElseExpr();
        SimpleCaseExpression simpleCaseExpression = new SimpleCaseExpression(caseExpr, list, elseExpr);
        simpleCaseExpression.setParenthesized(true);
        assertNotNull(simpleCaseExpression);
        assertThat(simpleCaseExpression.toString(),
            Matchers.equalToIgnoringCase("(CASE a=1 WHEN c=1 THEN d ELSE b=1 END)"));

    }

    private BaseExpression getElseExpr() {
        return new CustomExpression("b=1");
    }

    private List<WhenClause> getListExpr() {
        BaseExpression baseExpression = new CustomExpression("c=1");
        BaseExpression baseExpression1 = new CustomExpression("d");
        WhenClause whenClause = new WhenClause(baseExpression, baseExpression1);
        return Collections.singletonList(whenClause);
    }

    private BaseExpression getCaseExpr() {
        return new CustomExpression("a=1");
    }
}