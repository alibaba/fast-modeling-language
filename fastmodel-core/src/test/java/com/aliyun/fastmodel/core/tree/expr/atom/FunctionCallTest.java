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

package com.aliyun.fastmodel.core.tree.expr.atom;

import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.ArithmeticBinaryExpression;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.enums.ArithmeticOperator;
import com.aliyun.fastmodel.core.tree.util.QueryUtil;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author panguanjing
 * @date 2020/11/17
 */
public class FunctionCallTest {

    @Test
    public void testToString() {
        TableOrColumn tableOrColumn = new TableOrColumn(QualifiedName.of("price"));
        ArithmeticBinaryExpression arithmeticBinaryExpression = new ArithmeticBinaryExpression(
            ArithmeticOperator.SUBTRACT,
            tableOrColumn,
            new TableOrColumn(QualifiedName.of("fact_table", "xxx"))
        );

        ArithmeticBinaryExpression arithmeticBinaryExpression2 = new ArithmeticBinaryExpression(
            ArithmeticOperator.SUBTRACT,
            tableOrColumn,
            new TableOrColumn(QualifiedName.of("fact_table", "xxx"))
        );
        FunctionCall functionCall
            = QueryUtil.functionCall(
            "sum",
            new ArithmeticBinaryExpression(
                ArithmeticOperator.STAR, arithmeticBinaryExpression,
                arithmeticBinaryExpression2
            ));
        assertTrue(functionCall.toString().contains("sum"));
    }

    @Test
    public void testEquals() {
        ArithmeticBinaryExpression arithmeticBinaryExpression = new ArithmeticBinaryExpression(
            ArithmeticOperator.STAR,
            new Identifier("a"),
            new Identifier("b")
        );
        FunctionCall f = new FunctionCall(QualifiedName.of("sum"), false,
            ImmutableList.of(arithmeticBinaryExpression));

        FunctionCall f2 = new FunctionCall(QualifiedName.of("sum"), false,
            ImmutableList.of(arithmeticBinaryExpression));
        assertEquals(f, f2);

    }
}