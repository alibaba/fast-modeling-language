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

import com.aliyun.fastmodel.core.tree.expr.enums.BitOperator;
import com.aliyun.fastmodel.core.tree.expr.literal.LongLiteral;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author panguanjing
 * @date 2020/11/20
 */
public class BitOperationExpressionTest {


    @Test
    public void testEquals(){
        BitOperationExpression bitOperationExpression = new BitOperationExpression(
            BitOperator.AMPERSAND, new LongLiteral("2"), new LongLiteral("2")
        );

        BitOperationExpression bitOperationExpression2 = new BitOperationExpression(
            BitOperator.AMPERSAND, new LongLiteral("2"), new LongLiteral("2")
        );
        assertEquals(bitOperationExpression, bitOperationExpression2);
    }

}