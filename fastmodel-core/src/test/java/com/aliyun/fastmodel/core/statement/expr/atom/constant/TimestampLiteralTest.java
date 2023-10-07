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

package com.aliyun.fastmodel.core.statement.expr.atom.constant;

import com.aliyun.fastmodel.core.tree.expr.literal.TimestampLiteral;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author panguanjing
 * @date 2020/10/23
 */
public class TimestampLiteralTest {

    @Test
    public void testToStringVar() {
        TimestampLiteral timestampLiteral = new TimestampLiteral("2020-10-11 11:00:00");
        assertEquals("TIMESTAMP '2020-10-11 11:00:00'", timestampLiteral.toString());
    }
}