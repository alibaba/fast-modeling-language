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

package com.aliyun.fastmodel.core.formatter;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.aliyun.fastmodel.core.tree.datatype.GenericDataType;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/7/23
 */
public class ExpressionVisitorTest {

    @Test
    public void testFormatString() {
        ExpressionVisitor expressionVisitor = new ExpressionVisitor();
        String s = expressionVisitor.formatStringLiteral(null);
        assertEquals(s, "''");
    }

    @Test
    public void testCustom() {
        ExpressionVisitor expressionVisitor = new ExpressionVisitor();
        GenericDataType g = new GenericDataType("VARCHAR2");
        String result = expressionVisitor.visitGenericDataType(g, null);
        assertEquals(result, "VARCHAR2");
    }

    @Test
    public void testIdentifier() {
        Pattern pattern = Pattern.compile("[0-9]*");
        Matcher matcher = pattern.matcher("1");
        assertTrue(matcher.matches());
        matcher = pattern.matcher("1a");
        assertFalse(matcher.matches());
    }

    @Test
    public void testDigit() {
        Identifier identifier = new Identifier("1");
        assertTrue(identifier.isDelimited());
    }
}