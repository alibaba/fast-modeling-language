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

    /**
     * Quote digit-leading identifiers consistently.
     */
    @Test
    public void testDigitLeadingNonDelimitedIdentifierGetsQuoted() {
        ExpressionVisitor visitor = new ExpressionVisitor();

        Identifier badTable = new Identifier("123_example_table");
        assertFalse("回归保护:NAME_PATTERN 允许数字开头,Identifier 自动识别为非 delimited",
            badTable.isDelimited());
        assertEquals("`123_example_table`", visitor.visitIdentifier(badTable, null));

        // 短的纯数字+下划线
        Identifier shortBad = new Identifier("9a");
        assertEquals("`9a`", visitor.visitIdentifier(shortBad, null));

        // 纯数字仍维持原有行为(Identifier 自动识别为 delimited)
        Identifier pureDigit = new Identifier("123");
        assertTrue(pureDigit.isDelimited());
        assertEquals("`123`", visitor.visitIdentifier(pureDigit, null));
    }

    /**
     * 不以数字开头的正常标识符,行为保持不变,不加反引号。
     */
    @Test
    public void testLetterLeadingIdentifierStaysUnquoted() {
        ExpressionVisitor visitor = new ExpressionVisitor();
        assertEquals("user_id", visitor.visitIdentifier(new Identifier("user_id"), null));
        assertEquals("orders", visitor.visitIdentifier(new Identifier("orders"), null));
        // 单字母也是允许首字符
        assertEquals("a", visitor.visitIdentifier(new Identifier("a"), null));
    }

    /**
     * 已经被引号包裹的不应该重复加,delimited=true 但首字符是反引号时直接返回原值。
     */
    @Test
    public void testAlreadyQuotedIdentifierNotDoubled() {
        ExpressionVisitor visitor = new ExpressionVisitor();
        Identifier already = new Identifier("`123_xxx`", true);
        assertEquals("`123_xxx`", visitor.visitIdentifier(already, null));
    }
}