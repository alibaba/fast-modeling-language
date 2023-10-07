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

package com.aliyun.fastmodel.core.tree.util;

import java.util.List;

import com.aliyun.fastmodel.core.tree.expr.Identifier;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/6/21
 */
public class IdentifierUtilTest {

    @Test
    public void testIdentifier() {
        String code = "SYS_(dim_shop,dim_table)";
        Identifier identifier = new Identifier(code);
        List<Identifier> identifiers = IdentifierUtil.extractColumns(identifier);
        assertEquals(identifiers.get(0).getValue(), "dim_shop");
        assertEquals(identifiers.get(1).getValue(), "dim_table");
    }

    @Test
    public void testLower() {
        String code = "sys_test1234";
        Identifier identifier = new Identifier(code);
        assertTrue(IdentifierUtil.isSysIdentifier(identifier));
        code = "SYS_TEST1234";
        identifier = new Identifier(code);
        assertTrue(IdentifierUtil.isSysIdentifier(identifier));
    }

    @Test
    public void testDelimited() {
        String abc = IdentifierUtil.delimit("abc");
        assertEquals("`abc`", abc);

        abc = IdentifierUtil.delimit("`abc`");
        assertEquals("`abc`", abc);

        abc = IdentifierUtil.delimit("");
        assertEquals("", abc);
    }
}