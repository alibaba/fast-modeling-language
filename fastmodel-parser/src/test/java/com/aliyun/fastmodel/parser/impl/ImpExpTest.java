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

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.core.tree.statement.impexp.ExportStatement;
import com.aliyun.fastmodel.parser.BaseTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * 导入导出语句测试
 *
 * @author panguanjing
 * @date 2021/8/22
 */
public class ImpExpTest extends BaseTest {

    @Test
    public void testExp() {
        String exp = "export output='/home/admin/a.txt' where domain='domain';";
        BaseStatement baseStatement = nodeParser.parseStatement(exp);
        ExportStatement exportStatement = (ExportStatement)baseStatement;
        assertEquals(exportStatement.getOutput(), new StringLiteral("/home/admin/a.txt"));
    }

    @Test
    public void testExport() {
        String exp = "export auto_test output='/home/admin/a.txt' where domain='domain';";
        BaseStatement statement = nodeParser.parseStatement(exp);
        ExportStatement exportStatement = (ExportStatement)statement;
        assertEquals(exportStatement.getBaseUnit(), new Identifier("auto_test"));
    }

    @Test
    public void testExportTarget() {
        String exp = "export auto_test output='/home/admin/a.txt' target='zyb' where domain='domain';";
        BaseStatement statement = nodeParser.parseStatement(exp);
        ExportStatement exportStatement = (ExportStatement)statement;
        assertEquals(exportStatement.getBaseUnit(), new Identifier("auto_test"));
        assertEquals(exportStatement.getTargetValue(), "zyb");
    }
}
