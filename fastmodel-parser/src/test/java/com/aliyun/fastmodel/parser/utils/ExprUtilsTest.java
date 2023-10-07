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

package com.aliyun.fastmodel.parser.utils;

import java.util.List;

import com.aliyun.fastmodel.core.parser.DomainLanguage;
import com.aliyun.fastmodel.core.tree.expr.atom.TableOrColumn;
import com.aliyun.fastmodel.parser.NodeParser;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2020/9/27
 */
public class ExprUtilsTest {

    NodeParser fastModelAntlrParser = new NodeParser();

    @Test
    public void testExtract() throws Exception {
        String test = "sum(a + b)";
        List<TableOrColumn> result = fastModelAntlrParser.extract(new DomainLanguage(test));
        assertEquals(result.size(), 2);
    }

    @Test
    public void testDotExtract() {
        String test2 = "sum(a.b)";
        DomainLanguage expr = new DomainLanguage(test2);
        List<TableOrColumn> tableOrColumns = fastModelAntlrParser.extract(expr);
        assertEquals(1, tableOrColumns.size());
    }

}

