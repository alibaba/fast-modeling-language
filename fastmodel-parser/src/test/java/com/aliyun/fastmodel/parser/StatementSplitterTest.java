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

package com.aliyun.fastmodel.parser;

import java.util.List;

import com.aliyun.fastmodel.parser.StatementSplitter.StatementText;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.antlr.v4.runtime.TokenSource;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2020/12/28
 */
public class StatementSplitterTest {

    @Test
    public void testSplit() {
        StatementSplitter statementSplitter = new StatementSplitter("show tables; show create table a.b;");
        List<StatementText> statements = statementSplitter.getStatements();
        assertEquals(2, statements.size());
    }

    @Test
    public void testSplitWithNewDelimiter() {
        StatementSplitter statementSplitter = new StatementSplitter("show tables: show create table a.b:",
            ImmutableSet.of(":"));
        List<StatementText> statements = statementSplitter.getStatements();
        assertEquals(2, statements.size());
    }

    @Test
    public void testGetLexer() {
        TokenSource lexer = StatementSplitter.getLexer("show tables;", Sets.newHashSet(";"));
        assertNotNull(lexer);
    }

    @Test
    public void testIsEmpty() {
        assertTrue(StatementSplitter.isEmptyStatement(""));
        assertTrue(StatementSplitter.isEmptyStatement(" "));
        assertTrue(StatementSplitter.isEmptyStatement("\t\n "));
        assertTrue(StatementSplitter.isEmptyStatement("/* odps */"));
        assertFalse(StatementSplitter.isEmptyStatement("x"));
    }

}